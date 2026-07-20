from _socket import SHUT_RDWR
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from collections import defaultdict
import logging
import json
import os
import signal
import queue
import socket
import threading
from multiprocessing import Process
import time
from typing import Final

from constants import HEARTBEAT_INTERVAL, COORDINATOR_PORT, MAX_CKPT_ID
from message import Message, MT
from mylog import Logger

logging = Logger().get_logger()

TYPE_CP_MARKER: Final[int] = 1
TYPE_RECOVER: Final[int] = 2
TYPE_COUNT: Final[int] = 3
TYPE_EXIT: Final[int] = 4

def recvall(sock: socket.socket, length: int) -> bytes:
    data = b''
    while len(data) < length:
        packet = sock.recv(length - len(data))
        if not packet:
            raise Exception("Connection closed")
        data += packet
    return data


@dataclass
class ReducerState:
  pid: int
  id: str
  c_socket: socket.socket
  listen_port: int
  server_socket: socket.socket
  barrier: threading.Barrier
  wc: dict[str, int] = field(default_factory=lambda: defaultdict(int))
  last_recovery_id: int = 0
  last_cp_id: int = -1
  client_sockets: list[socket.socket] = field(default_factory=list)

  def __post_init__(self):
    # creating connections with coordinator
    self.c_socket.bind(("localhost", self.listen_port))

    # creating connections with mappers and threads to handle data incoming from mapper
    self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.server_socket.bind(("localhost", self.listen_port))
    self.server_socket.listen(5)
    logging.info(f"{self.id} listening on localhost:{self.listen_port}")

  def to_coordinator(self, msg: Message) -> None:
    self.c_socket.sendto(msg.serialize(), ("localhost", COORDINATOR_PORT))

  def checkpoint(self, checkpoint_id: int):
    os.makedirs('checkpoints', exist_ok=True)
    filename = f"checkpoints/{self.id}_{checkpoint_id}.txt"
    with open(filename, 'w') as file:
      json.dump(self.wc, file)
    self.last_cp_id = checkpoint_id

  def recover(self, recovery_id: int, checkpoint_id: int):
    if checkpoint_id == -1:
      self.wc = {}
    else:
      filename = f"checkpoints/{self.id}_{checkpoint_id}.txt"
      with open(filename, 'r') as file:
        self.wc = json.load(file)

    # Break existing map threads by re-opening sockets. while loop will create new ones
    for s in self.client_sockets:
      try:
        s.shutdown(SHUT_RDWR)
      except Exception as e:
        logging.warning(f"Mapper is probably dead. Ignoring {e}")
    self.client_sockets = []
    self.barrier.reset()

    self.last_recovery_id = recovery_id

  def exit(self):
    # Break existing map threads by re-opening sockets. while loop will create new ones
    for s in self.client_sockets:
      try:
        s.shutdown(SHUT_RDWR)
      except Exception as e:
        logging.warning(f"Mapper is probably dead. Ignoring {e}")
    self.client_sockets = []
    self.barrier.reset()

    try:
      self.server_socket.shutdown(SHUT_RDWR)
    except Exception as e:
      logging.warning(f"Mapper is probably dead. Ignoring {e}")

    os.kill(self.pid, signal.SIGKILL)


class Cmd(ABC):
  @abstractmethod
  def handle(self, state: ReducerState):
    raise NotImplementedError


class CPMarker(Cmd):
  def __init__(self, checkpoint_id: int, recovery_id: int):
    self.checkpoint_id = checkpoint_id
    self.recovery_id = recovery_id

  def handle(self, state: ReducerState):
    if self.recovery_id == state.last_recovery_id:
      logging.info(f"Checkpointing reducer {state.id}, chkpt_id={self.checkpoint_id}")
      state.checkpoint(self.checkpoint_id)
      chkack_msg = Message(msg_type=MT.CHECKPOINT_ACK, source=state.id, checkpoint_id=self.checkpoint_id, recovery_id=state.last_recovery_id)
      state.to_coordinator(chkack_msg)
    else:
      logging.warning(f"Ignoring message with recovery id {self.recovery_id} since mine is {state.last_recovery_id}")


class Recover(Cmd):
  def __init__(self, recovery_id: int, checkpoint_id: int):
    self.checkpoint_id = checkpoint_id
    self.recovery_id = recovery_id

  def handle(self, state: ReducerState):
    state.recover(self.recovery_id, self.checkpoint_id)
    recack_msg = Message(msg_type=MT.RECOVERY_ACK, source=state.id, recovery_id=self.recovery_id)
    state.to_coordinator(recack_msg)


class WC(Cmd):
  def __init__(self, word: str, count: int, recovery_id: int):
    self.recovery_id = recovery_id
    self.word = word
    self.count = count

  def handle(self, state: ReducerState):
    if self.recovery_id == state.last_recovery_id:
      state.wc[self.word] += self.count
      logging.debug(f"Adding word count {self.word}={self.count}")
    else:
      logging.warning(
        f"{state.id}: Ignoring in-flight messages with recovery_id = {self.recovery_id}. Expecting recovery_id = {state.last_recovery_id}")


class Exit(Cmd):
  def handle(self, state: ReducerState):
    logging.critical(f"{state.id} exiting!")
    state.exit()


def send_data(socket, key, value, id, local_counter):
  try:
    data = f"{key},{value},{id},{local_counter}".encode()
    length_prefix = f"{len(data):<10}".encode()
    socket.sendall(length_prefix + data)
  except Exception as e:
    logging.error(f"Error: {e}")


class CmdHandler(threading.Thread):
  def __init__(self, state: ReducerState, cmd_q: queue.Queue[Cmd]):
    super(CmdHandler, self).__init__()
    self.cmd_q = cmd_q
    self.state = state

  def run(self):
    while True:
      cmd: Cmd = self.cmd_q.get()
      try:
        cmd.handle(self.state)

      except Exception as e:
        logging.exception(e)
        logging.critical("Reducer should not have gotten exceptions! Killing self.")
        os.kill(self.state.pid, signal.SIGKILL)


class Reducer(Process):
  def __init__(self, idx: int, listen_port: int, num_mappers: int):
    super().__init__()
    self.id: Final[str] = f"Reducer_{idx}"
    self.cmd_q: queue.Queue[Cmd]

    self.listen_port = listen_port
    self.num_mappers: Final[int] = num_mappers

    self.cp_marker: dict[int, int] = {}
    for i in range(num_mappers):
      self.cp_marker[i] = -1

  def send_heartbeat(self, heartbeat_socket: socket.socket):
    coordinator_addr = ("localhost", COORDINATOR_PORT)
    while True:
      try:
        heartbeat_socket.sendto(Message(msg_type=MT.HEARTBEAT, source=self.id).serialize(), coordinator_addr)
        time.sleep(HEARTBEAT_INTERVAL)
      except Exception as e:
        logging.error(f"Heartbeat error: {e}")

  def handle_coordinator(self, coordinator_conn: socket.socket, cmd_q: queue.Queue[Cmd]):
    while True:
      try:
        response, _ = coordinator_conn.recvfrom(1024)
        message: Message = Message.deserialize(response)
        logging.info(f"Received {message.msg_type.name} from coordinator")
        if message.msg_type == MT.EXIT:
          cmd_q.put(Exit())
        elif message.msg_type == MT.RECOVER:
          cmd_q.put(Recover(checkpoint_id=message.kwargs["checkpoint_id"],
                            recovery_id=message.kwargs["recovery_id"]))
      except Exception as e:
        logging.error(f"Error: {e}")

  def handle_mappers(self, client_socket: socket.socket, cmd_q: queue.Queue[Cmd], barrier: threading.Barrier):
    try:
      while True:
        data = recvall(client_socket, 1024)
        if not data:
          break
        message = Message.deserialize(data)

        if message.msg_type == MT.FWD_CHECKPOINT:  # received checkpoint marker
          checkpoint_id = message.kwargs.get('checkpoint_id')
          recovery_id = message.kwargs.get('recovery_id')
          mapper_idx = message.kwargs.get("mapper_idx")
          logging.info(f"{self.id} has Received MARKER {checkpoint_id} from {message.source}")
          self.cp_marker[mapper_idx] = checkpoint_id
          barrier.wait()
          if mapper_idx == 0:  # Only one thread should checkpoint
            for i in range(self.num_mappers):
              assert checkpoint_id == self.cp_marker[i], (f"{self.id} received {checkpoint_id} from one side "
                                                          f"and {self.cp_marker[i]} from another!")
            # time.sleep(0.1)
            cmd_q.put(CPMarker(checkpoint_id=checkpoint_id, recovery_id=recovery_id))
          barrier.wait()
          logging.info(f"-- SYNCHRONISED --")
        else:
          assert message.msg_type == MT.WORD_COUNT
          key = message.kwargs.get('key')
          value = message.kwargs.get('value')
          recovery_id = message.kwargs.get('last_recovery_id')
          cmd_q.put(WC(key, value, recovery_id))
    except Exception as e:
      logging.error(f"Error: {e}")

    client_socket.close()
    logging.info(f"{self.id} handle_mappers thread exiting")

  def run(self):
    cmd_q = queue.Queue()

    state = ReducerState(pid=self.pid, id=self.id, listen_port=self.listen_port,
                         barrier=threading.Barrier(self.num_mappers),
                         c_socket=socket.socket(socket.AF_INET, socket.SOCK_DGRAM),
                         server_socket=socket.socket(socket.AF_INET, socket.SOCK_STREAM))
    cmd_thread = CmdHandler(state, cmd_q)
    cmd_thread.start()

    coordinator_handler = threading.Thread(target=self.handle_coordinator, args=(state.c_socket, cmd_q,))
    coordinator_handler.start()

    # thread to send heartbeats to the coordinator
    heartbeat_thread = threading.Thread(target=self.send_heartbeat, args=(state.c_socket,))
    heartbeat_thread.start()

    try:
      while True:
        # this will make one connection for each mapper
        client_socket, _ = state.server_socket.accept()
        state.client_sockets.append(client_socket)
        logging.info(f"{self.id} Accepted connection from mapper")
        client_handler = threading.Thread(target=self.handle_mappers, args=(client_socket, cmd_q, state.barrier))
        client_handler.start()
    except Exception as e:
      logging.error(f"Error: {e}")
