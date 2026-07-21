import logging
import os
import queue
import signal
import socket
import threading
import time
from _socket import SHUT_RDWR
from abc import abstractmethod, ABC
from collections import Counter
from dataclasses import dataclass, field
from multiprocessing import Process
from typing import Final, List

import pandas as pd
import redis

from constants import FNAME, HEARTBEAT_INTERVAL, STREAMS, COORDINATOR_PORT
from message import Message, MT
from mylog import Logger

logging = Logger().get_logger()

@dataclass
class MapperState:
  idx: int
  reducer_ports: list[int]
  pid: int
  id: str
  c_socket: socket.socket
  last_stream_id: bytes = b"0"
  last_cp_id: int = 0
  reducer_sockets: list[socket.socket] = field(default_factory=list)
  is_wc_done: bool = False

  def __post_init__(self):
    self.c_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    logging.info(f"{self.id} Connecting to reducers")
    for i in range(len(self.reducer_ports)):
      conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      while True:
        try:
          conn.connect(("localhost", self.reducer_ports[i]))
          break
        except ConnectionRefusedError:
          # Keep trying, eventually reducer will be up.
          logging.warning("Couldn't connect to reducer, waiting for it go up.")
          time.sleep(0.2)

      self.reducer_sockets.append(conn)

  def to_coordinator(self, msg: Message):
    self.c_socket.sendto(msg.serialize(), ("localhost", COORDINATOR_PORT))

  def checkpoint(self, checkpoint_id: int):
    logging.info(f"{self.id} performing checkpointing of its stream ID")
    os.makedirs('checkpoints', exist_ok=True)
    filename = f"checkpoints/{self.id}_{checkpoint_id}.txt"
    with open(filename, 'w') as file:
      file.write(self.last_stream_id.decode() + '\n')
    self.last_cp_id = checkpoint_id

  def word_2_socket(self, word: str) -> socket.socket:
    if word[0] == 'a':
      return self.reducer_sockets[0]
    else:
      return self.reducer_sockets[1]

  def exit(self):
    for rs in self.reducer_sockets:
      try:
        rs.shutdown(SHUT_RDWR)
      except Exception as e:
        logging.warning(f"Ignoring {e}")
    self.reducer_sockets = []

    os.kill(self.pid, signal.SIGKILL)


@dataclass
class Cmd(ABC):

  @abstractmethod
  def handle(self, state: MapperState):
    pass


@dataclass
class Checkpoint(Cmd):
  checkpoint_id: int

  def handle(self, state: MapperState):
    logging.info(f"{state.id} received checkpoint marker {self.checkpoint_id} from Coordinator")
    state.checkpoint(self.checkpoint_id)

    for idx, r_sock in enumerate(state.reducer_sockets):
      logging.info(f"{state.id} sending checkpoint marker {self.checkpoint_id} to R{idx}")
      msg_bytes = Message(msg_type=MT.FWD_CHECKPOINT, source=state.id, mapper_idx=state.idx,
                          checkpoint_id=self.checkpoint_id).serialize()
      r_sock.sendall(msg_bytes)

    logging.info(f"{state.id} sending checkpoint acknowledgement to Coordinator for checkpoint {self.checkpoint_id}")
    chkack_msg = Message(msg_type=MT.CHECKPOINT_ACK, source=state.id, checkpoint_id=self.checkpoint_id)
    state.to_coordinator(chkack_msg)


@dataclass
class Exit(Cmd):
  
  def handle(self, state: MapperState):
    logging.critical(f"{state.id} exiting!")
    state.exit()


class CmdHandler(threading.Thread):
  def __init__(self, state: MapperState, cmd_q: queue.Queue[Cmd], stream_name: bytes):
    super(CmdHandler, self).__init__()
    self.cmd_q = cmd_q
    self.state = state
    self.rds = redis.Redis(host='localhost', port=6379, db=0, decode_responses=False)
    self.my_stream: bytes = stream_name

  def run(self) -> None:
    while True:
      try:
        cmd: Cmd = self.cmd_q.get(block=self.state.is_wc_done)
      except queue.Empty:
        self.word_count()
        continue
      try:
        cmd.handle(self.state)
      
      except Exception as e:
        logging.exception(e)

  def word_count(self):
    logging.debug(f"Reading stream {self.my_stream} from {self.state.last_stream_id}")
    result = self.rds.xread({self.my_stream: self.state.last_stream_id}, count = 1)
    if not result:
      self.state.is_wc_done = True
      logging.info(f"{self.state.id} finished reading its stream!")
      logging.info(f"{self.state.id} telling the coordinator that I am done!")
      done_msg = Message(msg_type=MT.DONE, source=self.state.id)
      self.state.to_coordinator(done_msg)
      return
    try:
      _, entries = result[0]
      message_id, message_data = entries[0]
      filepath = message_data[FNAME].decode('utf-8')
      # logging.info(f"{self.state.id} processing file {filepath}")
      df = pd.read_csv(filepath)
      all_text = ' '.join(df['text'])
      tokens = all_text.split(" ")
      word_counts = Counter(tokens)
      for word, count in word_counts.items():
        r_sock = self.state.word_2_socket(word)
        msg_bytes = Message(msg_type=MT.WORD_COUNT, source=self.state.id, key=word, value=count).serialize()
        r_sock.sendall(msg_bytes)
      self.state.last_stream_id = message_id
    except Exception as e:
      # logging.exception(e)
      logging.error(e)
      time.sleep(0.1)
    

class Mapper(Process):
  def __init__(self, idx: int, downstream: List[int], port: int):
    super().__init__()
    self.idx = idx
    self.id: Final[str] = f"Mapper_{idx}"
    self.downstream = downstream
    self.listenPort = port
    self.my_stream = STREAMS[idx]

  def send_heartbeat(self):
    heartbeat_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
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
        if message.msg_type == MT.CHECKPOINT:
          checkpoint_id = message.kwargs['checkpoint_id']
          logging.info(f"{self.id} received checkpoint marker from {message.source} "
                       f"with id = {checkpoint_id}")
          try:
            cmd_q.put(Checkpoint(checkpoint_id=checkpoint_id))
          except Exception as e:
            logging.error(f"Error: {e}")
        elif message.msg_type == MT.EXIT:
          logging.info(f"{self.id} received EXIT marker from {message.source}")
          cmd_q.put(Exit())
      except Exception as e:
        logging.error(f"Error: {e}")
        
  def run(self):
    cmd_q = queue.Queue()
    state = MapperState(
      reducer_ports=self.downstream,
      pid=self.pid,
      id=self.id,
      idx=self.idx,
      last_stream_id=b"0",
      last_cp_id=0,
      c_socket=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    )

    self.rds = redis.Redis(host='localhost', port=6379, db=0, decode_responses=False)

    state.c_socket.bind(("localhost", self.listenPort))
    coordinator_handler = threading.Thread(target=self.handle_coordinator, args=(state.c_socket, cmd_q, ))
    coordinator_handler.start()

    # thread to send heartbeats to the coordinator
    heartbeat_thread = threading.Thread(target=self.send_heartbeat)
    heartbeat_thread.start()

    cmd_thread = CmdHandler(state, cmd_q, self.my_stream)
    cmd_thread.start()

    logging.info(f"{self.id} waiting for all threads to join")
    cmd_thread.join()
    coordinator_handler.join()
    heartbeat_thread.join()
