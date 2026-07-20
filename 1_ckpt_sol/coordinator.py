import datetime
from abc import ABC, abstractmethod
from enum import IntEnum
from multiprocessing import Process
import os
import signal
import socket
from typing import Final, Optional
import time
import threading
import logging
import queue

from mapper import Mapper
from constants import HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT, CHECKPOINT_INTERVAL, MAX_CKPT_ID, NUM_MAPPERS, \
  MAPPER_PORTS, REDUCER_PORTS, COORDINATOR_PORT
from reducer import Reducer
from message import Message, MT

from mylog import Logger
logging = Logger().get_logger()


class WorkerState:
  def __init__(self, idx: int, is_mapper: bool, addr: tuple[str, int]):
    self.idx: Final[int] = idx
    self.id: Final[str] = f"{'Mapper' if is_mapper else 'Reducer'}_{idx}"
    self.is_mapper: Final[bool] = is_mapper
    self.addr: Final[tuple[str, int]] = addr

    self.last_hb_recvd: int = 0
    self.last_cp_id: int = 0
    self.is_done: bool = False  # only for mappers
    self.last_checkpoint_done: bool = False
    self.process: Process

  def start_worker(self) -> None:
    if self.is_mapper:
      self.process = Mapper(self.idx, REDUCER_PORTS, MAPPER_PORTS[self.idx])
    else:
      self.process = Reducer(self.idx, REDUCER_PORTS[self.idx], NUM_MAPPERS)
    self.process.start()


class PHASE(IntEnum):
  CP = 1
  LAST_CP = 2
  EXITING = 3

class GlobalState:
  def __init__(self) -> None:
    self.phase: PHASE = PHASE.CP
    self.next_cp_id: int = 1
    self.workers: dict[str, WorkerState] = {}
    self.sock: Optional[socket.socket] = None

class RcvMsg(ABC):
  @abstractmethod
  def update(self, gs: GlobalState, source: str) -> Optional[PHASE]:
    raise NotImplementedError

class HBRecvMsg(RcvMsg):
  def update(self, gs: GlobalState, source: str) -> Optional[PHASE]:
    logging.debug(f"Coordinator received heartbeat from {source}")
    gs.workers[source].last_hb_recvd = int(time.time())
    return None

class CpktAckRecvMsg(RcvMsg):
  def __init__(self, checkpoint_id: str):
    self.checkpoint_id: Final[int] = int(checkpoint_id)

  def update(self, gs: GlobalState, source: str) -> Optional[PHASE]:
    if self.checkpoint_id == MAX_CKPT_ID:  # final checkpoint, taken once all mappers are done
      gs.workers[source].last_checkpoint_done = True
      if all(ws.last_checkpoint_done for ws in gs.workers.values()):
        return PHASE.EXITING
      return None

    gs.workers[source].last_cp_id = self.checkpoint_id
    if all(ws.last_cp_id == gs.next_cp_id for ws in gs.workers.values()):
      gs.next_cp_id += 1
      return PHASE.CP
    return None

class DoneRecvMsg(RcvMsg):
  def update(self, gs: GlobalState, source: str) -> Optional[PHASE]:
    logging.info(f"Received DONE message from {source}")
    assert gs.workers[source].is_mapper == True, "Only mappers should send DONE message"
    gs.workers[source].is_done = True
    are_all_mappers_done = True
    for _, ws in gs.workers.items():
      if ws.is_mapper:
        if ws.is_done == False:
          are_all_mappers_done = False

    if are_all_mappers_done:
      return PHASE.LAST_CP
    return None

def msg_factory(message: Message) -> RcvMsg:
  if message.msg_type == MT.HEARTBEAT:
    return HBRecvMsg()
  elif message.msg_type == MT.CHECKPOINT_ACK:
    return CpktAckRecvMsg(message.kwargs["checkpoint_id"])
  elif message.msg_type == MT.DONE:
    return DoneRecvMsg()
  else:
    logging.error(f"Unknown message type {message.msg_type}! Bad situtation.")
    raise Exception(f"Unknown message type {message.msg_type}! Bad situtation.")
    


class RecvThread(threading.Thread):
  def __init__(self, global_state: GlobalState, phase_queue: queue.Queue[PHASE]):
    super().__init__()
    self.global_state = global_state
    self.phase_queue = phase_queue
    signal.signal(signal.SIGALRM, self.monitor_health)
    signal.setitimer(signal.ITIMER_REAL, HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL)

  def monitor_health(self, signum, frame):
    logging.info("-- monitoring heartbeats --")

    for _, ws in self.global_state.workers.items():
      last = ws.last_hb_recvd
      cur = int(time.time())
      diff = cur - last
      logging.debug(f"{_} sent last heartbeat {diff} seconds ago")
      if diff > HEARTBEAT_TIMEOUT:
        logging.critical(f"{_} is facing heartbeat timeouts")

  def run(self):
    logging.info("RECV thread of coordinator started!")
    while True:
      response, _ = self.global_state.sock.recvfrom(1024)
      msg = Message.deserialize(response)  # type = Message(msg_type, source, key, value)
      logging.debug(f"Received message of type '{msg.msg_type.name}' from '{msg.source}'")
      deserialized_msg = msg_factory(msg)

      new_phase = deserialized_msg.update(self.global_state, msg.source)  # it will also transition the phase and add in queue if needed
      if new_phase is not None:
        logging.info(f"Moving from {self.global_state.phase.name} to {new_phase.name}")
        self.global_state.phase = new_phase
        self.phase_queue.put(new_phase)

class SendMsg(ABC):
  def send(self, sock: Optional[socket.socket], addr: tuple[str, int]) -> None:
    assert sock is not None
    try:
      b_msg = self.encode()
      sock.sendto(b_msg, addr)
    except socket.error as e:
      logging.error(f"Error sending data: {e}")
    except Exception as e:
      logging.error(f"Unexpected error: {e}")

  @abstractmethod
  def encode(self) -> bytes:
    raise NotImplementedError

class CPMsg(SendMsg):
  def __init__(self, checkpoint_id: int):
    self.checkpoint_id = checkpoint_id

  def encode(self) -> bytes:
    return Message(msg_type=MT.CHECKPOINT, source="Coordinator", checkpoint_id=self.checkpoint_id).serialize()


class ExitMsg(SendMsg):
  def encode(self) -> bytes:
    return Message(msg_type=MT.EXIT, source="Coordinator").serialize()


class SendThread(threading.Thread):
  def __init__(self, id: str, pid: int, global_state: GlobalState, phase_queue: queue.Queue[PHASE]):
    super().__init__()
    self.id: Final[str] = id
    self.pid: Final[int] = pid
    self.global_state: Final[GlobalState] = global_state
    self.phase_queue: queue.Queue[PHASE] = phase_queue


  def cp_phase(self):
    if self.global_state.phase != PHASE.CP:
      return
    logging.info(f"{self.id} sending checkpoint marker {self.global_state.next_cp_id}")
    for _, ws in self.global_state.workers.items():
      if ws.is_mapper:
        CPMsg(self.global_state.next_cp_id).send(self.global_state.sock, ws.addr)

  def last_cp_phase(self):
    assert self.global_state.phase == PHASE.LAST_CP
    logging.info(f"{self.id} sending last checkpoint markers")
    for _, ws in self.global_state.workers.items():
      if ws.is_mapper:
        CPMsg(MAX_CKPT_ID).send(self.global_state.sock, ws.addr)

  def exit_phase(self, start_time):
    assert self.global_state.phase == PHASE.EXITING
    logging.info(f"{self.id} sending exit command to workers")
    for _, ws in self.global_state.workers.items():
      ExitMsg().send(self.global_state.sock, ws.addr)
    logging.critical(f"{self.id} exiting!")
    # self.global_state.send_socket.close()
    end_time = datetime.datetime.now()
    logging.info(f"Job Finished at {end_time}")
    logging.info(f"Total Time Taken = {end_time - start_time}")
    os.kill(self.pid, signal.SIGKILL)

  def run(self):
    start_time = datetime.datetime.now()
    logging.info(f"Starting Job at {start_time}")

    logging.info("SENDING thread of coordinator started!")
    while True:
      current_phase = None
      if not self.phase_queue.empty():
        current_phase = self.phase_queue.get()

      if current_phase is None:
        continue

      elif current_phase == PHASE.CP:
        logging.info("The current phase is Checkpointing phase")
        time.sleep(CHECKPOINT_INTERVAL)
        self.cp_phase()

      elif current_phase == PHASE.LAST_CP:
        logging.info("The current phase is Last Checkpoint phase")
        self.last_cp_phase()

      elif current_phase == PHASE.EXITING:
        logging.info("The current phase is Exiting phase")
        self.exit_phase(start_time)

      else:
        logging.error("Unknown global phase. Exiting!")
        break
        
class Coordinator(Process):
  def __init__(self) -> None:
    super().__init__()
    self.global_state = GlobalState()
    self.phase_queue: queue.Queue[PHASE]

    for i, mp in enumerate(MAPPER_PORTS):
      m = WorkerState(i, True, ("localhost", mp))
      self.global_state.workers[m.id] = m

    for i, rp in enumerate(REDUCER_PORTS):
      r = WorkerState(i, False, ("localhost", rp))
      self.global_state.workers[r.id] = r

  def initialize_workers(self):
    # start reducers
    for _, ws in self.global_state.workers.items():
      if not ws.is_mapper:
        ws.start_worker()

    time.sleep(2)

    # start mappers
    for _, ws in self.global_state.workers.items():
      if ws.is_mapper:
        ws.start_worker()

  def run(self):
    self.phase_queue = queue.Queue()
    self.phase_queue.put(PHASE.CP)

    self.global_state.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    self.global_state.sock.bind(("localhost", COORDINATOR_PORT))
    self.initialize_workers()

    st = SendThread("Coordinator", os.getpid(), self.global_state, self.phase_queue)
    rt = RecvThread(self.global_state, self.phase_queue)

    st.start()
    rt.start()

    st.join()
    rt.join()

