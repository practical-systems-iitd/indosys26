import glob
import argparse
import os
import shutil

from constants import STREAMS, NUM_MAPPERS
from coordinator import Coordinator
from mrds import MyRedis
from mylog import Logger

logging = Logger().get_logger()


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument(
      "recovery_test_mode",
      type=Coordinator.RecoveryTestModes,
      choices=list(Coordinator.RecoveryTestModes),
  )
  opts = parser.parse_args()

  Logger()
  rds = MyRedis()
  COUNT = 5001
  for j in range(1, COUNT):
      rds.add_file(STREAMS[j % NUM_MAPPERS], "file.csv", j)


  # Kill any process that are using these ports
  # kill_process_on_port(*(MAPPER_PORTS + REDUCER_PORTS))
  
  ckpt_dir = "checkpoints/"
  if os.path.exists(ckpt_dir):
    shutil.rmtree(ckpt_dir)
  os.makedirs(ckpt_dir)

  C = Coordinator(opts.recovery_test_mode)
  C.start()
  C.join()
