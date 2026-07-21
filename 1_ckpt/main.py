import glob
import os
import shutil

from constants import STREAMS, NUM_MAPPERS
from coordinator import Coordinator
from mrds import MyRedis
from mylog import Logger

logging = Logger().get_logger()

if __name__ == "__main__":
  Logger()
  rds = MyRedis()

  COUNT = 5001
  for j in range(1, COUNT):
      rds.add_file(STREAMS[j % NUM_MAPPERS], "file.csv", j)

  ckpt_dir = "checkpoints/"
  if os.path.exists(ckpt_dir):
    shutil.rmtree(ckpt_dir)
  os.makedirs(ckpt_dir)

  C = Coordinator()
  C.start()
  C.join()
