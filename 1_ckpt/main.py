import glob

from constants import STREAMS, NUM_MAPPERS
from coordinator import Coordinator
from mrds import MyRedis
from mylog import Logger

logging = Logger().get_logger()


if __name__ == "__main__":
  Logger()
  rds = MyRedis()
  pattern = "csv_files/*.csv"

  j: int = 1
  for file in glob.glob(pattern):
      rds.add_file(STREAMS[j % NUM_MAPPERS], file, j)
      j += 1

  ckpt_dir = "checkpoints/"
  if os.path.exists(ckpt_dir):
    shutil.rmtree(ckpt_dir)
  os.makedirs(ckpt_dir)


  C = Coordinator()
  C.start()
  C.join()
