from mlopsdl.logger import logging
from mlopsdl.exception import MLOpsException
import sys

try:
    logging.info("Starting the demo.py script")
    a = 1 / 0
except Exception as e:
    raise MLOpsException(str(e), sys.exc_info())


# logging.info("Welcome to MLOps DL Project custom log")