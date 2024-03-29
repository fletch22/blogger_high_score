import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from bhs.config import constants


class LoggerFactory():
    rot_handler = None
    all_loggers = []

    def __init__(self):
        self.formatter = logging.Formatter('%(asctime)s - %(levelname)s [%(filename)s:%(lineno)d] %(message)s', datefmt='%H:%M:%S')

        self.handler_stream = logging.StreamHandler(sys.stdout)
        self.handler_stream.setFormatter(self.formatter)
        self.handler_stream.setLevel(logging.INFO)

        print(f'Will use logging path: {constants.LOGGING_PATH}')

        log_path = Path(constants.LOGGING_PATH, 'blogger_high_score.log')

        self.rot_handler = RotatingFileHandler(str(log_path), maxBytes=200000000, backupCount=10)
        self.rot_handler.setFormatter(self.formatter)
        self.rot_handler.setLevel(logging.INFO)

    def create_instance(self, name: str):
        logger = logging.getLogger(name)
        logger.addHandler(self.handler_stream)
        logger.addHandler(self.rot_handler)

        logger.setLevel(logging.INFO)
        self.all_loggers.append(logger)

        return logger
