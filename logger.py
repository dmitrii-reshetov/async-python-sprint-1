import logging
from settings import LOGS_DIR


logging.basicConfig(level=logging.CRITICAL)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False

log_format = '%(asctime)s|%(filename)s:%(lineno)d|' \
             '%(processName)s:%(threadName)s|%(levelname)s|%(message)s'
formatter = logging.Formatter(log_format)

f_handler = logging.FileHandler(LOGS_DIR / 'main.log')
f_handler.setFormatter(formatter)
f_handler.setLevel(logging.DEBUG)

s_handler = logging.StreamHandler()
s_handler.setFormatter(formatter)
s_handler.setLevel(logging.WARNING)

logger.addHandler(f_handler)
logger.addHandler(s_handler)