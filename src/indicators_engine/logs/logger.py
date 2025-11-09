import logging
import os
from logging.handlers import RotatingFileHandler

# Siempre usar la raíz (donde se ejecuta el main)
LOG_DIR = os.path.abspath(os.path.join(os.getcwd(), 'var', 'logs'))
LOG_LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'error': logging.ERROR,
}
LOG_FILES = {
    'debug': os.path.join(LOG_DIR, 'debug.log'),
    'info': os.path.join(LOG_DIR, 'info.log'),
    'error': os.path.join(LOG_DIR, 'error.log'),
}

os.makedirs(LOG_DIR, exist_ok=True)

class LevelFilter(logging.Filter):
    def __init__(self, level):
        super().__init__()
        self.level = level
    def filter(self, record):
        return record.levelno == self.level

def get_logger(name='indicators_engine'):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    if not getattr(logger, '_custom_handlers', False):
        # Handler por nivel
        for level_name, level in LOG_LEVELS.items():
            handler = RotatingFileHandler(
                LOG_FILES[level_name],
                maxBytes=5*1024*1024,  # 5MB por archivo
                backupCount=3
            )
            handler.setLevel(level)
            handler.addFilter(LevelFilter(level))
            formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(name)s: %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        logger._custom_handlers = True
    return logger
