import datetime
import logging


def get_app_logger(name):
    app_logger = logging.getLogger(name)
    app_logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    app_logger.addHandler(ch)
    return app_logger


logger = get_app_logger(__name__)


def log_performance(func):
    def wrapper(*args, **kwargs):
        logger.debug('Start {} at {}'.format(func.__name__, datetime.datetime.now()))
        result = func(*args, **kwargs)
        logger.debug('End {} at {}'.format(func.__name__, datetime.datetime.now()))
        return result

    return wrapper
