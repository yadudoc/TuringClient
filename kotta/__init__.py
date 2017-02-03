from kotta.kotta import Kotta
from kotta.kotta_job import KottaJob
from kotta.kotta_functions import *
import logging

__author__  = 'Yadu Nand Babuji'
__version__ = '0.1.0'

__all__ = [Kotta, KottaJob, KottaFn, kottajob]


def set_stream_logger(name='kotta', level=logging.DEBUG, format_string=None):
    ''' Add a stream log handler
    '''

    if format_string is None:
        format_string = "%(asctime)s %(name)s [%(levelname)s] %(message)s"

    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.StreamHandler()
    handler.setLevel(level)
    formatter = logging.Formatter(format_string)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


class NullHandler (logging.Handler):
    ''' Setup default logging to /dev/null since this is library.
    '''

    def emit(self, record):
        pass



logging.getLogger('kotta').addHandler(NullHandler())
