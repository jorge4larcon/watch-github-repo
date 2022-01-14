"""Utils for the program."""

from datetime import datetime
import logging


def console_logger(name=None, level: int = logging.INFO):
    """Get a basic console logger."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    ch = logging.StreamHandler()
    ch.setLevel(level)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


def utc2datetime(utc_date: str) -> datetime:
    """
    Takes a date represented in a string in UTC format and returns a datetime
    object.
    """
    return datetime.strptime(utc_date, '%Y-%m-%dT%H:%M:%SZ')


def datetime2utc(datetime_obj: datetime) -> str:
    """
    Takes a datetime object and converts it into a string in UTC format.
    """
    return datetime_obj.strftime('%Y-%m-%dT%H:%M:%SZ')
