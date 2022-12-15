from time import perf_counter
from functools import wraps
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
import pandas as pd
import csv
from logger import logger

now = datetime.now


def get_timestamp(date_only=False):
    tms = now().replace(tzinfo=ZoneInfo("Europe/Amsterdam"))
    if date_only:
        return tms.strftime("%Y-%m-%d")
    return tms.isoformat()


def func_timer(debug=True):
    def inner(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not debug:
                return func(*args, **kwargs)

            t0 = perf_counter()
            result = func(*args, **kwargs)
            tf = perf_counter()
            logger.info(f"{func.__name__} completed in {(tf - t0):.4f} s")
            return result

        return wrapper

    return inner


if __name__ == "__main__":
    f = get_timestamp
