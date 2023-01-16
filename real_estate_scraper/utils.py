from datetime import datetime
from functools import wraps, reduce
from math import ceil
from time import perf_counter
from typing import Callable
from zoneinfo import ZoneInfo


now = datetime.now


def get_timestamp(date_only=False) -> str:
    """Get timestamp with timezone info in iso8601 format as a string"""
    tms = now().replace(tzinfo=ZoneInfo("Europe/Amsterdam"))
    if date_only:
        return tms.strftime("%Y-%m-%d")
    return tms.isoformat()


def func_timer(active=True):
    def inner(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            if not active:
                return func(*args, **kwargs)

            t0 = perf_counter()
            result = func(self, *args, **kwargs)
            tf = perf_counter()
            self.logger.info(f"{func.__name__} completed in {(tf - t0):.4f} s")
            return result

        return wrapper

    return inner


def compose_functions(*func: Callable) -> Callable:
    def compose(f, g):
        return lambda x: f(g(x))
    return reduce(compose, func, lambda x: x)


def split_list(input_list: list, chunksize: int) -> list:
    size = len(input_list)
    n_chunks = ceil(size / chunksize)

    chunks = []
    for i in range(0, n_chunks):
        chunks.append(input_list[i * chunksize : i * chunksize + chunksize])
    return chunks


if __name__ == "__main__":
    f = get_timestamp
