from time import perf_counter
from functools import wraps
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

now = datetime.now


def get_timestamp():
    tms = now().replace(tzinfo=ZoneInfo("Europe/Amsterdam"))
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
            print(f"{func.__name__} completed in {(tf - t0):.4f} s")
            return result

        return wrapper

    return inner


def to_csv(df, filepath, index=False, mode='a', encoding='utf-8', **kwargs):
    return df.to_csv(filepath, index=index, mode=mode, encoding=encoding, **kwargs)


async def df_to_file_async(df: pd.DataFrame, filepath: str, file_format: str = 'csv', **kwargs):
    FORMAT_map = {'csv': to_csv}
    return await asyncio.to_thread(FORMAT_map[file_format], df, filepath, **kwargs)


if __name__ == "__main__":
    f = get_timestamp
