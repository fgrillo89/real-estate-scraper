from time import perf_counter
from functools import wraps
import asyncio
from datetime import datetime

now = datetime.now


def get_timestamp():
    return now().strftime("%d/%m/%Y, %H:%M:%S")


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


async def df_to_json_async(df, filepath):
    return await asyncio.to_thread(df.to_csv, filepath, index=False, mode='a')
