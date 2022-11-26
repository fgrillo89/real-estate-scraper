from time import perf_counter
from functools import wraps
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
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


async def df_to_json_async(df, filepath):
    return await asyncio.to_thread(df.to_csv, filepath, index=False, mode='a')


if __name__ == "__main__":
    f = get_timestamp