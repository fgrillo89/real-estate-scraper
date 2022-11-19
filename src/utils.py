from time import perf_counter
from functools import wraps


def func_timer(debug):

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


