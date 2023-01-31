import asyncio
import logging
from asyncio import Semaphore
from functools import wraps
from typing import Union, Optional

import aiohttp
from aiohttp import ClientResponseError
from aiolimiter import AsyncLimiter
from bs4 import BeautifulSoup


async def get_response(url_str: str,
                       header: dict,
                       read_format: str = "text",
                       max_retries: int = 3,
                       timeout: int = 10,
                       logger: Optional[logging.Logger] = None) -> Union[str, dict, list]:
    retries = 0
    status = None
    while retries < max_retries:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url_str,
                                       headers=header,
                                       timeout=timeout) as response:
                    status = response.status
                    response.raise_for_status()
                    return await process_response(response, read_format=read_format)

        except (asyncio.TimeoutError, ClientResponseError) as e:
            if status and status == 404:
                msg = f"Error 404, {e}"
                if logger:
                    logger.warning(msg)
                else:
                    print(msg)
                raise e
                return None
            retries += 1
            if retries == max_retries:
                raise e
            msg = f"Retrying request to {url_str} (attempt {retries}/{max_retries})" \
                  f"because of {e}"
            if logger:
                logger.warning(msg)
            else:
                print(msg)
        except Exception as e:
            msg = f"Could not request {url_str} because of {e}"
            if logger:
                logger.warning(msg)
            else:
                print(msg)
            raise e


async def process_response(response: aiohttp.ClientResponse, read_format: str = "text") \
        -> Union[str, dict, list]:
    method_factory = {"text": lambda x: x.content.read(),
                      "json": lambda x: x.content.json()}
    return await method_factory[read_format](response)


def add_limiter(limiter: AsyncLimiter):
    def inner(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            async with limiter:
                return await func(*args, **kwargs)

        return wrapper

    return inner


def add_semaphore(semaphore: Semaphore):
    def inner(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            async with semaphore:
                return await func(*args, **kwargs)

        return wrapper

    return inner


async def get_soup(url: str,
                   header: Optional[dict[str]] = None,
                   parse_only: Optional[list[str]] = None,
                   logger: Optional[logging.Logger] = None) -> BeautifulSoup:
    response = await get_response(url, header=header, logger=logger)
    if response:
        return BeautifulSoup(response, "lxml", parse_only=parse_only)


async def get_json(url: str,
                   header: Optional[dict[str]] = None,
                   logger: Optional[logging.Logger] = None) -> BeautifulSoup:
    return await get_response(url, header=header, logger=logger, read_format='json')


# def get_response_synch(url_str: str, header: dict) -> requests.Response:
#     return requests.request("GET", url_str, headers=header)


# async def get_response(url: str, header: dict) -> requests.Response:
#     return await asyncio.to_thread(get_response_synch, url, header)

if __name__ == "__main__":
    url = "https://www.funda.nl/en/koop/eede/huis-88953000-scheidingstraat-27/"
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ("
                      "KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
    }

    limit = AsyncLimiter(1, round(1 / 5, 3))
    sem = Semaphore(value=5)


    @add_semaphore(sem)
    @add_limiter(limit)
    async def limited_get_soup(*args, **kwargs):
        return get_soup(*args, **kwargs)


    soup = asyncio.run(get_soup(url=url, header=headers))
