import asyncio

import requests


def get_response_synch(url_str: str, header: dict) -> requests.Response:
    return requests.request("GET", url_str, headers=header)


async def get_response(url: str, header: dict) -> requests.Response:
    return await asyncio.to_thread(get_response_synch, url, header)


if __name__ == "__main__":
    pass
