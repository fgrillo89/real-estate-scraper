import asyncio
from asyncio import Semaphore

import requests
from aiolimiter import AsyncLimiter

#


def get_html_synch(url_str: str, header: dict) -> str:
    response = requests.request("GET", url_str, headers=header)
    print(f"Done requesting: {url_str}")
    return response.text


async def get_html(url: str, header: dict) -> str:
    return await asyncio.to_thread(get_html_synch, url, header)


if __name__ == '__main__':
    pass
    # from city_scraper import HEADERS_dict, get_url
    # url = get_url('funda', 'Delft', 1)
    # max_active_requests = 5
    # lim = AsyncLimiter(max_active_requests, 1)
    # sem = Semaphore(value=max_active_requests)
    #
    # html = asyncio.run(get_html(url, HEADERS_dict['funda'], lim, sem))
