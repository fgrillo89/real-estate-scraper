import asyncio
from asyncio import Semaphore
from typing import Union

from aiolimiter import AsyncLimiter
from bs4 import BeautifulSoup

from real_estate_scraper.html_handling import get_soup, add_limiter, add_semaphore

MAX_ACTIVE_REQUESTS = 5
REQUESTS_PER_SECOND = 5

limiter = AsyncLimiter(1, round(1 / REQUESTS_PER_SECOND, 3))
semaphore = Semaphore(value=MAX_ACTIVE_REQUESTS)


@add_limiter(limiter)
@add_semaphore(semaphore)
async def fetch_soup(url, header):
    return await get_soup(url, header=header)


def get_soups(urls: Union[str, list[str]],
              header=None) -> list[BeautifulSoup]:
    if isinstance(urls, str):
        urls = [urls]

    async def fetch_all():
        return await asyncio.gather(*(fetch_soup(url, header=header) for url in urls))

    return asyncio.run(fetch_all())


def main():
    urls = ["https://www.funda.nl/en/koop/hoogvliet-rotterdam/huis-42054036-schakelweg"
            "-108/",
            "https://www.funda.nl/en/koop/rotterdam/huis-42942117-cantecleerpad-8/",
            "https://www.funda.nl/en/koop/rotterdam/huis-42942750-noorder-kerkedijk-33/",
            "https://www.funda.nl/en/koop/rotterdam/huis-42945676-menorcalaan-18/",
            "https://www.funda.nl/en/koop/rotterdam/huis-42934983-koningsvaren-59/",
            "https://www.funda.nl/en/koop/rotterdam/huis-88358067-rotterdamse-rijweg-14/",
            "https://www.funda.nl/en/koop/rotterdam/huis-42999847-hazewinkelpad-4/",
            "https://www.funda.nl/en/koop/hoogvliet-rotterdam/huis-88335400-reinier"
            "-kloegstraat-101/",
            "https://www.funda.nl/en/koop/hoogvliet-rotterdam/huis-42976894-bongweg-77/",
            "https://www.funda.nl/en/koop/hoogvliet-rotterdam/huis-42954925-kaneelhof-34/"]
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ("
                      "KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
    }

    soups = get_soups(urls, header=headers)
    return soups


if __name__ == "__main__":
    results = main()
