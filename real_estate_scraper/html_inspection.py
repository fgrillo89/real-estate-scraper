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


def get_dd_text_from_dt_name(soup, text_in_website):
    dt = soup.find(
        lambda tag: tag.name == "dt" and text_in_website.lower() in tag.text.lower()
    )
    if dt:
        return ','.join(list(dt.find_next("dd").stripped_strings))
    else:
        return None


def main():
    url_template = "https://www.immobiliare.it/vendita-case/{}/?criterio=rilevanza&pag={}"

    get_url = lambda city, page: url_template.format(city, page)

    pages = [1, 2, 3]
    urls = [get_url("roma", page) for page in pages]
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ("
                      "KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
    }

    soups = get_soups(urls, header=headers)
    return soups


if __name__ == "__main__":
    results = main()
