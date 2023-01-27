import asyncio
from asyncio import Semaphore
from typing import Union, Optional

from aiolimiter import AsyncLimiter
from bs4 import BeautifulSoup
from pipe import traverse

from real_estate_scraper.configuration import ItemContent
from real_estate_scraper.html_handling import get_soup, add_limiter, add_semaphore
from real_estate_scraper.parsing import str_from_tag
from real_estate_scraper.utils import camelcase

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


def extract_all_dt(soup: BeautifulSoup) -> Optional[list[dict[str, str]]]:
    dt_list = soup.find_all("dt")
    if dt_list:
        return [str_from_tag(dt) for dt in dt_list]
    return None


def extract_all_dd_text(soup: BeautifulSoup, dt_names: list[str]):
    items = {}
    for dt_name in dt_names:
        items[dt_name] = get_dd_text_from_dt_name(soup, dt_name)
    return items


def item_content_from_dt_names(items_text: list[str]) -> list[dict[ItemContent]]:
    items = {}
    for text in items_text:
        items[camelcase(text)] = {"text_in_website": text, "type": "text"}
    return items


def main():
    urls = ['https://www.immobiliare.it/annunci/98531352/',
            'https://www.immobiliare.it/annunci/95014054/',
            'https://www.immobiliare.it/annunci/99087248/',
            'https://www.immobiliare.it/annunci/99709996/',
            'https://www.immobiliare.it/annunci/92273868/',
            'https://www.immobiliare.it/annunci/100508874/',
            'https://www.immobiliare.it/annunci/99742246/',
            'https://www.immobiliare.it/annunci/100035388/',
            'https://www.immobiliare.it/annunci/98811448/',
            'https://www.immobiliare.it/annunci/99706304/',
            'https://www.immobiliare.it/annunci/99509624/',
            'https://www.immobiliare.it/annunci/100160892/',
            'https://www.immobiliare.it/annunci/93174362/',
            'https://www.immobiliare.it/annunci/100603786/',
            'https://www.immobiliare.it/annunci/94219346/',
            'https://www.immobiliare.it/annunci/100027704/',
            'https://www.immobiliare.it/annunci/99100210/',
            'https://www.immobiliare.it/annunci/97474264/',
            'https://www.immobiliare.it/annunci/94935172/',
            'https://www.immobiliare.it/annunci/100632063/']

    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ("
                      "KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
    }

    soups = get_soups(urls, header=headers)

    unique_items = set([extract_all_dt(soup) for soup in soups] | traverse)

    return soups, unique_items


if __name__ == "__main__":
    results, items = main()
