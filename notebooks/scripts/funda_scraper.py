import json
from pathlib import Path
import asyncio
import requests
import lxml
import cchardet
from bs4 import BeautifulSoup
from pipe import traverse, select, sort, where
from matplotlib import pyplot as plt
import re
import numpy as np
import json
import pandas as pd
from aiolimiter import AsyncLimiter


limiter = AsyncLimiter(5, 1)

config_path = Path.cwd().parent / 'config' / 'config.json'

with open(config_path) as json_file:
    api_config = json.load(json_file)

HEADERS = api_config['headers']
URL = "https://www.funda.nl/en/koop/delft/p{}"


def get_html_synch(url):
    response = requests.request("GET", url, headers=HEADERS)
    print(url)
    return response.text


async def get_html(url):
    async with limiter:
        html = await asyncio.to_thread(get_html_synch, url)
    return html


async def get_num_pp_from_main_page(url):
    html = await get_html(url)
    soup = BeautifulSoup(html, 'lxml')
    pp_soup = soup.find("div", class_="pagination-pages")
    max_num_pages = list(pp_soup
                         | select(lambda x: re.findall('\d+', x.text))
                         | traverse
                         | select(lambda x: int(x))
                         | sort(reverse=True)
                         )[0]
    return max_num_pages


def extract_urls_from_main_page_synch(html):
    soup = BeautifulSoup(html, 'lxml')
    try:
        data = json.loads(soup.find_all('script', type='application/ld+json')[3].text)
        return [item['url'] for item in data['itemListElement']]
    except IndexError as e:
        print(html)
        return e


async def extract_urls_from_main_page(soup):
    return await asyncio.to_thread(extract_urls_from_main_page_synch, soup)


async def get_all_children_urls(url):
    max_pp = await get_num_pp_from_main_page(url.format(1))
    # max_pp = 150
    htmls = await asyncio.gather(*(get_html(url.format(i)) for i in range(1, max_pp + 1)))
    # urls = await asyncio.gather(*(extract_urls_from_main_page(html) for html in htmls))
    return htmls


if __name__ == '__main__':
    semaphore = asyncio.Semaphore(value=10)
    # pp = asyncio.run(get_all_children_urls(URL))
    pp = asyncio.run(get_all_children_urls(URL))
