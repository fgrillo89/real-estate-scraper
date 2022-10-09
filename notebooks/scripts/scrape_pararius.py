import asyncio

import pandas as pd
from requests import get as requests_get

import aiohttp
from bs4 import BeautifulSoup
from matplotlib import pyplot as plt

CITY = "rotterdam"
BASE_URL = "https://www.pararius.nl/koopwoningen/"


def get_url(city: str, page_nr: int) -> str:
    return f"{BASE_URL}{city}/page-{page_nr}"


def get_soup(city: str, page_nr: int, method='lxml') -> BeautifulSoup:
    url = get_url(city, page_nr)
    page = requests_get(url)
    return BeautifulSoup(page.text, method)


def estimate_num_pages(city: str, page_nr=1) -> int:
    soup = get_soup(city, page_nr)
    results = soup.find('div', {'class': 'pagination__summary'}).get_text()
    current_max = int(results[results.find("-") + 2:results.find(" van")])
    total = int(results[results.find("van") + 4:results.find(" resultaten")])
    print(f"{current_max} out of {total}")
    return total // current_max + int(total % current_max > 0)


def get_prices_from_page(city: str, page_nr: int):
        soup = get_soup(city, page_nr)
        print(page_nr)
        return soup.find_all('div', {'class': 'listing-search-item__price'})


def get_tasks(session, page_nr_max):
    tasks = []
    for i in range(1, page_nr_max + 1):
        tasks.append(asyncio.create_task(session.get(get_url(CITY, i), ssl=False)))
    return tasks

#
# async def get_responses(page_nr_max):
#     async  with aiohttp.ClientSession() as session:
#         tasks = get_tasks(session, page_nr_max)
#         responses = await asyncio.gather(*tasks)
#         for response in responses
#         # prices = []
#         # for response in responses:
#         #     soup = await BeautifulSoup(response.text(), 'lxml')
#         #     prices = prices + soup.find_all('div', {'class': 'listing-search-item__price'})

async def main():
    # page_nr_max = estimate_num_pages(CITY)
    res = await asyncio.gather(asyncio.to_thread(get_soup(CITY,1)),
                          asyncio.to_thread(get_soup(CITY,2)))

    # res = [get_prices_from_page(CITY, page_nr) for page_nr in range(1, 2)]
    return res


if __name__ == '__main__':
    result = asyncio.run(main())
    #
    # from bs4 import BeautifulSoup
    # from selenium import webdriver
    # from webdriver_manager.firefox import GeckoDriverManager
    #
    # driver = webdriver.Firefox()
    #
    # driver.get(get_url(CITY, 2))
    #
    # html = driver.page_source
    # soup = BeautifulSoup(html)
    # print(soup)
    # for c in cookies:
    #     cookie = {'domain': c.domain, 'name': c.name, 'value': c.value, 'secure': c.secure and True or False}
    #     driver.add_cookie(cookie)
    # driver.get('http://www.google.com')

