import asyncio
import aiohttp
import os
import time
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import browser_cookie3
ua = UserAgent(use_cache_server=True)



CITY = "rotterdam"
BASE_URL = "https://www.pararius.nl/koopwoningen/"


def get_url(city: str, page_nr: int) -> str:
    return f"{BASE_URL}{city}/page-{page_nr}/"


def estimate_num_pages(city: str, page_nr=1) -> int:
    soup = get_soup(city, page_nr)
    results = soup.find('div', {'class': 'pagination__summary'}).get_text()
    current_max = int(results[results.find("-") + 2:results.find(" van")])
    total = int(results[results.find("van") + 4:results.find(" resultaten")])
    print(f"{current_max} out of {total}")
    return total // current_max + int(total % current_max > 0)



def get_tasks(session, page_nr_max):
    tasks = []
    for i in range(1, page_nr_max + 1):
        tasks.append(asyncio.create_task(fetch(session, get_url(CITY, i))))
    return tasks

async def fetch(session, url):
    async with session.get(url, ssl=False, allow_redirects=True) as response:
        text = await response.text()
        # prices = await extract_prices(text)
        return text


async def extract_prices(text):
    soup = BeautifulSoup(text, 'lxml')
    return soup.find_all('div', {'class': 'listing-search-item__price'})


start = time.time()


ck = browser_cookie3.chrome(domain_name='.pararius.nl')
# cookies = {c.name: c.value for c in ck}
headers = {'User-agent': ua.random}
async def get_symbols():
    async with aiohttp.ClientSession(headers=headers) as session:
        results = await asyncio.gather(*get_tasks(session, 60))
    return results


res = asyncio.new_event_loop().run_until_complete(get_symbols())
end = time.time()

print(f"{start-end}")
# res = asyncio.run(get_symbols())

# if __name__ == '__main__':
