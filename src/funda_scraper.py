import json
from collections import namedtuple
from enum import Enum
from pathlib import Path
from typing import Union
from time import perf_counter
from bs4.element import Tag

import pandas as pd
from pipe import traverse, select, sort, where
import asyncio
import re
from scraper import Scraper
from parsing import str_from_tag, parse_shallow_dataframe
from config_loader import config_loader

config_path = Path.cwd() / 'config' / 'config.json'
config = config_loader(config_path)

MAIN_URL = config['website']['main_url']
CITY_SEARCH_URL = config['website']['city_search_url_template']
HEADER = config['header']

house_shallow = config['house_attributes_shallow']


def extract_number_of_rooms(soup):
    result = soup.find('ul', class_="search-result-kenmerken").find_all('li')
    if len(result) > 1:
        return result[1]


attribute_func_map_sh = {'Address': lambda soup: soup.find('h2'),
                         'PostCode': lambda soup: soup.find('h4'),
                         'LivingArea': lambda soup: soup.find(attrs={'title': 'Living area'}),
                         'PlotSize': lambda soup: soup.find(attrs={'title': 'Plot size'}),
                         'Price': lambda soup: soup.find('span', class_='search-result-price'),
                         'Rooms': extract_number_of_rooms,
                         'href': lambda soup: soup.find('a', attrs={"data-object-url-tracking": "resultlist"})
                                                  .get('href'),
                         'HouseId': lambda soup: soup.find('a', attrs={"data-object-url-tracking": "resultlist"})
                                                     .get('data-search-result-item-anchor')
                         }

for attr in attribute_func_map_sh:
    house_shallow.map_function_to_attribute(attr, attribute_func_map_sh[attr])

class FundaScraper(Scraper):
    def __init__(self, **kwargs):
        super().__init__(header=HEADER,
                         main_url=MAIN_URL,
                         city_search_url=CITY_SEARCH_URL,
                         house_attributes_shallow=house_shallow,
                         **kwargs)

    def scrape_city(self, city, max_pp=None, method='shallow'):
        pass

    async def _get_number_of_pages_and_listings(self, city='heel-nederland'):
        soup = await self._get_soup_main_url(city=city, page=1)
        pp_soup = soup.find("div", class_="pagination-pages")
        num_pages = list(pp_soup
                         | select(lambda x: re.findall('\d+', x.text))
                         | traverse
                         | select(lambda x: int(x))
                         | sort(reverse=True)
                         )[0]
        num_listings = json.loads(soup.find_all('script', type="application/ld+json")[2].text)['results_total']
        return num_pages, num_listings

    def scrape_shallow(self, city='heel-nederland', pages: Union[None, list[int]] = None) -> list:
        t0 = perf_counter()
        res = asyncio.run(self.scrape_shallow_async(city, pages))
        time_elapsed = round(perf_counter() - t0, 3)
        print(f"{time_elapsed=} s")
        return res

    async def scrape_shallow_async(self, city='heel-nederland', pages: Union[None, list[int]] = None) -> pd.DataFrame:
        if pages is None:
            max_pp, _ = await self._get_number_of_pages_and_listings(city)
            pages = range(1, max_pp + 1)

        soups = await asyncio.gather(*(self._get_soup_main_url(city, i) for i in pages))

        house_data = []
        for soup in soups:
            results = self.get_main_page_results(soup)
            houses = [self.get_house_details_sh(result) for result in results]
            house_data = house_data + houses

        df = pd.DataFrame(house_data)

        return parse_shallow_dataframe(self.house_attributes_shallow, df)

    def scrape_deep(self, city: str, pages: Union[None, list[int]]):
        pass

    def get_house_details_sh(self, soup):
        house = {}
        for attribute in self.house_attributes_shallow:
            retrieved_attribute = attribute.retrieve_func(soup)
            if isinstance(retrieved_attribute, Tag):
                retrieved_attribute = str_from_tag(retrieved_attribute)
            house[attribute.name] = retrieved_attribute
        return house

    @staticmethod
    def get_main_page_results(soup):
        return soup.find_all('div', class_="search-result-content-inner")


    # async def get_all_children_urls(self, ):
    #     max_pp = await get_num_pp_from_main_page(url.format(1))
    #     # max_pp = 150
    #     htmls = await asyncio.gather(*(get_html(url.format(i)) for i in range(1, max_pp + 1)))
    #     # urls = await asyncio.gather(*(extract_urls_from_main_page(html) for html in htmls))
    #     return htmls


if __name__ == '__main__':
    scraper = FundaScraper()
    results = scraper.scrape_shallow(city='Delft', pages=[1, 2])
