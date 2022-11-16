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

config_path = Path.cwd() / 'config' / 'config.json'

with open(config_path) as json_file:
    api_config = json.load(json_file)

HEADER = api_config['headers']
URL = "https://www.funda.nl/en/koop/{}/p{}"


def extract_number_of_rooms(soup):
    result = soup.find('ul', class_="search-result-kenmerken").find_all('li')
    if len(result) > 1:
        return result[1]


HouseAttribute = namedtuple("Attribute", "name type")

AttributesShallow = [('Address', 'text'),
                     ('PostCode', 'text'),
                     ('LivingArea', 'numeric'),
                     ('PlotSize', 'numeric'),
                     ('Price', 'numeric'),
                     ('Rooms', 'numeric'),
                     ('href', 'text'),
                     ('HouseId', 'text')
                     ]

house_shallow = Enum('HouseShallow', {attribute[0]: HouseAttribute(*attribute) for attribute in AttributesShallow})

detail_method_map_sh = {house_shallow.Address: lambda soup: soup.find('h2'),
                        house_shallow.PostCode: lambda soup: soup.find('h4'),
                        house_shallow.LivingArea: lambda soup: soup.find(attrs={'title': 'Living area'}),
                        house_shallow.PlotSize: lambda soup: soup.find(attrs={'title': 'Plot size'}),
                        house_shallow.Price: lambda soup: soup.find('span', class_='search-result-price'),
                        house_shallow.Rooms: extract_number_of_rooms,
                        house_shallow.href: lambda soup: soup.find('a', attrs={"data-object-url-tracking"
                                                                               : "resultlist"})
                                                             .get('href'),
                        house_shallow.HouseId: lambda soup: soup.find('a', attrs={"data-object-url-tracking"
                                                                                  : "resultlist"})
                                                                .get('data-search-result-item-anchor')
                        }


class FundaScraper(Scraper):
    def __init__(self, **kwargs):
        super().__init__(header=HEADER, main_url=URL, **kwargs)

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

        return parse_shallow_dataframe(house_shallow, df)

    def scrape_deep(self, city: str, pages: Union[None, list[int]]):
        pass

    @staticmethod
    def get_main_page_results(soup):
        return soup.find_all('div', class_="search-result-content-inner")

    @staticmethod
    def get_house_details_sh(soup):
        house = {}

        for detail in house_shallow:

            retrieved_detail = detail_method_map_sh[detail](soup)

            if isinstance(retrieved_detail, Tag):
                retrieved_detail = str_from_tag(retrieved_detail)

            house[detail.name] = retrieved_detail

        return house

    # async def get_all_children_urls(self, ):
    #     max_pp = await get_num_pp_from_main_page(url.format(1))
    #     # max_pp = 150
    #     htmls = await asyncio.gather(*(get_html(url.format(i)) for i in range(1, max_pp + 1)))
    #     # urls = await asyncio.gather(*(extract_urls_from_main_page(html) for html in htmls))
    #     return htmls


if __name__ == '__main__':
    scraper = FundaScraper()
    results = scraper.scrape_shallow(city='Delft', pages=None)
