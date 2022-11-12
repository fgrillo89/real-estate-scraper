import json
from pathlib import Path
from typing import Union
from dataclasses import dataclass

import pandas as pd
from pipe import traverse, select, sort, where
import asyncio
import re
from enum import Enum
from city_scraper import Scraper
from parsing import str_from_tag

config_path = Path.cwd() / 'config' / 'config.json'

with open(config_path) as json_file:
    api_config = json.load(json_file)

HEADER = api_config['headers']
URL = "https://www.funda.nl/en/koop/{}/p{}"


class HouseDetailsShallow(Enum):
    address = 'Address'
    postcode = 'PostCode'
    living_area = 'LivingArea'
    plot_size = 'PlotSize'
    price = 'Price'
    rooms = 'Rooms'


def extract_number_of_rooms(soup):
    result = soup.find('ul', class_="search-result-kenmerken").find_all('li')
    if len(result) > 1:
        return result[1]


detail_method_map_sh = {HouseDetailsShallow.address: lambda soup: soup.find('h2'),
                        HouseDetailsShallow.postcode: lambda soup: soup.find('h4'),
                        HouseDetailsShallow.living_area: lambda soup: soup.find(attrs={'title': 'Living area'}),
                        HouseDetailsShallow.plot_size: lambda soup: soup.find(attrs={'title': 'Plot size'}),
                        HouseDetailsShallow.price: lambda soup: soup.find('span', class_='search-result-price'),
                        HouseDetailsShallow.rooms: extract_number_of_rooms
                        }


def parse_shallow_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    for key in [HouseDetailsShallow.price,
                HouseDetailsShallow.plot_size,
                HouseDetailsShallow.living_area,
                HouseDetailsShallow.rooms]:
        df[key.value] = pd.to_numeric(df[key.value].str.replace('\D+', '', regex=True))

    return df


class FundaScraper(Scraper):
    def __init__(self, **kwargs):
        super().__init__(header=HEADER, main_url=URL, **kwargs)

    def scrape_city(self, city, max_pp=None, method='shallow'):
        pass

    async def _get_number_of_pages_and_listings(self, city='heel-nederland'):
        soup = await self._get_soup(city, 1)
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
        return asyncio.run(self.scrape_shallow_async(city, pages))

    async def scrape_shallow_async(self, city='heel-nederland', pages: Union[None, list[int]] = None) -> pd.DataFrame:
        house_data = []
        if pages is None:
            max_pp, _ = await self._get_number_of_pages_and_listings(city)
            pages = range(1, max_pp + 1)

        soups = await asyncio.gather(*(self._get_soup(city, i) for i in pages))

        for soup in soups:
            results = self.get_main_page_results(soup)
            houses = [self.get_house_details(result) for result in results]
            house_data = house_data + houses

        return parse_shallow_dataframe(pd.DataFrame(house_data))

    @staticmethod
    def get_main_page_results(soup):
        return soup.find_all('div', class_="search-result-content-inner")

    @staticmethod
    def get_house_details(result):
        house = {}

        for detail in HouseDetailsShallow:
            house[detail.value] = str_from_tag(detail_method_map_sh[detail](result))

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
