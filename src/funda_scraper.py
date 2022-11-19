import asyncio
import json
import re
from pathlib import Path
from time import perf_counter
from typing import Union
from functools import partial

import pandas as pd
from bs4.element import Tag
from bs4 import BeautifulSoup
from pipe import traverse, select, sort

from config_loader import config_loader, AttributesEnum
from parsing import str_from_tag, parse_dataframe
from scraper import Scraper

config_path = Path.cwd() / 'config' / 'config.json'
config = config_loader(config_path)

MAIN_URL = config['website']['main_url']
CITY_SEARCH_URL = config['website']['city_search_url_template']
HEADER = config['header']
house_shallow = config['house_attributes_shallow']
house_deep = config["house_attributes_deep"]
search_results_attrs = config['search_results_attributes']


def extract_number_of_rooms(soup):
    result = soup.find('ul', class_="search-result-kenmerken").find_all('li')
    if len(result) > 1:
        return result[1]


house_attrs_sh_func_map = {'Address': lambda soup: soup.find('h2'),
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

for attr in house_attrs_sh_func_map:
    house_shallow.map_func_to_attr(attr, house_attrs_sh_func_map[attr])


def get_attribute_deep(soup, attribute_name):
    dt = soup.find(lambda tag: tag.name == 'dt' and attribute_name.lower() in tag.text.lower())
    if dt:
        return list(dt.find_next("dd").stripped_strings)[0]
    else:
        return None


for attr in house_deep:
    if attr.name not in ['Description', 'Neighbourhood']:
        func = partial(get_attribute_deep, attribute_name=attr.name)
        house_deep.map_func_to_attr(attr.name, func)

get_neighbourhood = lambda soup: soup.find("span", class_="object-header__subtitle")
get_description = lambda soup: soup.find("div", class_="object-description-body")

house_deep.Neighbourhood.retrieve_func = get_neighbourhood
house_deep.Description.retrieve_func = get_description


def get_max_num_pages(soup):
    pp_soup = soup.find("div", class_="pagination-pages")
    num_pages = list(pp_soup
                     | select(lambda x: re.findall('\d+', x.text))
                     | traverse
                     | select(lambda x: int(x))
                     | sort(reverse=True)
                     )[0]
    return num_pages


def get_num_listings(soup):
    listings_results = soup.find_all('script', type="application/ld+json")[2].text
    return json.loads(listings_results)['results_total']


search_results_func_map = {'max_num_pages': get_max_num_pages,
                           'num_listings': get_num_listings,
                           'listings': lambda soup: soup.find_all('div', class_="search-result-content-inner")
                           }

for attr in search_results_func_map:
    search_results_attrs.map_func_to_attr(attr, search_results_func_map[attr])


class FundaScraper(Scraper):
    def __init__(self, **kwargs):
        super().__init__(header=HEADER,
                         main_url=MAIN_URL,
                         city_search_url=CITY_SEARCH_URL,
                         default_city='heel-nederland',
                         house_attributes_shallow=house_shallow,
                         house_attributes_deep=house_deep,
                         search_results_attributes=search_results_attrs,
                         **kwargs)

    def scrape_city(self, city, pages: Union[None, list[int]] = None, method='shallow'):
        method_map = {'shallow': self.scrape_shallow,
                      'deep': self.scrape_deep}
        func = method_map[method]
        return func(city, pages)

    async def get_num_pages_and_listings(self, city=None):
        if city is None:
            city = self.default_city
        soup = await self._get_soup_city(city=city, page=1)
        num_pages = self.search_results_attributes['max_num_pages'].retrieve_func(soup)
        num_listings = self.search_results_attributes['num_listings'].retrieve_func(soup)
        return num_pages, num_listings

    async def get_city_soups(self, city=None, pages: Union[None, list[int]] = None):
        if city is None:
            city = self.default_city

        if pages is None:
            max_pp, _ = await self.get_num_pages_and_listings(city)
            pages = range(1, max_pp + 1)

        soups = await asyncio.gather(*(self._get_soup_city(city, i) for i in pages))
        return soups

    def scrape_shallow(self, city: str, pages: Union[None, list[int]] = None):
        t0 = perf_counter()
        df = asyncio.run(self.scrape_shallow_async(city, pages))
        time_elapsed = round(perf_counter() - t0, 3)
        print(f"{time_elapsed=} s")
        return df

    def scrape_deep(self, city: str, pages: Union[None, list[int]] = None):
        t0 = perf_counter()
        df = asyncio.run(self.scrape_deep_async(city, pages))
        time_elapsed = round(perf_counter() - t0, 3)
        print(f"{time_elapsed=} s")
        return df

    async def scrape_shallow_async(self, city=None, pages: Union[None, list[int]] = None) -> pd.DataFrame:
        soups = await self.get_city_soups(city=city, pages=pages)

        house_data = []
        for soup in soups:
            results = self.search_results_attributes['listings'].retrieve_func(soup)
            houses = [self.get_house_attributes(result, self.house_attributes_shallow) for result in results]
            house_data = house_data + houses

        df = pd.DataFrame(house_data)
        parsed_df = parse_dataframe(self.house_attributes_shallow, df)
        return parsed_df

    async def scrape_deep_async(self, city: str, pages: Union[None, list[int]]):
        df_shallow = await self.scrape_shallow_async(city, pages)
        urls = df_shallow.href.transform(lambda x: self.main_url + x).values
        ids = df_shallow.HouseId.values

        async def get_soup(id, url):
            soup = await self._get_soup(url)
            return id, soup

        soups_ids = await asyncio.gather(*(get_soup(id, url) for id, url in zip(ids, urls)))

        houses = [self.get_house_attributes(soup, self.house_attributes_deep) for _, soup in soups_ids]

        df = pd.DataFrame(houses)
        df['HouseId'] = [id for id, _ in soups_ids]
        parsed_df = parse_dataframe(self.house_attributes_deep, df)
        return df_shallow.merge(parsed_df, on='HouseId')

    def from_href_to_url(self, href: str) -> str:
        return self.main_url + href

    @staticmethod
    def get_house_attributes(soup, attributes_enum: AttributesEnum) -> dict:
        house = {}
        for attribute in attributes_enum:
            retrieved_attribute = attribute.retrieve_func(soup)
            if isinstance(retrieved_attribute, Tag):
                retrieved_attribute = str_from_tag(retrieved_attribute)
            house[attribute.name] = retrieved_attribute
        return house

    # async def get_all_children_urls(self, ):
    #     max_pp = await get_num_pp_from_main_page(url.format(1))
    #     # max_pp = 150
    #     htmls = await asyncio.gather(*(get_html(url.format(i)) for i in range(1, max_pp + 1)))
    #     # urls = await asyncio.gather(*(extract_urls_from_main_page(html) for html in htmls))
    #     return htmls


if __name__ == '__main__':
    scraper = FundaScraper()
    results = scraper.scrape_city(city='Delft', pages=[1, 2], method='shallow')
