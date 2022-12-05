import asyncio
import json
import re
from asyncio import Semaphore
from functools import partial
from itertools import chain
from pathlib import Path
from typing import Union
from datetime import datetime
import pandas as pd
from bs4.element import Tag
from pipe import traverse, select, sort

from configuration import ScraperConfig, NamedItemsDict
from parsing import str_from_tag, parse_dataframe
from scraper import Scraper
from utils import func_timer, df_to_json_async, get_timestamp

now = datetime.now
config_path = Path.cwd() / 'config' / 'config_refactored.json'
config = ScraperConfig.from_json(config_path)

DEBUG = True

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

for item in config.house_items_shallow:
    item.retrieve = house_attrs_sh_func_map[item.name]


def get_attribute_deep(soup, text_in_website):
    dt = soup.find(lambda tag: tag.name == 'dt' and text_in_website.lower() in tag.text.lower())
    if dt:
        return list(dt.find_next("dd").stripped_strings)[0]
    else:
        return None


for item in config.house_items_deep:
    if item.name not in ['Description', 'Neighbourhood']:
        func = partial(get_attribute_deep, text_in_website=item.text_in_website)
        item.retrieve = func

get_neighbourhood = lambda soup: soup.find("span", class_="object-header__subtitle")
get_description = lambda soup: soup.find("div", class_="object-description-body")

config.house_items_deep.Neighbourhood.retrieve = get_neighbourhood
config.house_items_deep.Description.retrieve = get_description


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


search_results_func_map = {'number_of_pages': get_max_num_pages,
                           'number_of_listings': get_num_listings,
                           'listings': lambda soup: soup.find_all('div', class_="search-result-content-inner")
                           }


for item in config.search_results_items:
    item.retrieve = search_results_func_map[item.name]


class FundaScraper(Scraper):
    def __init__(self):
        super().__init__(config=config)

    async def get_num_pages_and_listings(self, city=None):
        if city is None:
            city = self.config.website_settings.default_city
        soup = await self._get_soup_city(city=city, page=1)
        num_pages = self.config.search_results_items['max_num_pages'].retrieve(soup)
        num_listings = self.config.search_results_items['num_listings'].retrieve(soup)
        return num_pages, num_listings

    async def get_houses_from_page_shallow(self, city=None, page: int = 1) -> list[dict]:
        soup = await self._get_soup_city(city=city, page=page)
        listings = self.config.search_results_items['listings'].retrieve(soup)
        houses = [self.get_house_attributes(listing, self.config.house_items_shallow) for listing in listings]
        return houses

    async def get_house_from_url_deep(self, url, id) -> dict:
        soup = await self._get_soup(url)
        house = self.get_house_attributes(soup, self.config.house_items_deep)
        house['Id'] = id
        return house

    async def _scrape_shallow_async(self, city=None, pages: Union[None, list[int]] = None) -> pd.DataFrame:
        if city is None:
            city = self.config.website_settings.default_city

        if pages is None:
            max_pp, _ = await self.get_num_pages_and_listings(city)
            pages = range(1, max_pp + 1)

        houses = await asyncio.gather(*(self.get_houses_from_page_shallow(city, i) for i in pages))
        house_data = list(chain(*houses))

        df_shallow = pd.DataFrame(house_data)
        parsed_df = parse_dataframe(self.config.house_items_shallow, df_shallow)
        parsed_df['Id'] = self.id_from_df(parsed_df)
        parsed_df['url'] = parsed_df.href.transform(lambda x: self.config.website_settings.main_url + x).values
        parsed_df['TimeStampShallow'] = get_timestamp()
        return parsed_df

    async def _scrape_deep_async(self, city=None, pages: Union[None, list[int]] = None):
        df_shallow = await self._scrape_shallow_async(city, pages)
        urls = df_shallow.url.values
        ids = df_shallow.Id.values

        houses = await asyncio.gather(*(self.get_house_from_url_deep(url, id) for url, id in zip(urls, ids)))

        df_deep = pd.DataFrame(houses)

        parsed_df = parse_dataframe(self.config.house_items_deep, df_deep)
        parsed_df['TimeStampDeep'] = get_timestamp()
        return df_shallow.merge(parsed_df, on='Id')

    async def _download_page(self, city: str, page: int = 1, method='shallow'):
        methods = {'shallow': self._scrape_shallow_async,
                   'deep': self._scrape_deep_async}

        df = await methods[method](city=city, pages=[page])
        await df_to_json_async(df, 'test.csv')

    async def _download_async(self, city: str, pages: Union[None, list[int]], method='shallow'):
        if city is None:
            city = self.config.website_settings.default_city

        if pages is None:
            max_pp, _ = await self.get_num_pages_and_listings(city)
            pages = range(1, max_pp + 1)

        await asyncio.gather(*(self._download_page(city=city, page=page, method=method) for page in pages))

    @func_timer(debug=DEBUG)
    def download(self, city: str, pages: Union[None, list[int]], method='shallow'):
        self.semaphore = Semaphore(value=self.max_active_requests)
        asyncio.run(self._download_async(city=city, pages=pages, method=method))

    def from_href_to_url(self, href: str) -> str:
        return self.config.website_settings.main_url + href

    @staticmethod
    def id_from_df(df):
        return df.href.transform(lambda x: x.split('/')[4])

    @staticmethod
    def get_house_attributes(soup, attributes_enum: NamedItemsDict) -> dict:
        house = {}

        for attribute in attributes_enum:
            retrieved_attribute = attribute.retrieve(soup)
            if isinstance(retrieved_attribute, Tag):
                retrieved_attribute = str_from_tag(retrieved_attribute)
            house[attribute.name] = retrieved_attribute
        if all([value is None for value in house.values()]):
            print("No details found")
        return house


if __name__ == '__main__':
    scraper = FundaScraper()
    # results = scraper.scrape_city(city='Delft', pages=[1], method='shallow')
