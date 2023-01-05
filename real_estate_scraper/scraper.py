import asyncio
from asyncio import Semaphore
from itertools import chain
from pathlib import Path
from typing import Union, Optional

import pandas as pd
from aiolimiter import AsyncLimiter
from bs4 import BeautifulSoup
from bs4.element import SoupStrainer, Tag

from real_estate_scraper.html_handling import get_html
from real_estate_scraper.configuration import ScraperConfig, NamedItemsDict
from real_estate_scraper.parsing import str_from_tag
from real_estate_scraper.save import df_to_file_async, write_to_sqlite_async, write_to_sqlite, to_csv
from real_estate_scraper.utils import func_timer, get_timestamp, split_list
from tqdm import tqdm
import logging
from real_estate_scraper.logging_mgt import create_logger

DEBUG = True
DOWNLOAD_FOLDER = Path.cwd().parent / 'downloads'


# logger.setLevel(logging.DEBUG)

class Scraper:
    """A web scraper for scraping real estate listings from a specified website.

    Parameters:
        config (ScraperConfig): An object containing the necessary configurations for scraping the website.
        max_active_requests (int, optional): The maximum number of active requests allowed at any given time.
            Defaults to 5.
        requests_per_sec (int, optional): The maximum number of requests allowed per second. Defaults to 5.

    Attributes:
        config (ScraperConfig): An object containing the necessary configurations for scraping the website.
        max_active_requests (int): The maximum number of active requests allowed at any given time.
        semaphore (Semaphore): A semaphore object used to limit the number of active requests.
        limiter (AsyncLimiter): An async limiter object used to limit the number of requests per second.
        parse_only (SoupStrainer): A SoupStrainer object used to parse only certain parts of the HTML.
    """

    def __init__(self,
                 config: ScraperConfig,
                 max_active_requests: int = 5,
                 requests_per_sec: int = 5,
                 logger: logging.Logger = None):

        self.logger = logger
        self.config = config
        self.max_active_requests = max_active_requests
        self.semaphore = Semaphore(value=max_active_requests)
        self.limiter = AsyncLimiter(1, round(1 / requests_per_sec, 3))
        parse_only = config.website_settings.parse_only
        self.parse_only = SoupStrainer(parse_only) if parse_only else None

        if logger is None:
            logger = create_logger(self.config.website_settings.name)
        self.logger = logger

    def _get_city_url(self, city: Optional[str], page: int) -> str:
        if city is None:
            city = self.config.website_settings.default_city
        return self.config.website_settings.city_search_url_template.format(city=city, page=page)

    async def _get_soup(self, url: str) -> BeautifulSoup:
        async with self.semaphore:
            async with self.limiter:
                html = await get_html(url, header=self.config.website_settings.header)
        return BeautifulSoup(html, 'lxml', parse_only=self.parse_only)

    async def _get_city_soup(self, city: str, page: int) -> tuple[str, BeautifulSoup]:
        url = self._get_city_url(city, page)
        soup = await self._get_soup(url=url)
        return url, soup

    @func_timer(debug=DEBUG)
    def scrape_city(self, city, pages: Union[None, int, list[int]] = None, deep=False):
        self.semaphore = Semaphore(self.max_active_requests)
        return asyncio.run(self._scrape_city_async(city, pages, deep=deep))

    async def get_num_pages_and_listings(self, city=None):
        _, soup = await self._get_city_soup(city=city, page=1)
        num_pages = self.config.search_results_items['number_of_pages'].retrieve(soup)
        num_listings = self.config.search_results_items['number_of_listings'].retrieve(soup)
        return num_pages, num_listings

    async def get_houses_from_page_shallow(self, city=None, page: int = 1) -> list[dict]:
        url, soup = await self._get_city_soup(city=city, page=page)
        listings = self.config.search_results_items['listings'].retrieve(soup)
        houses = [self.get_house_attributes(listing, self.config.house_items_shallow) for listing in listings]
        for house in houses:
            house['url_shallow'] = url
            house['page_shallow'] = page
        return houses

    async def get_house_from_url_deep(self, url) -> dict:
        soup = await self._get_soup(url)
        house = self.get_house_attributes(soup, self.config.house_items_deep)
        house['url_deep'] = url
        return house

    async def _scrape_city_async(self,
                                 city=None,
                                 pages: Union[None, int, list[int]] = None,
                                 deep=False) -> pd.DataFrame:
        if pages is None:
            max_number_of_pages, _ = await self.get_num_pages_and_listings(city)
            pages = range(1, max_number_of_pages + 1)

        if isinstance(pages, int):
            pages = [pages]

        houses = await asyncio.gather(*(self.get_houses_from_page_shallow(city, i) for i in pages))

        house_data = list(chain(*houses))

        df_shallow = pd.DataFrame(house_data)
        df_shallow['url_deep'] = df_shallow.href.transform(lambda x: self.config.website_settings.main_url + x).values
        df_shallow['TimeStampShallow'] = get_timestamp()

        if not deep:
            return df_shallow

        urls = df_shallow.url_deep.values
        houses = await asyncio.gather(*(self.get_house_from_url_deep(url) for url in urls))
        df_deep = pd.DataFrame(houses)
        df_deep['TimeStampDeep'] = get_timestamp()
        return df_shallow.merge(df_deep, on='url_deep')

    @func_timer(debug=DEBUG)
    def download_to_file(self,
                         filepath: Optional[str] = None,
                         city: str = None,
                         pages: Union[None, int, list[int]] = None,
                         deep=False,
                         shallow_pages_per_iteration: int = 5):

        if isinstance(pages, int):
            pages = [pages]

        if filepath is None:
            deep_str = 'deep' if deep else 'shallow'
            pages_str = '_'.join(map(str, pages)) if pages else 'all'
            city_str = city if city else 'all'
            date_str = get_timestamp(date_only=True)
            filepath = DOWNLOAD_FOLDER / f"City_{city_str}_{deep_str}_pages_{pages_str}_{date_str}.csv"

        if pages is None:
            max_number_of_pages, _ = asyncio.run(self.get_num_pages_and_listings(city))
            pages = range(1, max_number_of_pages + 1)

        chunks = split_list(pages, chunksize=shallow_pages_per_iteration)

        for chunk in tqdm(chunks, total=len(chunks)):
            self.semaphore = Semaphore(value=self.max_active_requests)
            df = asyncio.run(self._scrape_city_async(city=city, pages=chunk, deep=deep))
            to_csv(df, filepath=filepath)

    @func_timer(debug=DEBUG)
    def download_to_db(self,
                       db_path: Optional[str] = None,
                       table_name: Optional[str] = None,
                       city: str = None,
                       pages: Union[None, int, list[int]] = None,
                       shallow_pages_per_iteration: int = 5,
                       deep=False):

        if isinstance(pages, int):
            pages = [pages]

        if db_path is None:
            db_path = (DOWNLOAD_FOLDER / f"{self.config.website_settings.name}.db").__str__()

        if table_name is None:
            deep_str = 'deep' if deep else 'shallow'
            city_str = city if city else 'all'
            date_str = get_timestamp(date_only=True).replace('-', '_')
            table_name = f"raw.{city_str}_{deep_str}_{date_str}"

        if pages is None:
            max_number_of_pages, _ = asyncio.run(self.get_num_pages_and_listings(city))
            pages = range(1, max_number_of_pages + 1)

        chunks = split_list(pages, chunksize=shallow_pages_per_iteration)

        for chunk in tqdm(chunks, total=len(chunks)):
            self.semaphore = Semaphore(value=self.max_active_requests)
            df = asyncio.run(self._scrape_city_async(city=city, pages=chunk, deep=deep))
            write_to_sqlite(df, database_name=db_path, table_name=table_name)

    def from_href_to_url(self, href: str) -> str:
        return self.config.website_settings.main_url + href

    def get_house_attributes(self, soup, attributes_enum: NamedItemsDict) -> dict:
        house = {}

        for attribute in attributes_enum:
            try:
                retrieved_attribute = attribute.retrieve(soup)
            except Exception as e:
                msg = f"{attribute.name} was not retrieve because {e}"
                self.logger.info(msg)
                retrieved_attribute = None
            if isinstance(retrieved_attribute, Tag):
                retrieved_attribute = str_from_tag(retrieved_attribute)
            house[attribute.name] = retrieved_attribute

        total_items = len(attributes_enum)
        not_none_items = total_items - list(house.values()).count(None)

        if not_none_items != total_items:
            msg = f"Retrieved {not_none_items}/{total_items} items"
            self.logger.info(msg)

        if all([value is None for value in house.values()]):
            self.logger.warning("No items retrieved")
        return house
