import asyncio
import logging
from asyncio import Semaphore
from itertools import chain
from pathlib import Path
from typing import Union, Optional, Tuple, Dict

import pandas as pd
from aiolimiter import AsyncLimiter
from bs4 import BeautifulSoup
from bs4.element import SoupStrainer, Tag
from tqdm import tqdm

from real_estate_scraper.configuration import ScraperConfig, NamedItemsDict
from real_estate_scraper.html_handling import get_html
from real_estate_scraper.logging_mgmt import create_logger
from real_estate_scraper.parsing import str_from_tag, get_extraction_statistics
from real_estate_scraper.save import write_to_sqlite, to_csv, create_folder
from real_estate_scraper.utils import func_timer, get_timestamp, split_list

TIMER_ACTIVE = True
DOWNLOAD_FOLDER = Path.cwd() / "downloads"

House = Dict[str, str]


def get_house_from_soup(soup: BeautifulSoup,
                        named_items_dict: NamedItemsDict,
                        logger: Optional[logging.Logger] = None) -> House:
    """
    Returns a House object given a BeautifulSoup object and a NamedItemsDict.
    Args:
        soup (BeautifulSoup): A BeautifulSoup object.
        named_items_dict (NamedItemsDict): A NamedItemsDict with the items to retrieve
        logger (Optional[logging.Logger]): A logger. Defaults to None.
    Returns:
        House: A dictionary of house items.
    """
    house = {}

    for item in named_items_dict:
        try:
            retrieved_item = item.retrieve(soup)
        except Exception as e:
            msg = f"{item.name} was not retrieved because {e}"
            logger.info(msg)
            retrieved_item = None
        if isinstance(retrieved_item, Tag):
            retrieved_item = str_from_tag(retrieved_item)
        house[item.name] = retrieved_item

    if all([value is None for value in house.values()]):
        logger.warning("No items retrieved")
    return house


class Scraper:
    """A web scraper for scraping real estate listings.

    Args:
        config (ScraperConfig): An object containing the configurations
        for scraping the website.
        max_active_requests (int, optional): The maximum number of
        active requests allowed. Defaults to 5.
        requests_per_sec (int, optional): The maximum number of requests
        allowed per second. Defaults to 5.
        logger (logging.Logger, optional): A logger object. If not provided,
        a default logger will be created.

    Attributes:
        config (ScraperConfig): Object containing the necessary configurations for
        scraping the website.
        max_active_requests (int): Maximum number of active requests allowed.
        semaphore (Semaphore): Used to limit the number of active requests.
        limiter (AsyncLimiter): Used to limit the number of requests per second.
        parse_only (SoupStrainer): Used to parse only certain parts of the HTML.
        logger (logging.Logger): A logger object for logging messages.
    """

    def __init__(
            self,
            config: ScraperConfig,
            max_active_requests: int = 5,
            requests_per_sec: int = 5,
            logger: Optional[logging.Logger] = None,
    ):

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

    @func_timer(active=TIMER_ACTIVE)
    def download_to_dataframe(self,
                              city: Optional[str] = None,
                              pages: Union[None, int, list[int]] = None,
                              deep=False,
                              shallow_batch_size: int = 5) -> pd.DataFrame:
        """Scrapes the website for the given city.

        Args:
            city (Optional[str]): The name of the city to scrape.
            pages (Optional[int, list[int]): The number of the pages to scrape.
                    If None, scrape all pages.
                    If an integer, scrape that page. For example, page 2.
                    If a list of integers, scrape all the pages with those numbers.
                    Defaults to None.
            deep (bool, optional): If True, scrape each listing's webpage.
                    Defaults to False.
            shallow_batch_size(int, optional): Number of shallow pages to scrape
            in a batch.

        Returns:
            pd.DataFrame: A dataframe containing the scraped data.

        Example:
            >>> scraper = Scraper(config)
            >>> df = scraper.download_to_dataframe("Delft")
            >>> df.Price[0:5]
            0    € 375, 000 k.k.
            1    € 775, 000 k.k.
            2    € 465, 000 k.k.
            3    € 359, 000 k.k.
            4    € 400, 000 k.k.
            Name: Price, dtype: object

        """

        dataframes = []
        for df in self._dataframe_generator(city, pages, deep, shallow_batch_size):
            dataframes.append(df)
        return pd.concat(dataframes)

    @func_timer(active=TIMER_ACTIVE)
    def download_to_file(
            self,
            city: Optional[str] = None,
            pages: Union[None, int, list[int]] = None,
            deep=False,
            shallow_batch_size: int = 5,
            filepath: Optional[str] = None
    ):
        """
        Downloads listings a file.
        Args:
            city (str, optional): City to download houses from. Defaults to None.
            pages (int, optional): Number of the shallow pages to scrape.
                Defaults to None.
            deep (bool, optional): Whether to scrape deep. Defaults to False.
            shallow_batch_size (int, optional): Number of shallow pages to scrape in a
            batch. The listings will be downloaded in batches of shallow_batch_size.
            filepath (str, optional): Path to the file to write. Defaults to None.
        """

        if filepath is None:
            msg = create_folder(DOWNLOAD_FOLDER)
            if msg:
                self.logger.info(msg)

            deep_str = "deep" if deep else "shallow"
            pages_str = "_".join(map(str, pages)) if pages else "all"
            city_str = city if city else "all"
            date_str = get_timestamp(date_only=True)

            filepath = (
                    DOWNLOAD_FOLDER
                    / f"City_{city_str}_{deep_str}_pages_{pages_str}_{date_str}.csv"
            )

        for df in self._dataframe_generator(city, pages, deep, shallow_batch_size):
            to_csv(df, filepath=filepath)

    @func_timer(active=TIMER_ACTIVE)
    def download_to_db(
            self,
            city: Optional[str] = None,
            pages: Union[None, int, list[int]] = None,
            deep=False,
            shallow_batch_size: int = 5,
            db_path: Optional[str] = None,
            table_name: Optional[str] = None,
    ):
        """
        Downloads listings to a SQLite database.
        Args:
            city (str, optional): City to download houses from. Defaults to None.
            pages (int, optional): Number of shallow pages to scrape. Defaults to None.
            deep (bool, optional): Whether to scrape deep. Defaults to False.
            shallow_batch_size (int, optional): Number of shallow pages to scrape in a
            batch. The listings will be downloaded in batches of shallow_batch_size.
            db_path (str, optional): Path to the database to write. Defaults to None.
            table_name (str, optional): Name of the table to write. Defaults to None.
        """

        if db_path is None:
            msg = create_folder(DOWNLOAD_FOLDER)
            if msg:
                self.logger.info(msg)
            db_path = (DOWNLOAD_FOLDER /
                       f"{self.config.website_settings.name}.db").as_posix()

        if table_name is None:
            deep_str = "deep" if deep else "shallow"
            city_str = city if city else "all"
            date_str = get_timestamp(date_only=True).replace("-", "_")
            table_name = f"raw.{city_str}_{deep_str}_{date_str}"

        for df in self._dataframe_generator(city, pages, deep, shallow_batch_size):
            write_to_sqlite(df, database_name=db_path, table_name=table_name)

    def _dataframe_generator(self,
                             city: Optional[str] = None,
                             pages: Optional[int] = None,
                             deep=False,
                             shallow_batch_size: int = 5) -> pd.DataFrame:

        chunks = asyncio.run(self._get_pages_batches(city, pages, shallow_batch_size))

        item_list = self.house_items_shallow_names
        if deep:
            item_list += self.house_items_deep_names

        for chunk in tqdm(chunks, total=len(chunks)):
            self.semaphore = Semaphore(value=self.max_active_requests)
            df = asyncio.run(self._scrape_city_async(city=city, pages=chunk, deep=deep))

            success_rate, max_items, min_items = get_extraction_statistics(df,
                                                                           item_list)

            self.logger.info(f"Batch mean items-retrieval success rate:"
                             f" {success_rate}%\n"
                             f"Max items retrieved: {max_items}/{len(item_list)}\n"
                             f"Min items retrieved: {min_items}/{len(item_list)}")
            yield df

    async def _get_pages_batches(self,
                                 city: Optional[str] = None,
                                 pages: Union[None, int, list[int]] = None,
                                 batch_size: Optional[int] = None) -> list[int]:
        if isinstance(pages, int):
            pages = [pages]

        if pages is None:
            max_number_of_pages, _ = await self._get_num_pages_and_listings(city)
            pages = range(1, max_number_of_pages + 1)

        if batch_size:
            return split_list(pages, chunksize=batch_size)

        return pages

    async def _scrape_city_async(
            self,
            city: Optional[str] = None,
            pages: Union[None, int, list[int]] = None,
            deep=False
    ) -> pd.DataFrame:

        pages = await self._get_pages_batches(city, pages)

        houses = await asyncio.gather(
            *(self._get_houses_from_page_shallow(city, i) for i in pages)
        )

        house_data = list(chain(*houses))

        df_shallow = pd.DataFrame(house_data)
        df_shallow["url_deep"] = df_shallow.href.transform(
            lambda x: self.config.website_settings.main_url + x
        ).values
        df_shallow["TimeStampShallow"] = get_timestamp()

        if not deep:
            return df_shallow

        urls = df_shallow.url_deep.values
        houses = await asyncio.gather(
            *(self._get_house_from_url_deep(url) for url in urls)
        )
        df_deep = pd.DataFrame(houses)
        df_deep["TimeStampDeep"] = get_timestamp()
        return df_shallow.merge(df_deep, on="url_deep")

    async def _get_city_soup(self, city: str, page: int) -> tuple[str, BeautifulSoup]:
        url = self._get_city_url(city, page)
        soup = await self._get_soup(url=url)
        return url, soup

    async def _get_soup(self, url: str) -> BeautifulSoup:
        async with self.semaphore:
            async with self.limiter:
                html = await get_html(url, header=self.config.website_settings.header)
        self.logger.info(f"Done requesting {url}")
        return BeautifulSoup(html, "lxml", parse_only=self.parse_only)

    def _get_city_url(self, city: Optional[str] = None, page: int = 1) -> str:
        if city is None:
            city = self.config.website_settings.default_city
        return self.config.website_settings.city_search_url_template.format(
            city=city, page=page
        )

    async def _get_num_pages_and_listings(self, city: Optional[str] = None) \
            -> Tuple[int, int]:
        _, soup = await self._get_city_soup(city=city, page=1)
        num_pages = self.config.search_results_items["number_of_pages"].retrieve(soup)
        num_listings = self.config.search_results_items["number_of_listings"].retrieve(
            soup
        )
        return num_pages, num_listings

    async def _get_houses_from_page_shallow(
            self, city: str = None, page: int = 1
    ) -> list[dict[str, str]]:
        url, soup = await self._get_city_soup(city=city, page=page)
        listings = self.config.search_results_items["listings"].retrieve(soup)
        houses = [
            get_house_from_soup(listing,
                                self.config.house_items_shallow,
                                self.logger)
            for listing in listings
        ]
        for house in houses:
            house["url_shallow"] = url
            house["page_shallow"] = page
        return houses

    async def _get_house_from_url_deep(self, url: str) -> dict[str, str]:
        soup = await self._get_soup(url)
        house = get_house_from_soup(soup,
                                    self.config.house_items_deep,
                                    self.logger)
        house["url_deep"] = url
        return house

    def _from_href_to_url(self, href: str) -> str:
        return self.config.website_settings.main_url + href

    @property
    def num_house_items_shallow(self) -> int:
        return len(self.config.house_items_shallow)

    @property
    def num_house_items_deep(self) -> int:
        if self.config.house_items_deep:
            return len(self.config.house_items_deep)
        return 0

    @property
    def house_items_shallow_names(self) -> int:
        return self.config.house_items_shallow.names

    @property
    def house_items_deep_names(self) -> int:
        if self.config.house_items_deep:
            return self.config.house_items_deep.names
        return 0
