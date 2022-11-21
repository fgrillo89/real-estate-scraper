import asyncio
from abc import ABC, abstractmethod
from asyncio import Semaphore
import lxml
import cchardet
from typing import Union

from aiolimiter import AsyncLimiter
from bs4 import BeautifulSoup
from bs4.element import SoupStrainer

from html_handling import get_html
from src.config_loader import AttributesEnum
from src.utils import func_timer

DEBUG=True

class Scraper(ABC):
    def __init__(self,
                 header: dict,
                 main_url: str,
                 city_search_url: str,
                 default_city: str,
                 house_attributes_shallow: AttributesEnum,
                 house_attributes_deep: AttributesEnum,
                 search_results_attributes: AttributesEnum,
                 max_active_requests=10,
                 requests_per_sec=10,
                 parse_only: Union[list[str], None] = None):
        self.header = header
        self.main_url = main_url
        self.city_search_url = city_search_url
        self.default_city = default_city
        self.house_attributes_shallow = house_attributes_shallow
        self.house_attributes_deep = house_attributes_deep
        self.search_results_attributes = search_results_attributes
        self.max_active_requests = 10
        self.semaphore = Semaphore(value=max_active_requests)
        self.limiter = AsyncLimiter(1, round(1 / requests_per_sec, 3))
        self.parse_only = SoupStrainer(parse_only) if parse_only else None

    def get_city_url(self, city: str, page: int) -> str:
        return self.city_search_url.format(city=city, page=page)

    async def _get_soup(self, url: str) -> BeautifulSoup:
        async with self.semaphore:
            async with self.limiter:
                html = await get_html(url, self.header)
        return BeautifulSoup(html, 'lxml', parse_only=self.parse_only)

    async def _get_soup_city(self, city: str, page: int) -> BeautifulSoup:
        url = self.get_city_url(city, page)
        return await self._get_soup(url=url)

    @func_timer(debug=DEBUG)
    def scrape_city(self, city, pages: Union[None, list[int]] = None, method='shallow'):
        method_map = {'shallow': self._scrape_shallow_async,
                      'deep': self._scrape_deep_async}

        if method not in method_map:
            raise KeyError(f"'{method}' is not a valid method. Available methods: {list(method_map)}")

        self.semaphore = Semaphore(self.max_active_requests)
        return asyncio.run(method_map[method](city, pages))

    @abstractmethod
    def _scrape_shallow_async(self, city=None, pages: Union[None, list[int]] = None):
        pass

    @abstractmethod
    def _scrape_deep_async(self, city=None, pages: Union[None, list[int]] = None):
        pass
