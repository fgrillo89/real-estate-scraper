from abc import ABC, abstractmethod
from asyncio import Semaphore
from collections import namedtuple
from enum import Enum
from typing import Union

from aiolimiter import AsyncLimiter
from bs4 import BeautifulSoup

from html_handling import get_html
from src.config_loader import AttributesEnum


class Scraper(ABC):
    def __init__(self,
                 header: dict,
                 main_url: str,
                 city_search_url: str,
                 default_city: str,
                 house_attributes_shallow: AttributesEnum,
                 search_results_attributes: AttributesEnum,
                 max_active_requests=10,
                 requests_per_sec=6):
        self.header = header
        self.main_url = main_url
        self.city_search_url = city_search_url
        self.default_city = default_city
        self.house_attributes_shallow = house_attributes_shallow
        self.search_results_attributes = search_results_attributes
        self.semaphore = Semaphore(value=max_active_requests)
        self.limiter = AsyncLimiter(1, round(1 / requests_per_sec, 3))

    def get_city_url(self, city: str, page: int) -> str:
        return self.city_search_url.format(city=city, page=page)

    async def _get_soup(self, url: str) -> BeautifulSoup:
        await self.semaphore.acquire()

        async with self.limiter:
            html = await get_html(url, self.header)

        self.semaphore.release()
        return BeautifulSoup(html, 'lxml')

    async def _get_soup_city(self, city: str, page: int) -> BeautifulSoup:
        url = self.get_city_url(city, page)
        return await self._get_soup(url=url)

    @abstractmethod
    def scrape_shallow(self, city: str, pages: Union[None, list[int]]):
        pass

    @abstractmethod
    def scrape_deep(self, city: str, pages: Union[None, list[int]]):
        pass

    def scrape_city(self, city: str, max_pp=None, method='shallow'):
        pass
