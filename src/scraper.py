import json

from abc import ABC, abstractmethod

from asyncio import Semaphore
from enum import Enum
from pathlib import Path
from typing import Union

from bs4 import BeautifulSoup
from html_handling import get_html
from aiolimiter import AsyncLimiter




class Scraper(ABC):
    def __init__(self, header: dict, main_url: str, max_active_requests=10):
        self.header = header
        self.main_url = main_url
        self.limiter = AsyncLimiter(max_active_requests, 3)
        self.semaphore = Semaphore(value=max_active_requests)

    def get_url(self, city: str, page: int) -> str:
        return self.main_url.format(city, page)

    async def _get_soup(self, url: str) -> BeautifulSoup:
        await self.semaphore.acquire()

        async with self.limiter:
            html = await get_html(url, self.header)

        self.semaphore.release()
        return BeautifulSoup(html, 'lxml')

    async def _get_soup_main_url(self, city: str, page: int) -> BeautifulSoup:
        url = self.get_url(city, page)
        return await self._get_soup(url=url)

    @abstractmethod
    def scrape_shallow(self, city: str, pages: Union[None, list[int]]):
        pass

    @abstractmethod
    def scrape_deep(self, city: str, pages: Union[None, list[int]]):
        pass

    def scrape_city(self, city: str, max_pp=None, method='shallow'):
        pass
