import json

from abc import ABC

from asyncio import Semaphore
from pathlib import Path

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

    async def _get_soup(self, city: str, page: int) -> BeautifulSoup:
        url = self.get_url(city, page)
        await self.semaphore.acquire()

        async with self.limiter:
            html = await get_html(url, self.header)

        self.semaphore.release()
        return BeautifulSoup(html, 'lxml')

    def scrape_city(self, city: str, max_pp=None, method='shallow'):
        pass
