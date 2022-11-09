import json
from pathlib import Path

from pipe import traverse, select, sort, where
import asyncio
import re
from city_scraper import Scraper

config_path = Path.cwd() / 'config' / 'config.json'

with open(config_path) as json_file:
    api_config = json.load(json_file)

HEADER = api_config['headers']
URL = "https://www.funda.nl/en/koop/{}/p{}"


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

    async def scrape_shallow(self, city='heel-nederland', max_pp=None) -> list:
        listings = []
        if max_pp is None:
            max_pp, _ = await self._get_number_of_pages_and_listings(city)

        soups = await asyncio.gather(*(self._get_soup(city, i) for i in range(1, max_pp + 1)))

        for soup in soups:
            results = self.get_main_page_results(soup)
            details = [self.get_house_details(result) for result in results]
            listings = listings + details

        return listings

    @staticmethod
    def get_main_page_results(soup):
        return soup.find_all('div', class_="search-result-content-inner")

    @staticmethod
    def get_house_details(result):
        details = {}

        extract_number = lambda x: int(re.search('\d+', x.get_text(strip=True)).group()) if x else None
        extract_text = lambda x: x.get_text(strip=True) if x else None

        details['Address'] = extract_text(result.find('h2'))
        details['Postcode'] = extract_text(result.find('h4'))

        details['PlotSize'] = extract_number(result.find(attrs={'title': 'Plot size'}))
        details['LivingArea'] = extract_number(result.find(attrs={'title': 'Living area'}))
        details['Price'] = ''.join(
            re.findall('\d+', result.find('span', class_='search-result-price').get_text(strip=True)))
        # details['Rooms'] = extract_number(result.find('ul', class_="search-result-kenmerken").find_all('li')[1])

        details['Agency'] = extract_text(result.find('a', class_='search-result-makelaar'))
        if details['Address'] is None:
            print("Address not found")
        return details

    # async def get_all_children_urls(self, ):
    #     max_pp = await get_num_pp_from_main_page(url.format(1))
    #     # max_pp = 150
    #     htmls = await asyncio.gather(*(get_html(url.format(i)) for i in range(1, max_pp + 1)))
    #     # urls = await asyncio.gather(*(extract_urls_from_main_page(html) for html in htmls))
    #     return htmls


if __name__ == '__main__':
    scraper = FundaScraper()
