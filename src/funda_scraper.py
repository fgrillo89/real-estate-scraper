import json
import re
from datetime import datetime
from functools import partial
from pathlib import Path

from pipe import traverse, select, sort

from configuration import ScraperConfig
from scraper import Scraper

now = datetime.now
config_path = Path.cwd().parent / 'config' / 'config_refactored.json'
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
                     | select(lambda x: re.findall('\d+', x.text.replace(',','')))
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

FundaScraper = Scraper(config=config)

if __name__ == '__main__':
    scraper = FundaScraper
    # results = scraper.scrape_city(city='Delft', pages=[1], method='shallow')
