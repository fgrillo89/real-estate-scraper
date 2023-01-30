import json
import re
from functools import partial
from pathlib import Path
from typing import Callable, Optional

from bs4 import BeautifulSoup
from pipe import traverse, select, sort

from real_estate_scraper.configuration import ScraperConfig
from real_estate_scraper.html_inspection import get_dd_text_from_dt_name
from real_estate_scraper.parsing import str_from_tag, extract_numeric_value
from real_estate_scraper.scraper import Scraper
from real_estate_scraper.utils import compose_functions

config_path = Path(__file__).parent / "immobiliare_config.json"
immobiliare_config = ScraperConfig.from_json(config_path)


def get_max_num_pages(soup: BeautifulSoup) -> int:
    pagination_list = soup.find("div", attrs={"data-cy": "pagination-list"})
    if pagination_list:
        num_pages = list(
            pagination_list.contents
            | select(lambda x: re.findall("\d+", x.text.replace(",", "")))
            | traverse
            | select(lambda x: int(x))
            | sort(reverse=True)
        )[0]
        return num_pages
    return 1


def get_num_listings(soup: BeautifulSoup) -> int:
    listings_results = str_from_tag(soup.find("div", class_="in-searchList__title"))
    number = extract_numeric_value(listings_results,
                                   thousands_delimiter=".",
                                   decimal_delimiter=",")
    if number:
        return int(number)
    return None


search_results_func_map = {
    "number_of_pages": get_max_num_pages,
    "number_of_listings": get_num_listings,
    "listings": lambda soup: soup.find_all("div",
                                           attrs={"class":
                                                      "nd-mediaObject__content "
                                                      "in-card__content "
                                                      "in-realEstateListCard__content"}),
}

for item in immobiliare_config.search_results_items:
    item.retrieve = compose_functions(str_from_tag, search_results_func_map[item.name])

house_attrs_sh_func_map = {
    "Address": lambda soup: soup.find("a", attrs={"class": "in-card__title"}),
    "LivingArea": lambda soup: soup.find("li", attrs={"aria-label": "superficie"}),
    "Price": lambda soup: soup.find("li",
                                    class_="nd-list__item in-feat__item "
                                           "in-feat__item--main "
                                           "in-realEstateListCard__features--main"),
    "Rooms": lambda soup: soup.find("li", attrs={"aria-label": "locali"}),
    "Floor": lambda soup: soup.find("li", attrs={"aria-label": "piano"}),
    "Bathrooms": lambda soup: soup.find("li", attrs={"aria-label": "bagno"}),
    "NumberOfApartments": lambda soup: soup.find("li", attrs={"aria-label": "tipologie"}),
    "href": lambda soup: soup.find("a", attrs={"class": "in-card__title"}).get("href")
}

for item in immobiliare_config.house_items_shallow:
    item.retrieve = compose_functions(str_from_tag, house_attrs_sh_func_map[item.name])

special_items = ["Latitude",
                 "Longitude",
                 "City",
                 "Province",
                 "Region",
                 "AddressDeep",
                 "Microzone",
                 "Macrozone",
                 "StreetNumber"]

for item in immobiliare_config.house_items_deep:
    if item.name not in special_items:
        func = partial(get_dd_text_from_dt_name, text_in_website=item.text_in_website)
        item.retrieve = compose_functions(str_from_tag, func)


def fetch_location(soup: BeautifulSoup) -> dict:
    text = soup.find("script", attrs={"type": "application/json",
                                      "id": "js-hydration"}).text
    return json.loads(text)['listing']['properties'][0]['location']


def fetch_special_item(soup: BeautifulSoup, key1: str, key2: Optional[str] = None):
    location_dict = fetch_location(soup)
    if location_dict:
        first = location_dict.get(key1)
        if key2:
            if first:
                return first.get(key2)
            else:
                return None
        return first
    return None


def fetch_item_factory(key1: str, key2: Optional[str] = None) -> Callable:
    fun = partial(fetch_special_item, key1=key1, key2=key2)
    return fun


location_items_map = {"Latitude": fetch_item_factory('latitude'),
                      "Longitude": fetch_item_factory('longitude'),
                      "City": fetch_item_factory('city', 'name'),
                      "Province": fetch_item_factory('province', 'name'),
                      "Region": fetch_item_factory('region', 'name'),
                      "Microzone": fetch_item_factory('microzone', 'name'),
                      "Macrozone": fetch_item_factory('macrozone', 'name'),
                      "StreetNumber": fetch_item_factory('streetNumber'),
                      "AddressDeep": fetch_item_factory('address')}

for item in special_items:
    immobiliare_config.house_items_deep[item].retrieve = location_items_map[item]


def get_immobiliare_scraper(logger, **kwargs):
    return Scraper(config=immobiliare_config, logger=logger, **kwargs)


if __name__ == "__main__":
    scraper = get_immobiliare_scraper(logger=None)
