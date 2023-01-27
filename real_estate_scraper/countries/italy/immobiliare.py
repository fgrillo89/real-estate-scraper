import json
import re
from functools import partial
from pathlib import Path

from bs4 import BeautifulSoup
from pipe import traverse, select, sort

from real_estate_scraper.html_inspection import extract_all_dd_text, \
    get_dd_text_from_dt_name
from real_estate_scraper.parsing import str_from_tag, extract_numeric_value
from real_estate_scraper.utils import compose_functions

from real_estate_scraper.configuration import ScraperConfig
from real_estate_scraper.scraper import Scraper

config_path = Path(__file__).parent / "immobiliare_config.json"
immobiliare_config = ScraperConfig.from_json(config_path)


def get_max_num_pages(soup: BeautifulSoup) -> int:
    pp_soup = soup.find("div", attrs={"data-cy": "pagination-list"}).contents
    num_pages = list(
        pp_soup
        | select(lambda x: re.findall("\d+", x.text.replace(",", "")))
        | traverse
        | select(lambda x: int(x))
        | sort(reverse=True)
    )[0]
    return num_pages


def get_num_listings(soup: BeautifulSoup) -> int:
    listings_results = str_from_tag(soup.find("div", class_="in-searchList__title"))
    return int(extract_numeric_value(listings_results,
                                     thousands_delimiter=".",
                                     decimal_delimiter=","))


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

for item in immobiliare_config.house_items_deep:
    if item.name != "Coordinates":
        func = partial(get_dd_text_from_dt_name, text_in_website=item.text_in_website)
        item.retrieve = compose_functions(str_from_tag, func)


def extract_coordinates(soup: BeautifulSoup) -> str:
    map_string = soup.find("nd-map").contents[1].replace("mapConfig", "")
    return str(json.loads(map_string)['center'])


immobiliare_config.house_items_deep.Coordinates.retrieve = extract_coordinates


def get_immobiliare_scraper(logger, **kwargs):
    return Scraper(config=immobiliare_config, logger=logger, **kwargs)


if __name__ == "__main__":
    scraper = get_immobiliare_scraper(logger=None)
