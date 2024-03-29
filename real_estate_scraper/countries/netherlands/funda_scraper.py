import json
import re
from functools import partial
from pathlib import Path

from bs4 import BeautifulSoup
from pipe import traverse, select, sort

from real_estate_scraper.parsing import str_from_tag
from real_estate_scraper.utils import compose_functions

from real_estate_scraper.configuration import ScraperConfig
from real_estate_scraper.scraper import Scraper

config_path = Path(__file__).parent / "funda_config.json"
funda_config = ScraperConfig.from_json(config_path)


def extract_number_of_rooms(soup: BeautifulSoup) -> BeautifulSoup:
    result = soup.find("ul", class_="search-result-kenmerken").find_all("li")
    if len(result) > 1:
        return result[1]


def get_href(soup: BeautifulSoup) -> str:
    href = soup.find("a", attrs={"data-object-url-tracking": "resultlist"}).get("href")
    return funda_config.website_settings.main_url + str_from_tag(href)


house_attrs_sh_func_map = {
    "Address": lambda soup: soup.find("h2"),
    "PostCode": lambda soup: soup.find("h4"),
    "LivingArea": lambda soup: soup.find(attrs={"title": "Living area"}),
    "PlotSize": lambda soup: soup.find(attrs={"title": "Plot size"}),
    "Price": lambda soup: soup.find("span", class_="search-result-price"),
    "Rooms": extract_number_of_rooms,
    "href": get_href,
    "HouseId": lambda soup: soup.find(
        "a", attrs={"data-object-url-tracking": "resultlist"}
    ).get("data-search-result-item-anchor"),
}

for item in funda_config.house_items_shallow:
    item.retrieve = compose_functions(str_from_tag, house_attrs_sh_func_map[item.name])


def get_attribute_deep(soup, text_in_website):
    dt = soup.find(
        lambda tag: tag.name == "dt" and text_in_website.lower() in tag.text.lower()
    )
    if dt:
        return list(dt.find_next("dd").stripped_strings)[0]
    else:
        return None


for item in funda_config.house_items_deep:
    if item.name not in ["Description", "Neighbourhood"]:
        func = partial(get_attribute_deep, text_in_website=item.text_in_website)
        item.retrieve = compose_functions(str_from_tag, func)

get_neighbourhood = lambda soup: soup.find("span", class_="object-header__subtitle")
get_description = lambda soup: soup.find("div", class_="object-description-body")

funda_config.house_items_deep.Neighbourhood.retrieve = compose_functions(str_from_tag,
                                                                         get_neighbourhood)
funda_config.house_items_deep.Description.retrieve = compose_functions(str_from_tag,
                                                                       get_description)


def fetch_location(soup: BeautifulSoup) -> dict:
    text = str_from_tag(soup.find_all("script", attrs={"type": "application/json"})[-1])
    latitude, longitude = None, None
    if text:
        loc_dict = json.loads(text)
        latitude, longitude = loc_dict['lat'], loc_dict['lng']
    return latitude, longitude


funda_config.house_items_deep.Latitude.retrieve = lambda soup: fetch_location(soup)[0]
funda_config.house_items_deep.Longitude.retrieve = lambda soup: fetch_location(soup)[1]


def get_max_num_pages(soup: BeautifulSoup) -> int:
    pp_soup = soup.find("div", class_="pagination-pages")
    num_pages = list(
        pp_soup
        | select(lambda x: re.findall("\d+", x.text.replace(",", "")))
        | traverse
        | select(lambda x: int(x))
        | sort(reverse=True)
    )[0]
    return num_pages


def get_num_listings(soup: BeautifulSoup) -> int:
    listings_results = soup.find_all("script", type="application/ld+json")[2].text
    return json.loads(listings_results)["results_total"]


search_results_func_map = {
    "number_of_pages": get_max_num_pages,
    "number_of_listings": get_num_listings,
    "listings": lambda soup: soup.find_all("div", class_="search-result-content-inner"),
}

for item in funda_config.search_results_items:
    item.retrieve = compose_functions(str_from_tag, search_results_func_map[item.name])


def get_funda_scraper(logger, **kwargs):
    return Scraper(config=funda_config, logger=logger, **kwargs)


if __name__ == "__main__":
    scraper = Scraper(config=funda_config)
