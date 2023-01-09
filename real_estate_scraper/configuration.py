import json
from pathlib import Path
from typing import Union, Optional, TypedDict, Protocol, runtime_checkable
from dataclasses import dataclass, field
from collections.abc import Callable

from bs4 import BeautifulSoup


class ItemContent(TypedDict):
    type: str
    text_in_website: Optional[str]


@runtime_checkable
class RetrieveItemFn(Protocol):
    def __call__(self, soup: BeautifulSoup) -> Union[str, int]:
        pass


@dataclass
class ConfigObject:
    """A base class for objects representing configuration data."""

    def __post_init__(self):
        self.validate_fields()

    def validate_fields(self):
        for attr_name in self.__dataclass_fields__:
            attr = self.__getattribute__(attr_name)
            expected_type = self.__dataclass_fields__[attr_name].type
            actual_type = type(attr)
            message = f"The expected type of {attr_name!r} is " \
                      f"{expected_type!r} but was instead {actual_type!r}"
            try:
                assert isinstance(attr, expected_type)
            except AssertionError:
                raise TypeError(message)


@dataclass(slots=True)
class Item(ConfigObject):
    """A class representing a house item with a name and a type.

    Args:
        name (str): The name of the item.
        type (str, optional): The type of the item. Can be either 'text' or
        'numeric'. Defaults to 'text'.
        text_in_website (str, optional): A string used to search for the item in the
        website's HTML. Defaults to None.
        retrieve (Callable, optional): A function used to retrieve the item from the
        website's HTML. Defaults to None.

    """

    _FIELD_TYPES = ["text", "numeric"]
    name: str
    type: str = field(default="text")
    text_in_website: Optional[str] = field(default=None, repr=False)
    retrieve: Optional[RetrieveItemFn] = field(default=None, repr=False)

    def __post_init__(self):
        super(Item, self).__post_init__()
        self.validate_type()

    def validate_type(self):
        if self.type not in self._FIELD_TYPES:
            raise TypeError(
                f"{self.type} is not a valid type. "
                f"Allowed types: {self._FIELD_TYPES}"
            )


@dataclass(slots=True)
class WebsiteConfig(ConfigObject):
    """A class representing the necessary settings for scraping a website.

    Args:
        name (str): The name of the website.
        main_url (str): The main URL of the website. For example,
        "https://www.funda.nl".
        city_search_url_template (str): URL template for searching for listings in
        a specific city. For example, "https://www.funda.nl/en/koop/{city}/p{page}".
        default_city (str): Default city to use for searches if no city is specified.
        header (dict, optional): dictionary containing the HTTP headers to use for
        requests to the website. Defaults to None.
        parse_only (list, optional): A list of strings representing the HTML tags to
        parse when scraping the website. Defaults to None.
    """

    name: str
    main_url: str
    city_search_url_template: str
    default_city: str
    header: Optional[dict] = None
    parse_only: Optional[list] = None


class NamedItemsDict:
    """A dictionary-like class for storing and accessing named items.

    Args:
        items (ItemContent): A dictionary storing the details of the item.
    """
    _names: list[str]

    def __init__(self, **items: ItemContent):
        self._names = []
        for item in items:
            setattr(self, item, Item(name=item, **items[item]))
            self._names.append(item)

    def map_func_to_attr(self, item: str, func: Callable):
        self[item].retrieve = func

    def __getitem__(self, item):
        return self.__getattribute__(item)

    def __iter__(self):
        for item in self.__dict__:
            if item in self._names:
                yield self.__dict__[item]

    def __len__(self):
        return len(self._names)

    def __repr__(self):
        return f"NamedItemsDict(items={[attr.name for attr in self]})"

    @property
    def names(self):
        return self._names


class SearchResultsItems(NamedItemsDict):
    """General items of the websites containing the search results (shallow pages).

    Args:
        number_of_pages (ItemContent): Dictionary containing the attributes of the
        'number_of_pages' item.
        number_of_listings (ItemContent): Dictionary containing the attributes of
        the 'number_of_listings' item.
        listings (ItemContent): Dictionary containing the attributes of the
        'listings' item.
    """

    def __init__(
            self,
            number_of_pages: ItemContent,
            number_of_listings: ItemContent,
            listings: ItemContent,
    ):
        super(SearchResultsItems, self).__init__(
            number_of_pages=number_of_pages,
            number_of_listings=number_of_listings,
            listings=listings,
        )


class HouseItemsShallow(NamedItemsDict):
    """House items to be retrieved from the search results (shallow)."""

    def __init__(
            self,
            Address: ItemContent,
            LivingArea: ItemContent,
            Price: ItemContent,
            href: ItemContent,
            **kwargs: ItemContent,
    ):
        super(HouseItemsShallow, self).__init__(
            Address=Address, LivingArea=LivingArea, Price=Price, href=href, **kwargs
        )


def config_factory(
        config_type: str, config_dict: Union[dict, dict[str, ItemContent]]
) -> Union[ConfigObject, NamedItemsDict]:
    """Returns a configuration object for the given type.

    Args:
        config_type (str): The type of object to create. Can be either
        'website_settings', 'search_results_items', 'house_items_shallow',
        or 'house_items_deep'.
        config_dict (dict): The data to use for initializing the object.

    Returns:
        Union[ConfigObject, NamedItemsDict]: A `ConfigObject` subclass or a
        `NamedItemsDict` object initialized with the given data.
    """
    config_type_map = {
        "website_settings": WebsiteConfig,
        "search_results_items": SearchResultsItems,
        "house_items_shallow": HouseItemsShallow,
        "house_items_deep": NamedItemsDict,
    }
    return config_type_map[config_type](**config_dict)


@dataclass(slots=True)
class ScraperConfig(ConfigObject):
    """Object containing the configuration for the scraper."""

    website_settings: WebsiteConfig
    search_results_items: SearchResultsItems
    house_items_shallow: HouseItemsShallow
    house_items_deep: Optional[NamedItemsDict]

    @classmethod
    def from_json(cls, json_path: Union[Path, str]):
        """Create a `ScraperConfig` object from a JSON file."""
        with open(json_path, "r") as file:
            data = json.load(file)

        data_by_field = {}
        for field_name in cls.__dataclass_fields__:
            field_data = data.get(field_name)
            data_by_field[field_name] = (
                config_factory(field_name, field_data) if field_data else None
            )

        return cls(**data_by_field)


if __name__ == "__main__":
    path_refactored = Path.cwd() / "countries" / "netherlands" / "funda_config.json"
    conf_refactored = ScraperConfig.from_json(path_refactored)
