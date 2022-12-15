import json
from pathlib import Path
from typing import Callable, Union, Optional
from dataclasses import dataclass, field


@dataclass
class ConfigObject:
    def __post_init__(self):
        self.validate_fields()

    def validate_fields(self):
        for attr_name in self.__dataclass_fields__:
            attr = self.__getattribute__(attr_name)
            expected_type = self.__dataclass_fields__[attr_name].type
            actual_type = type(attr)
            message = f"The expected type of {attr_name!r} is {expected_type!r} but was instead {actual_type!r}"
            try:
                assert isinstance(attr, expected_type)
            except AssertionError:
                raise TypeError(message)


@dataclass(slots=True)
class Item(ConfigObject):
    _FIELD_TYPES = ['text', 'numeric']
    name: str
    type: str = field(default='text')
    text_in_website: Optional[str] = field(default=None, repr=False)
    retrieve: Optional[Callable] = field(default=None, repr=False)

    def __post_init__(self):
        super(Item, self).__post_init__()
        self.validate_type()

    def validate_type(self):
        if self.type not in self._FIELD_TYPES:
            raise TypeError(f"{self.type} is not a valid type. Allowed types: {self._FIELD_TYPES}")


@dataclass(slots=True)
class WebsiteConfig(ConfigObject):
    name: str
    main_url: str
    city_search_url_template: str
    default_city: str
    header: Optional[dict] = None
    parse_only: Optional[list] = None


class NamedItemsDict:
    def __init__(self, **kwargs: dict):
        self._items = []
        for key in kwargs:
            setattr(self, key, Item(name=key, **kwargs[key]))
            self._items.append(key)

    def map_func_to_attr(self, item: str, func: Callable):
        self[item].retrieve = func

    def __getitem__(self, item):
        return self.__getattribute__(item)

    def __iter__(self):
        for item in self.__dict__:
            if item in self._items:
                yield self.__dict__[item]

    def __repr__(self):
        return f"NamedItemsDict(items={[attr.name for attr in self]})"


class SearchResultsItems(NamedItemsDict):
    def __init__(self, number_of_pages: dict, number_of_listings: dict, listings: dict):
        super(SearchResultsItems, self).__init__(number_of_pages=number_of_pages,
                                                 number_of_listings=number_of_listings,
                                                 listings=listings)


class HouseItemsShallow(NamedItemsDict):
    def __init__(self, Address: dict, LivingArea: dict, Price: dict, href: dict, **kwargs):
        super(HouseItemsShallow, self).__init__(Address=Address,
                                                LivingArea=LivingArea,
                                                Price=Price,
                                                href=href,
                                                **kwargs)


def config_factory(config_type: str, config_dict: dict) -> Union[ConfigObject, NamedItemsDict]:
    config_type_map = {"website_settings": WebsiteConfig,
                       "search_results_items": SearchResultsItems,
                       "house_items_shallow": HouseItemsShallow,
                       "house_items_deep": NamedItemsDict
                       }
    return config_type_map[config_type](**config_dict)


@dataclass(slots=True)
class ScraperConfig(ConfigObject):
    website_settings: WebsiteConfig
    search_results_items: SearchResultsItems
    house_items_shallow: HouseItemsShallow
    house_items_deep: Optional[NamedItemsDict]

    @classmethod
    def from_json(cls, json_path: Union[Path, str]):
        with open(json_path, 'r') as file:
            data = json.load(file)

        data_by_field = {}
        for field_name in cls.__dataclass_fields__:
            field_data = data.get(field_name)
            data_by_field[field_name] = config_factory(field_name, field_data) if field_data else None

        return cls(**data_by_field)


if __name__ == '__main__':
    path_refactored = Path.cwd() / 'config' / 'config_refactored.json'
    conf_refactored = ScraperConfig.from_json(path_refactored)
