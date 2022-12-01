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
    header: Optional[dict] = None
    parse_only: Optional[list] = None


@dataclass(slots=True)
class SearchResultsItems(ConfigObject):
    number_of_pages: Item
    number_of_listings: Item
    listings: Item

    @classmethod
    def from_dict(cls, items_dict):
        kwargs = {name: Item(name, **items_dict[name]) for name in items_dict}
        return cls(**kwargs)


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
        return f"NamedItemsList(items={[attr.name for attr in self]})"


def config_factory(config_type: str, config_dict: dict) -> Union[ConfigObject, NamedItemsDict]:
    config_type_map = {"website_settings": lambda x: WebsiteConfig(**x),
                       "search_results_items": lambda x: SearchResultsItems.from_dict(x),
                       "house_items_shallow": lambda x: NamedItemsDict(**x),
                       "house_items_deep": lambda x: NamedItemsDict(**x)
                       }
    return config_type_map[config_type](config_dict)


@dataclass(slots=True)
class ScraperConfig(ConfigObject):
    website_settings: WebsiteConfig
    search_results_items: SearchResultsItems
    house_items_shallow: NamedItemsDict
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
    with open(path_refactored, 'r') as file:
        json_ref = json.load(file)
    conf_refactored = ScraperConfig.from_json(path_refactored)
