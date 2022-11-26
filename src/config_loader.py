import json
from pathlib import Path
from typing import Callable, Union
from dataclasses import dataclass, field

ATTRIBUTES_CONFIG_TYPES = ["house_attributes_shallow",
                           "search_results_attributes",
                           "house_attributes_deep"]


@dataclass(slots=True)
class Item:
    FIELD_TYPES = ['text', 'numeric']
    name: str
    type: str
    text_in_website: Union[None, str] = field(default=None, repr=False)
    retrieve: Union[None, Callable] = field(default=None, repr=False)

    @classmethod
    def from_dict(cls, item_dict):
        kwargs = {attr: item_dict.get(attr) for attr in cls.__dataclass_fields__}
        return cls(**kwargs)

    def __post_init__(self):
        self.validate_type()
        self.validate_fields()

    def validate_fields(self):
        for attr_name in Item.__dataclass_fields__:
            attr = self.__getattribute__(attr_name)
            expected_type = Item.__dataclass_fields__[attr_name].type
            actual_type = type(attr)
            message = f"The expected type of {attr_name!r} is {expected_type!r} and was instead {actual_type!r}"
            try:
                assert isinstance(attr, expected_type)
            except AssertionError:
                raise TypeError(message)

    def validate_type(self):
        if self.type not in self.FIELD_TYPES:
            raise TypeError(f"{self.type} is not a valid type. Allowed types: {self.FIELD_TYPES}")


@dataclass(slots=True)
class WebsiteConfig:
    name: str
    main_url: str
    city_search_url_template: str
    header: Union[None, dict] = field(default=None)
    parse_only: Union[None, list[str]] = field(default=None)


@dataclass(slots=True)
class SearchResultsItems:
    number_of_pages: Item
    number_of_listings: Item
    listings: Item


class ItemsEnum:
    def __init__(self, *attrs: dict):
        self._attributes = []
        for attr in attrs:
            setattr(self, attr['name'], Item.from_dict(attr))
            self._attributes.append(attr['name'])

    def map_func_to_attr(self, item: str, func: Callable):
        self[item].retrieve = func

    def __getitem__(self, item):
        return self.__getattribute__(item)

    def __iter__(self):
        for key in self.__dict__:
            if key in self._attributes:
                yield self.__dict__[key]

    def __repr__(self):
        return f"AttributesEnum(items={[attr.name for attr in self]})"


def config_loader(config_path):
    with open(config_path, 'r') as file:
        data = json.load(file)
        config = {'header': data['header'],
                  'website': data['website'],
                  'parse_only': data.get('parse_only')}
        for conf in ATTRIBUTES_CONFIG_TYPES:
            attr = data.get(conf)
            if attr:
                config[conf] = ItemsEnum(*attr)
        return config


if __name__ == '__main__':
    path = Path.cwd() / 'config' / 'config.json'
    config = config_loader(path)
