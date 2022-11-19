import json
from pathlib import Path
from typing import Callable
from dataclasses import dataclass, field

ATTRIBUTES_CONFIG_TYPES = ["house_attributes_shallow",
                           "search_results_attributes",
                           "house_attributes_deep"]


@dataclass(slots=True)
class Attribute:
    name: str
    type: str
    text_in_website: str = field(default=None, repr=False)
    retrieve_func: Callable = field(default=None, repr=False)


class AttributesEnum:
    def __init__(self, attrs: list[dict]):
        self._attributes = []
        for attr in attrs:
            if attr["type"] not in ['text', 'numeric']:
                raise TypeError(f"{attr['type']} is not a valid type. Allowed types: 'numeric' or 'text'")
            setattr(self, attr['name'], Attribute(name=attr['name'],
                                                  type=attr['type'],
                                                  text_in_website=attr.get("text_in_website")))
            self._attributes.append(attr['name'])

    def map_func_to_attr(self, item: str, func: Callable):
        self[item].retrieve_func = func

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
        config = {'header': data['header'], 'website': data['website']}
        for type in ATTRIBUTES_CONFIG_TYPES:
            attr = data.get(type)
            if attr:
                config[type] = AttributesEnum(attr)
        return config


if __name__ == '__main__':
    path = Path.cwd() / 'config' / 'config.json'
    config = config_loader(path)