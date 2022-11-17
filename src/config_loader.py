import json
from pathlib import Path
from typing import Callable
from dataclasses import dataclass, field

ATTRIBUTES_CONFIG_TYPES = ["house_attributes_shallow", "search_results_attributes"]


@dataclass
class Attribute:
    name: str
    type: str
    retrieve_func: Callable = field(default=None, repr=False)


class AttributesEnum:
    def __init__(self, attrs: list[dict]):
        self._attributes = []
        for attr in attrs:
            if attr["type"] not in ['text', 'numeric']:
                raise ValueError(f"{attr['type']} is not a valid type. Allowed types: 'numeric' or 'text'")
            setattr(self, attr['name'], Attribute(attr['name'], attr['type']))
            self._attributes.append(attr['name'])

    def map_function_to_attribute(self, item: str, func: Callable):
        self[item].retrieve_func = func

    def __getitem__(self, item):
        return self.__getattribute__(item)

    def __iter__(self):
        for key in self.__dict__:
            if key in self._attributes:
                yield self.__dict__[key]

    def __repr__(self):
        return f"AttributesEnum(items={[a.name for a in self]})"


def config_loader(config_path):
    with open(config_path, 'r') as file:
        data = json.load(file)
        config = {'header': data['header']}
        config['website'] = data['website']
        for type in ATTRIBUTES_CONFIG_TYPES:
            attr = data.get(type)
            if attr:
                config[type] = AttributesEnum(attr)
        return config


if __name__ == '__main__':
    path = Path.cwd() / 'config' / 'config.json'
    config = config_loader(path)

#     house_attributes_shallow = [('Address', 'text'),
#                                 ('PostCode', 'text'),
#                                 ('LivingArea', 'numeric'),
#                                 ('PlotSize', 'numeric'),
#                                 ('Price', 'numeric'),
#                                 ('Rooms', 'numeric'),
#                                 ('href', 'text'),
#                                 ('HouseId', 'text')
#                                 ]
#
#     website_attributes = [('pages', 'numeric'),
#                           ('listings', 'numeric')
#                           ]
#
#     # page_enum =
#     house_shallow = house_attributes_factory('HouseShallow', house_attributes_shallow)
#
#
#     def extract_number_of_rooms(soup):
#         result = soup.find('ul', class_="search-result-kenmerken").find_all('li')
#         if len(result) > 1:
#             return result[1]
#
#
#     detail_method_map_sh = {house_shallow.Address: lambda soup: soup.find('h2'),
#                             house_shallow.PostCode: lambda soup: soup.find('h4'),
#                             house_shallow.LivingArea: lambda soup: soup.find(attrs={'title': 'Living area'}),
#                             house_shallow.PlotSize: lambda soup: soup.find(attrs={'title': 'Plot size'}),
#                             house_shallow.Price: lambda soup: soup.find('span', class_='search-result-price'),
#                             house_shallow.Rooms: extract_number_of_rooms,
#                             house_shallow.href: lambda soup: soup.find('a', attrs={"data-object-url-tracking"
#                                                                                    : "resultlist"})
#                                 .get('href'),
#                             house_shallow.HouseId: lambda soup: soup.find('a', attrs={"data-object-url-tracking"
#                                                                                       : "resultlist"})
#                                 .get('data-search-result-item-anchor')
#                             }
