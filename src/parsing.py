from enum import Enum

import pandas as pd
from bs4.element import Tag
from typing import Union
import re



def str_from_tag(tag: Tag, strip=True, **kwargs) -> Union[None, str]:
    """
    Get the text of a BeautifulSoup tag
    :param tag:
    :param strip:
    :param kwargs:
    :return:
    """
    if tag is None:
        return None

    try:
        text = tag.get_text(strip=strip, **kwargs)
        return text
    except AttributeError as e:
        print(e)


def parse_shallow_dataframe(house_shallow: Enum, df: pd.DataFrame) -> pd.DataFrame:
    for attribute in house_shallow:
        if attribute.value.type == 'numeric':
            df[attribute.name] = pd.to_numeric(df[attribute.name].str.replace('\D+', '', regex=True))
    return df


#

# def digits_from_str(string: str, join=False) -> Union[None, int, list[int]]:
#     """
#     Extracts digits from a string
#     :param string:
#     :param join:
#     :return:
#     """
#     if string is None:
#         return None
#
#     digits_list = re.findall('\d+', string)
#
#     if not digits_list:
#         return None
#
#     if join:
#         digits_str = ''.join(digits_list)
#         return int(digits_str)
#
#     return list(map(int, digits_list))
