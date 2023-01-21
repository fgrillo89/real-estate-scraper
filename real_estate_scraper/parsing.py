import re
from typing import Union

import pandas as pd
from bs4.element import Tag

from real_estate_scraper.configuration import NamedHouseItems


def str_from_tag(tag: Tag, strip=True, **kwargs) -> Union[None, str]:
    """Get the text of a BeautifulSoup tag"""
    if not isinstance(tag, Tag):
        return tag

    try:
        text = tag.get_text(strip=strip, **kwargs)
        return text
    except AttributeError as e:
        print(e)


def extract_numeric_value_from_string(string: str, decimal_delimiter: str = ".",
                                      group_delimiter: str = ","):
    """Extracts a numeric value, including decimal parts if any, from a string
    containing delimiters and optional unit of measure"""
    if not string:
        return None
    if decimal_delimiter == group_delimiter:
        raise ValueError("Decimal and group delimiters cannot be the same")
    string = string.replace(group_delimiter, "")
    match_list = re.findall('[\d' + decimal_delimiter + ']+', string)
    if match_list:
        string = ''.join(match_list)
        string = string.replace(decimal_delimiter, '.')
        return float(string)


def parse_dataframe(house_attributes: NamedHouseItems, df: pd.DataFrame) -> pd.DataFrame:
    """Convert the values in the dataframe to the appropriate data type."""
    for attribute in house_attributes:
        if attribute.type == "numeric":
            df[attribute.name] = pd.to_numeric(
                df[attribute.name].str.replace("\D+", "", regex=True)
            )
    return df


def get_retrieval_statistics(df: pd.DataFrame, items_list: list[str]) \
        -> tuple[float, int, int]:
    """Get the retrieval statistics for a list of items"""

    nan_count = df[items_list].isnull().sum(axis=1)
    num_items = len(items_list)
    success_rate = ((num_items - nan_count) / num_items).mean().round(2) * 100
    max_items = num_items - nan_count.min()
    min_items = num_items - nan_count.max()
    return success_rate, max_items, min_items



