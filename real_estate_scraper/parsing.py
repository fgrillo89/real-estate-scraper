import re
from typing import Union, Optional

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


def extract_numeric_value(string: str, decimal_delimiter: str = ".",
                          thousands_delimiter: str = ",") -> Optional[float]:
    """Extracts the numeric value from a string, takes into account the unit of
    measure, the thousands' delimiter, the decimal delimiter, and ignores any character
    or space before a digit"""

    if decimal_delimiter == thousands_delimiter:
        raise ValueError("Decimal and group delimiters cannot be the same")

    if not string:
        return None
    string = string.replace(thousands_delimiter, "")
    string = string.replace(decimal_delimiter, ".")
    match = re.search(r'(?<![^\s])\d+(?:\.\d+)?(?![^\s])', string)
    if match:
        numeric_value = float(match.group())
    else:
        return None
    return numeric_value


def get_retrieval_statistics(df: pd.DataFrame, items_list: list[str]) \
        -> tuple[float, int, int]:
    """Get the retrieval statistics for a list of items"""

    nan_count = df[items_list].isnull().sum(axis=1)
    num_items = len(items_list)
    success_rate = ((num_items - nan_count) / num_items).mean().round(2) * 100
    max_items = num_items - nan_count.min()
    min_items = num_items - nan_count.max()
    return success_rate, max_items, min_items
