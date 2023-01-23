import re
from typing import Union, Optional, Tuple

import pandas as pd
from bs4.element import Tag


def str_from_tag(tag: Tag, strip=True, **kwargs) -> Union[None, str]:
    """Get the text of a BeautifulSoup tag"""
    if not isinstance(tag, Tag):
        return tag

    try:
        text = tag.get_text(strip=strip, **kwargs)
        return text
    except AttributeError as e:
        print(e)


def extract_numeric_value(
        string: str, decimal_delimiter: str = ".", thousands_delimiter: str = ","
) -> Optional[float]:
    """Extracts the numeric value from a string, takes into account the unit of
    measure, the thousands' delimiter, the decimal delimiter, and ignores any character
    or space before a digit"""

    if decimal_delimiter == thousands_delimiter:
        raise ValueError("Decimal and group delimiters cannot be the same")

    if not string:
        return None
    string = string.replace(thousands_delimiter, "")
    string = string.replace(decimal_delimiter, ".")
    match = re.search(r"(?<![^\s])\d+(?:\.\d+)?(?![^\s])", string)
    if match:
        numeric_value = float(match.group())
    else:
        return None
    return numeric_value


def extract_rooms_and_bedrooms(string: str) -> Optional[Tuple[float, Optional[float]]]:
    rooms, bedrooms = None, None

    if not string:
        return rooms, bedrooms

    match = re.search(r"(\d+) room[s]?(?: \((\d+) bedroom[s]?\))?", string)

    if match:
        rooms = float(match.group(1))
        bedrooms = float(match.group(2)) if match.group(2) is not None else None

    return rooms, bedrooms


def extract_text_before_and_within_brackets(string: str) \
        -> Optional[Tuple[str, Optional[str]]]:
    before_brackets, within_brackets = None, None

    if not string:
        return before_brackets, within_brackets

    match = re.search(r"([^()]+)(?:\(([^)]*)\))?", string)

    if match:
        before_brackets = match.group(1).strip() if match.group(1).strip() != '' else None
        within_brackets = match.group(2).strip() if match.group(2) else None

    return before_brackets, within_brackets


def extract_dutch_postcode_and_city(string: str) \
        -> Optional[Tuple[str, Optional[str], str]]:
    if not string:
        return None

    match = re.search(r"(\d\d\d\d)(?:\s([A-Z]{2}))?\s([^\d]{2,})", string)

    if match:
        return match.groups()

    return None


def get_retrieval_statistics(
        df: pd.DataFrame, items_list: list[str]
) -> tuple[float, int, int]:
    """Get the retrieval statistics for a list of items"""

    nan_count = df[items_list].isnull().sum(axis=1)
    num_items = len(items_list)
    success_rate = ((num_items - nan_count) / num_items).mean().round(2) * 100
    max_items = num_items - nan_count.min()
    min_items = num_items - nan_count.max()
    return success_rate, max_items, min_items
