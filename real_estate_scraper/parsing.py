from typing import Union

import pandas as pd
from bs4.element import Tag

from real_estate_scraper.configuration import NamedItemsDict


def str_from_tag(tag: Tag, strip=True, **kwargs) -> Union[None, str]:
    """Get the text of a BeautifulSoup tag"""
    if tag is None:
        return None

    try:
        text = tag.get_text(strip=strip, **kwargs)
        return text
    except AttributeError as e:
        print(e)


def parse_dataframe(house_attributes: NamedItemsDict, df: pd.DataFrame) -> pd.DataFrame:
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
