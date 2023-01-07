import pandas as pd
from bs4.element import Tag
from typing import Union

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
