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


def numbers_from_str(string: str, join=False) -> Union[None, int, list[int]]:
    """
    Extracts digits from a string
    :param string:
    :param join:
    :return:
    """
    if string is None:
        return None

    results = re.findall('\d+', string)

    if not results:
        return None

    if join:
        number_str = ''.join(results)
        return int(number_str)

    return list(map(int, results))
