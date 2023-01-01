import asyncio
import requests
import logging

logger = logging.getLogger('main_logger')


def get_html_synch(url_str: str, header: dict) -> str:
    """Get the HTML content of a webpage synchronously.

    Args:
        url_str (str): The URL of the webpage.
        header (dict): A dictionary containing the HTTP headers to use for the request.

    Returns:
        str: The HTML content of the webpage.
    """

    response = requests.request("GET", url_str, headers=header)
    logger.info(f"Done requesting: {url_str}")
    return response.text


async def get_html(url: str, header: dict) -> str:
    """Get the HTML content of a webpage asynchronously.

    Args:
        url (str): The URL of the webpage.
        header (dict): A dictionary containing the HTTP headers to use for the request.

    Returns:
        str: The HTML content of the webpage.
    """

    return await asyncio.to_thread(get_html_synch, url, header)


if __name__ == '__main__':
    pass
