import asyncio

import requests


def get_html_synch(url_str: str, header: dict) -> str:
    """Get the HTML content of a webpage synchronously."""
    response = requests.request("GET", url_str, headers=header)
    return response.text


async def get_html(url: str, header: dict) -> str:
    """Get the HTML content of a webpage asynchronously."""
    return await asyncio.to_thread(get_html_synch, url, header)


if __name__ == '__main__':
    pass
