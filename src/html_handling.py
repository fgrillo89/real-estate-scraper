import asyncio

import requests


#
# s=requests.Session()

def get_html_synch(url_str: str, header: dict) -> str:
    response = requests.request("GET", url_str, headers=header)
    print(f"Done requesting: {url_str}")
    return response.text


async def get_html(url: str, header: dict) -> str:
    return await asyncio.to_thread(get_html_synch, url, header)


if __name__ == '__main__':
    pass