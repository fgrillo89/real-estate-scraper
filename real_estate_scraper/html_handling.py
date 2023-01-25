from typing import Union

import aiohttp


async def get_response(url_str: str,
                       header: dict,
                       read_format: str = "text",
                       max_retries: int = 3,
                       timeout: int = 10) -> Union[str, dict, list]:
    retries = 0
    while retries < max_retries:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url_str,
                                       headers=header,
                                       timeout=timeout) as response:
                    response.raise_for_status()
                    return await process_response(response, read_format=read_format)
        except aiohttp.ClientError as e:
            retries += 1
            if retries == max_retries:
                raise e
            print(f'Retrying request to {url_str} (attempt {retries}/{max_retries})')


async def process_response(response: aiohttp.ClientResponse, read_format: str = "text") \
        -> Union[str, dict, list]:
    method_factory = {"text": lambda x: x.content.read(),
                      "json": lambda x: x.content.json()}
    return await method_factory[read_format](response)


# def get_response_synch(url_str: str, header: dict) -> requests.Response:
#     return requests.request("GET", url_str, headers=header)


# async def get_response(url: str, header: dict) -> requests.Response:
#     return await asyncio.to_thread(get_response_synch, url, header)

if __name__ == "__main__":
    pass
