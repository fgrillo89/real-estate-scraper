import asyncio
import logging
from asyncio import Semaphore

import aiohttp
from aiolimiter import AsyncLimiter
from geopy import GoogleV3
from geopy.geocoders import Nominatim
from geopy.adapters import AioHTTPAdapter
import geopy
from typing import Tuple, Iterable

from geopy.exc import GeocoderTimedOut, GeocoderServiceError

from real_estate_scraper.html_handling import add_limiter, add_semaphore


def get_coordinates(country: str,
                    city: str,
                    post_code: str = "",
                    address: str = "") -> Tuple[float, float]:
    """Retrieve the latitude and longitude of a given address using geopy library."""

    geolocator = geopy.geocoders.Nominatim(user_agent="real-estate")
    try:
        location = geolocator.geocode(f"{address}, {post_code}, {city}, {country}")
    except GeocoderTimedOut as e:
        location = get_coordinates(country, city, post_code, address)
        logging.error(f"GeocoderTimedOut: {e}")
    except GeocoderServiceError as e:
        logging.error(f"GeocoderServiceError: {e}")
        return None, None

    if location:
        return location.latitude, location.longitude
    else:
        return None, None


class GoogleGeolocator:
    def __init__(self,
                 api_key: str,
                 max_active_requests: int = 10,
                 requests_per_sec: int = 10):
        self.api_key = api_key
        self.max_active_requests = max_active_requests
        self.requests_per_sec = requests_per_sec
        self.semaphore = Semaphore(value=max_active_requests)
        self.limiter = AsyncLimiter(1, round(1 / requests_per_sec, 3))
        self.adapter_factory = AioHTTPAdapter
        self.geolocator = GoogleV3(api_key=api_key, adapter_factory=AioHTTPAdapter)

    async def get_coordinates_async(self, query: str) -> dict:
        async with GoogleV3(api_key=self.api_key,
                            adapter_factory=self.adapter_factory) as geolocator:
            try:
                result = await geolocator.geocode(query)
                return {"query": query,
                        "latitude": result.latitude,
                        "longitude": result.longitude}
            except GeocoderTimedOut:
                # If the geocoder times out, try again
                return await self.get_coordinates_async(query)

    def retrieve_coordinates_from_queries(self, queries: list[str]):
        self.semaphore = Semaphore(value=self.max_active_requests)

        @add_semaphore(semaphore=self.semaphore)
        @add_limiter(limiter=self.limiter)
        async def fetch_all(queries_list):
            results = await asyncio.gather(*(self.get_coordinates_async(query)
                                             for query in queries_list))
            return results

        return asyncio.run(fetch_all(queries))

