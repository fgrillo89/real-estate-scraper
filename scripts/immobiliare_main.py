from pathlib import Path

from real_estate_scraper.logging_mgmt import create_logger
from real_estate_scraper.countries.italy.immobiliare import \
    get_immobiliare_scraper

module_path = Path(__file__)
module_name = module_path.stem
logger = create_logger(module_name)
scraper = get_immobiliare_scraper(logger=logger,
                                  max_active_requests=5,
                                  requests_per_sec=5)

cities = ["milano", "roma", "messina", "palermo", "anzio", "torino", "venezia",
          "firenze", "alghero", "bari", "aosta", "trento", "ancona"]

for city in cities:
    scraper.download_to_db(city=city, deep=True)
