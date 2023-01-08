from pathlib import Path

from real_estate_scraper.logging_mgmt import create_logger
from real_estate_scraper.countries.netherlands.funda_scraper import \
    get_funda_scraper

module_path = Path(__file__)
module_name = module_path.stem
logger = create_logger(module_name)
scraper = get_funda_scraper(logger=logger)

df = scraper.download_to_dataframe(city=None, pages=1)
