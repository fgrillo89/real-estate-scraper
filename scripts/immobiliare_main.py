from pathlib import Path

from real_estate_scraper.logging_mgmt import create_logger
from real_estate_scraper.countries.italy.immobiliare import \
    get_immobiliare_scraper

module_path = Path(__file__)
module_name = module_path.stem
logger = create_logger(module_name)
scraper = get_immobiliare_scraper(logger=logger)

df = scraper.download_to_dataframe(city='roma', pages=1)