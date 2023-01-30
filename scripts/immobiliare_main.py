from pathlib import Path
from real_estate_scraper.save import generate_table_name
import pandas as pd

from real_estate_scraper.logging_mgmt import create_logger
from real_estate_scraper.countries.italy.immobiliare import \
    get_immobiliare_scraper
from real_estate_scraper.parsing import normalize_city_names
from tqdm import tqdm

module_path = Path(__file__)
module_name = module_path.stem
logger = create_logger(module_name)
scraper = get_immobiliare_scraper(logger=logger,
                                  max_active_requests=5,
                                  requests_per_sec=5)

cities_file_path = Path.cwd().parent / "real_estate_scraper" / "countries" / "italy" / \
                   "Elenco-comuni-italiani.xls"

cities_df = pd.read_excel(cities_file_path)
all_city_names = cities_df['Denominazione in italiano'].apply(
    normalize_city_names).to_list()

# cities = ["milano", "roma", "messina", "palermo", "anzio", "torino", "venezia",
#           "firenze", "alghero", "bari", "aosta", "trento", "ancona"]

table_name = generate_table_name(pages=None, city=None, deep=True)

for city in tqdm(all_city_names):
    scraper.download_to_db(city=city, deep=True, table_name=table_name)

