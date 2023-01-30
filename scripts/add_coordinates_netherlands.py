from pathlib import Path
import json

import pandas as pd

from real_estate_scraper.database import load_data
from real_estate_scraper.geolocalization import GoogleGeolocator
from real_estate_scraper.save import write_to_sqlite
from tqdm import tqdm

from real_estate_scraper.utils import split_list

database_name = str(Path.cwd() / 'downloads' / 'funda.db')
table_name = 'raw.all_deep_2022_12_24'

with open('google_api.json') as file:
    API_KEY = json.load(file)['api_key']

df = load_data(db_path=database_name, table_name=table_name)

queries = df.apply(lambda x: f"{x.Address}, {x.PostCode}, the Netherlands",
                   axis=1)

chunks = split_list(queries, chunksize=100)

geolocator = GoogleGeolocator(api_key=API_KEY)

table_name_coordinates = f"{table_name}_coordinates"

for chunk in tqdm(chunks, total=len(chunks)):
    coordinates = geolocator.retrieve_coordinates_from_queries(chunk)

    df_coordinates = pd.DataFrame(coordinates).dropna()

    df_final = df_coordinates.assign(
        Address=df_coordinates.apply(lambda x: x['query'].split(", ")[0], axis=1),
        PostCode=df_coordinates.apply(lambda x: x['query'].split(", ")[1], axis=1)
    )[['Address', 'PostCode', 'latitude', 'longitude']]

    write_to_sqlite(df_final, table_name_coordinates, database_name=database_name)
