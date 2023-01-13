from pathlib import Path
from typing import Optional

from sqlalchemy import create_engine
import pandas as pd


def load_data(db_path: str, table_name: str, columns: Optional[list] = None) -> pd.DataFrame:
    """loads data from a sqlite database to a pandas dataframe"""
    if not Path(db_path).is_file():
        raise ValueError(f"Error: the {db_path} database does not exist.")
    engine = create_engine(f'sqlite:///{db_path}')
    if columns:
        query = f"SELECT {', '.join(columns)} from {table_name}"
        df = pd.read_sql(query, engine)
    else:
        df = pd.read_sql(table_name, engine)
    return df
