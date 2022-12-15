import asyncio
import sqlite3
from pathlib import Path
import pandas as pd


def file_exists(filepath: str) -> bool:
    # Create a Path object from the file path
    file = Path(filepath)

    # Check if the file exists and is a regular file (not a directory or something else)
    return file.exists() and file.is_file()


def to_csv(df, filepath, index=False, mode='a', encoding='utf-8', header=True, **kwargs):
    if file_exists(filepath):
        header = False
    return df.to_csv(filepath, index=index, mode=mode, encoding=encoding, header=header, **kwargs)


async def df_to_file_async(df: pd.DataFrame, filepath: str, file_format: str = 'csv', **kwargs):
    FORMAT_map = {'csv': to_csv}
    return await asyncio.to_thread(FORMAT_map[file_format], df, filepath, **kwargs)


def write_to_sqlite(table_name: str, database_name: str, df: pd.DataFrame):
    with sqlite3.connect(database_name) as conn:
        df.to_sql(table_name, conn, if_exists='append', index=False)
