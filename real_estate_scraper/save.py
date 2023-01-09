import asyncio
import sqlite3
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

from real_estate_scraper.utils import get_timestamp

DOWNLOAD_FOLDER = Path.cwd() / "downloads"
FILE_EXTENSIONS = [".csv"]


def generate_name_string(pages: Optional[list] = None,
                         city: Optional[str] = None,
                         deep: bool = False) -> str:
    """Generate a name string for the scraping results"""
    if isinstance(pages, int):
        pages = [pages]

    deep_str = "deep" if deep else "shallow"
    pages_str = "_".join(map(str, pages)) if pages else "all"
    city_str = city if city else "all"
    date_str = get_timestamp(date_only=True)

    return f"City_{city_str}_depth_{deep_str}_pages_{pages_str}_{date_str}"


def generate_filename(pages: Optional[list] = None,
                      city: Optional[str] = None,
                      deep: bool = False,
                      extension: str = ".csv") -> str:
    """Generate a filename for the scraping results"""

    if extension not in FILE_EXTENSIONS:
        raise ValueError(f"Extension {extension} is not supported"
                         f" (supported extensions: {FILE_EXTENSIONS})")

    name_string = generate_name_string(pages=pages, city=city, deep=deep)
    return f"{name_string}{extension}"


def generate_table_name(pages: Optional[list] = None,
                        city: Optional[str] = None,
                        deep: bool = False,
                        schema: str = 'raw') -> str:
    """Generate a table name for the scraping results"""

    return f"{schema}.{generate_name_string(pages=pages, city=city, deep=deep)}"


def create_folder(folder_path: Optional[str] = DOWNLOAD_FOLDER) \
        -> Tuple[Path, Optional[str]]:
    """ Create folder if given path does not exist."""

    path = Path(folder_path)

    if not path.exists():
        path.mkdir()
        msg = f"Folder {folder_path} was created as it did not exist"
    else:
        msg = None

    return path, msg


def file_exists(filepath: str) -> bool:
    file = Path(filepath)
    return file.exists() and file.is_file()


def to_csv(
        df, filepath, index=False, mode="a", encoding="utf-8", header=True, **kwargs
):
    if file_exists(filepath):
        header = False
    df.to_csv(
        filepath, index=index, mode=mode, encoding=encoding, header=header, **kwargs
    )


async def df_to_file_async(
        df: pd.DataFrame, filepath, file_format: str = "csv", **kwargs
):
    FORMAT_MAP = {"csv": to_csv}
    return await asyncio.to_thread(FORMAT_MAP[file_format], df, filepath, **kwargs)


def write_to_sqlite(df: pd.DataFrame, table_name: str, database_name: str):
    with sqlite3.connect(database_name) as conn:
        df.to_sql(table_name, conn, if_exists="append", index=False)


async def write_to_sqlite_async(df: pd.DataFrame, table_name: str, database_name: str):
    return await asyncio.to_thread(write_to_sqlite, df, table_name, database_name)
