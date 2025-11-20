"""Scrape LaLiga team statistics tables from FBref and save them as CSV files."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd
import requests
from bs4 import BeautifulSoup


DATA_RAW_PATH = Path("00_data/00a_raw")
DATA_CLEAN_PATH = Path("00_data/00b_clean")
RAW_HTML_FILENAME = DATA_RAW_PATH / "LaLiga_stats_raw.html"


def flatten_columns(columns: pd.Index) -> List[str]:
    """Flatten single or MultiIndex columns into simple strings."""
    flattened = []
    for col in columns:
        if isinstance(col, tuple):
            parts = [str(part) for part in col if str(part) != "nan" and str(part).strip()]
            flattened.append(" ".join(parts).strip())
        else:
            flattened.append(str(col).strip())
    return flattened


def parse_fbref_table(html: str, table_id: str) -> pd.DataFrame:
    """
    Find the <table> with the given id inside the FBref HTML and return it
    as a pandas DataFrame.
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", id=table_id)
    if table is None:
        raise ValueError(f"Table with id '{table_id}' not found in the provided HTML.")

    df = pd.read_html(str(table))[0]
    df.columns = flatten_columns(df.columns)

    if "Squad" in df.columns:
        df = df[df["Squad"].notna()]
        df = df[df["Squad"].astype(str).str.strip().ne("")]
        df = df[df["Squad"] != "Squad"]
    df.reset_index(drop=True, inplace=True)
    return df


def fetch_and_parse_laliga_stats(url: str, table_ids: List[str]) -> Dict[str, pd.DataFrame]:
    """
    Download the LaLiga stats page from FBref, save raw HTML,
    and return a dict {table_id: DataFrame} with the requested tables.
    """
    DATA_RAW_PATH.mkdir(parents=True, exist_ok=True)
    DATA_CLEAN_PATH.mkdir(parents=True, exist_ok=True)

    print("Downloading LaLiga stats from FBref...")
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    try:
        response = requests.get(url, headers=headers, timeout=30)
    except requests.RequestException as exc:
        print(f"Request to {url} failed: {exc}")
        return {}

    if response.status_code != 200:
        print(f"Failed to fetch page. Status code: {response.status_code}")
        return {}

    RAW_HTML_FILENAME.write_text(response.text, encoding="utf-8")
    print(f"Saved raw HTML to {RAW_HTML_FILENAME}")

    tables: Dict[str, pd.DataFrame] = {}
    for table_id in table_ids:
        print(f"Parsing table {table_id}...")
        try:
            df = parse_fbref_table(response.text, table_id)
            tables[table_id] = df
        except ValueError as err:
            print(err)
    return tables


if __name__ == "__main__":
    URL_LALIGA_STATS = "https://fbref.com/en/comps/12/La-Liga-Stats"
    TABLE_IDS = [
        "stats_squads_standard_for",
        "stats_squads_shooting_for",
        "stats_squads_passing_for",
        "stats_squads_defense_for",
        "stats_squads_possession_for",
    ]

    tables = fetch_and_parse_laliga_stats(URL_LALIGA_STATS, TABLE_IDS)
    for table_id, df in tables.items():
        suffix = table_id.replace("stats_squads_", "")
        output_path = DATA_CLEAN_PATH / f"LaLiga_stats_{suffix}.csv"
        df.to_csv(output_path, index=False)
        print(f"Saved CSV: {output_path}")

    if not tables:
        sys.exit("No tables were parsed. Please check the logs above for details.")
