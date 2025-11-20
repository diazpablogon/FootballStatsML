"""Download schedules from FBref and compute league tables for configured leagues/seasons."""
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import soccerdata as sd

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "00_data"
DATA_RAW_PATH = DATA_DIR / "00a_raw"

LEAGUES: Dict[str, str] = {
    "LaLiga_ESP": "ESP-La Liga",
    "PremierLeague_ENG": "ENG-Premier League",
    "SerieA_ITA": "ITA-Serie A",
    "Ligue1_FRA": "FRA-Ligue 1",
    "Bundesliga_GER": "GER-Bundesliga",
}

SEASONS: Dict[str, int] = {
    "2024-25": 2025,
    "2025-26": 2026,
}


class LeagueNotSupported(Exception):
    """Raised when a requested league is not supported by FBref."""


def build_fbref_client(league_id: str, season_id: int) -> Optional[sd.FBref]:
    """Create an FBref client for the given league and season if supported."""
    available = sd.FBref.available_leagues()
    if league_id not in available:
        print(f"League '{league_id}' is not supported by FBref.available_leagues(), skipping.")
        return None

    print(f"FBref(leagues={league_id}, seasons={season_id})")
    return sd.FBref(leagues=league_id, seasons=season_id)


def read_fbref_schedule(fbref: sd.FBref) -> pd.DataFrame:
    """Read the match schedule from FBref."""
    schedule = fbref.read_schedule()
    if schedule.empty:
        print("Schedule data is empty, skipping.")
        return pd.DataFrame()
    return schedule


def compute_table_from_schedule(schedule: pd.DataFrame) -> pd.DataFrame:
    """Compute a league table from a schedule DataFrame containing scores."""
    required_cols = {"score", "home_team", "away_team"}
    missing_cols = required_cols - set(schedule.columns)
    if missing_cols:
        raise ValueError(f"Schedule is missing required columns: {', '.join(sorted(missing_cols))}")

    valid_schedule = schedule.dropna(subset=["score"]).copy()
    if valid_schedule.empty:
        return pd.DataFrame(columns=["Pos", "Team", "MP", "W", "D", "L", "GF", "GA", "GD", "Pts"])

    scores = valid_schedule["score"].astype(str).str.replace("–", "-", regex=False)
    goals = scores.str.split("-", expand=True)
    if goals.shape[1] < 2:
        raise ValueError("Score column could not be split into home and away goals.")

    valid_schedule["home_goals"] = goals[0].astype(int)
    valid_schedule["away_goals"] = goals[1].astype(int)

    home_stats = pd.DataFrame(
        {
            "Team": valid_schedule["home_team"],
            "MP": 1,
            "W": (valid_schedule["home_goals"] > valid_schedule["away_goals"]).astype(int),
            "D": (valid_schedule["home_goals"] == valid_schedule["away_goals"]).astype(int),
            "L": (valid_schedule["home_goals"] < valid_schedule["away_goals"]).astype(int),
            "GF": valid_schedule["home_goals"],
            "GA": valid_schedule["away_goals"],
        }
    )

    away_stats = pd.DataFrame(
        {
            "Team": valid_schedule["away_team"],
            "MP": 1,
            "W": (valid_schedule["away_goals"] > valid_schedule["home_goals"]).astype(int),
            "D": (valid_schedule["away_goals"] == valid_schedule["home_goals"]).astype(int),
            "L": (valid_schedule["away_goals"] < valid_schedule["home_goals"]).astype(int),
            "GF": valid_schedule["away_goals"],
            "GA": valid_schedule["home_goals"],
        }
    )

    combined = pd.concat([home_stats, away_stats], ignore_index=True)
    grouped = combined.groupby("Team", as_index=False).sum()
    grouped["GD"] = grouped["GF"] - grouped["GA"]
    grouped["Pts"] = grouped["W"] * 3 + grouped["D"]

    sorted_table = grouped.sort_values(by=["Pts", "GD", "GF"], ascending=[False, False, False]).reset_index(drop=True)
    sorted_table.insert(0, "Pos", range(1, len(sorted_table) + 1))

    numeric_cols = ["Pos", "MP", "W", "D", "L", "GF", "GA", "GD", "Pts"]
    sorted_table[numeric_cols] = sorted_table[numeric_cols].astype("int64")

    return sorted_table


def save_league_table_raw(
    table: pd.DataFrame,
    league_key: str,
    season_label: str,
    base_raw_path: Path,
) -> None:
    """Save a league table to parquet, with CSV fallback if no parquet engine is available."""
    season_folder = base_raw_path / season_label
    season_folder.mkdir(parents=True, exist_ok=True)

    parquet_path = season_folder / f"{league_key}.parquet"
    try:
        table.to_parquet(parquet_path, index=False)
        print(f"Saved parquet: {parquet_path}")
    except Exception:
        csv_path = parquet_path.with_suffix(".csv")
        print(f"Could not write parquet (missing engine), falling back to CSV: {csv_path}")
        table.to_csv(csv_path, index=False)


def process_league_season(
    league_key: str,
    league_id: str,
    season_label: str,
    season_id: int,
    base_raw_path: Path,
) -> None:
    """Process one league and season pair: fetch schedule, compute table, and save."""
    print(f"=== {season_label} | {league_key} ({league_id}) ===")
    try:
        fbref_client = build_fbref_client(league_id, season_id)
        if fbref_client is None:
            return

        schedule = read_fbref_schedule(fbref_client)
        if schedule.empty:
            print("No schedule data available, skipping.")
            return

        table = compute_table_from_schedule(schedule)
        print(table.head())
        save_league_table_raw(table, league_key, season_label, base_raw_path)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error processing {league_key} {season_label}: {exc}")


def main() -> None:
    """Run league table extraction for all configured leagues and seasons."""
    print(f"Base directory: {BASE_DIR}")
    print(f"Raw data path: {DATA_RAW_PATH}")

    print("Leagues to process:")
    for key, val in LEAGUES.items():
        print(f"  - {key}: {val}")

    print("Seasons to process:")
    for label, sid in SEASONS.items():
        print(f"  - {label}: {sid}")

    print("Available FBref leagues:")
    print(sd.FBref.available_leagues())

    for season_label, season_id in SEASONS.items():
        for league_key, league_id in LEAGUES.items():
            process_league_season(league_key, league_id, season_label, season_id, DATA_RAW_PATH)


if __name__ == "__main__":
    main()
