from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from _fbref_utils import (
    BASE_DIR,
    RAW_DIR,
    Logger,
    build_fbref_client,
    flatten_columns,
    load_config,
    safe_call,
    save_dataframe,
)

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "00_data" / "00a_raw"


def _find_column(schedule: pd.DataFrame, candidates: list[str]) -> str | None:
    lower_map = {col.lower(): col for col in schedule.columns}
    for name in candidates:
        if name.lower() in lower_map:
            return lower_map[name.lower()]
    return None


def compute_ranking(schedule: pd.DataFrame, logger: Logger | None = None) -> pd.DataFrame:
    required_pairs = [
        ("home_goals", "away_goals"),
        ("home_score", "away_score"),
        ("goals_home", "goals_away"),
    ]
    team_candidates = ["home_team", "team_home"]
    opponent_candidates = ["away_team", "team_away"]

    home_team_col = _find_column(schedule, team_candidates)
    away_team_col = _find_column(schedule, opponent_candidates)

    goal_cols: tuple[str, str] | None = None
    for home_goal_name, away_goal_name in required_pairs:
        home_col = _find_column(schedule, [home_goal_name])
        away_col = _find_column(schedule, [away_goal_name])
        if home_col and away_col:
            goal_cols = (home_col, away_col)
            break

    if not home_team_col or not away_team_col or not goal_cols:
        if logger:
            logger.error(
                "Missing columns for ranking computation. Found columns: "
                f"{list(schedule.columns)}"
            )
        return pd.DataFrame(columns=["Pos", "Team", "MP", "W", "D", "L", "GF", "GA", "GD", "Pts"])

    home_goals_col, away_goals_col = goal_cols
    finished = schedule.dropna(subset=[home_goals_col, away_goals_col])
    if finished.empty:
        return pd.DataFrame(columns=["Pos", "Team", "MP", "W", "D", "L", "GF", "GA", "GD", "Pts"])

    home = pd.DataFrame(
        {
            "Team": finished[home_team_col],
            "GF": finished[home_goals_col],
            "GA": finished[away_goals_col],
            "W": (finished[home_goals_col] > finished[away_goals_col]).astype(int),
            "D": (finished[home_goals_col] == finished[away_goals_col]).astype(int),
            "L": (finished[home_goals_col] < finished[away_goals_col]).astype(int),
            "MP": 1,
        }
    )

    away = pd.DataFrame(
        {
            "Team": finished[away_team_col],
            "GF": finished[away_goals_col],
            "GA": finished[home_goals_col],
            "W": (finished[away_goals_col] > finished[home_goals_col]).astype(int),
            "D": (finished[away_goals_col] == finished[home_goals_col]).astype(int),
            "L": (finished[away_goals_col] < finished[home_goals_col]).astype(int),
            "MP": 1,
        }
    )

    totals = pd.concat([home, away], ignore_index=True)
    grouped = totals.groupby("Team", as_index=False).sum(numeric_only=True)
    grouped["GD"] = grouped["GF"] - grouped["GA"]
    grouped["Pts"] = grouped["W"] * 3 + grouped["D"]

    ranking = grouped.sort_values(by=["Pts", "GD", "GF"], ascending=[False, False, False]).reset_index(drop=True)
    ranking.insert(0, "Pos", ranking.index + 1)
    return ranking[["Pos", "Team", "MP", "W", "D", "L", "GF", "GA", "GD", "Pts"]]


def main(
    config: dict[str, Any] | None = None,
    only_leagues: set[str] | None = None,
    only_seasons: set[str] | None = None,
) -> None:
    config = config or load_config()
    logger = Logger(config.get("logging", {}).get("verbose", True))
    fbref_cfg = config.get("fbref", {})

    if not fbref_cfg.get("enable_league_init", True):
        logger.info("League initialization (schedule & ranking) disabled via configuration")
        return

    leagues: dict[str, str] = fbref_cfg.get("leagues", {})
    seasons: dict[str, int] = fbref_cfg.get("seasons", {})

    for season_label, season_id in seasons.items():
        if only_seasons and season_label not in only_seasons:
            continue
        for league_key, league_id in leagues.items():
            if only_leagues and league_key not in only_leagues:
                continue

            logger.info(f"=== {season_label} | {league_key} ({league_id}) ===")
            try:
                fbref = build_fbref_client(league_id, season_id)
            except Exception as exc:
                logger.error(str(exc))
                continue

            schedule = safe_call(fbref.read_schedule)
            schedule = flatten_columns(schedule)
            if schedule.empty:
                logger.warning("Schedule is empty; skipping ranking computation")
                continue

            league_dir = RAW_DIR / season_label / league_key
            schedule_path = league_dir / "Schedule.parquet"
            save_dataframe(schedule, schedule_path, logger)
            logger.info(f"Downloaded schedule: {len(schedule)} rows")

            try:
                ranking = compute_ranking(schedule, logger=logger)
                if ranking.empty:
                    logger.warning("Ranking could not be computed (no finished matches)")
                else:
                    logger.info(f"Computed ranking: {len(ranking)} teams")
                ranking_path = league_dir / "Ranking.parquet"
                save_dataframe(ranking, ranking_path, logger)
            except Exception as exc:  # pragma: no cover - defensive
                logger.error(f"Ranking computation failed: {exc}")


def cli() -> None:
    main()


if __name__ == "__main__":
    main()
