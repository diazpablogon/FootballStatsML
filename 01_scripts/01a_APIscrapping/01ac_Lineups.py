from __future__ import annotations

from pathlib import Path
from typing import Any

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


def main(
    config: dict[str, Any] | None = None,
    only_leagues: set[str] | None = None,
    only_seasons: set[str] | None = None,
) -> None:
    config = config or load_config()
    logger = Logger(config.get("logging", {}).get("verbose", True))
    fbref_cfg = config.get("fbref", {})

    if not fbref_cfg.get("enable_lineups", True):
        logger.info("Lineups download disabled via configuration")
        return

    leagues: dict[str, str] = fbref_cfg.get("leagues", {})
    seasons: dict[str, int] = fbref_cfg.get("seasons", {})

    for season_label, season_id in seasons.items():
        if only_seasons and season_label not in only_seasons:
            continue
        for league_key, league_id in leagues.items():
            if only_leagues and league_key not in only_leagues:
                continue

            logger.info(f"=== Lineups | {season_label} | {league_key} ({league_id}) ===")
            try:
                fbref = build_fbref_client(league_id, season_id)
            except Exception as exc:
                logger.error(str(exc))
                continue

            df = safe_call(fbref.read_lineup)
            df = flatten_columns(df)
            if df.empty:
                logger.warning("Lineups returned no data")
                continue
            output_path = RAW_DIR / season_label / league_key / "Lineups.parquet"
            save_dataframe(df, output_path, logger)


def cli() -> None:
    main()


if __name__ == "__main__":
    main()
