from __future__ import annotations

import argparse
from copy import deepcopy
from pathlib import Path
from typing import Any

from _fbref_utils import BASE_DIR, RAW_DIR, Logger, load_config, load_module

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "00_data" / "00a_raw"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="FBref data downloader orchestrator")
    parser.add_argument(
        "--only-leagues",
        type=str,
        help="Comma-separated league keys to process",
    )
    parser.add_argument(
        "--only-seasons",
        type=str,
        help="Comma-separated season labels to process",
    )
    parser.add_argument("--skip-player-match", action="store_true", help="Skip player match stats")
    parser.add_argument("--skip-player-season", action="store_true", help="Skip player season stats")
    parser.add_argument("--skip-team-match", action="store_true", help="Skip team match stats")
    parser.add_argument("--skip-team-season", action="store_true", help="Skip team season stats")
    return parser.parse_args()


def apply_runtime_flags(config: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    cfg = deepcopy(config)
    fbref_cfg = cfg.setdefault("fbref", {})
    if args.skip_player_match:
        fbref_cfg["enable_player_match"] = False
    if args.skip_player_season:
        fbref_cfg["enable_player_season"] = False
    if args.skip_team_match:
        fbref_cfg["enable_team_match"] = False
    if args.skip_team_season:
        fbref_cfg["enable_team_season"] = False
    return cfg


def parse_filter(value: str | None) -> set[str] | None:
    if not value:
        return None
    return {item.strip() for item in value.split(",") if item.strip()}


def run_step(
    title: str,
    module_name: str,
    filename: str,
    config: dict[str, Any],
    only_leagues: set[str] | None,
    only_seasons: set[str] | None,
    logger: Logger,
) -> None:
    logger.info(title)
    try:
        module = load_module(module_name, filename)
        module.main(config=config, only_leagues=only_leagues, only_seasons=only_seasons)
    except Exception as exc:  # pragma: no cover - defensive
        logger.error(f"Step failed: {exc}")


def main() -> None:
    args = parse_args()
    config = load_config()
    config = apply_runtime_flags(config, args)
    logger = Logger(config.get("logging", {}).get("verbose", True))

    only_leagues = parse_filter(args.only_leagues)
    only_seasons = parse_filter(args.only_seasons)

    steps = [
        ("=== STEP 1: LeagueInit (Ranking + Schedule) ===", "module_league_init", "01ab_LeagueInit.py"),
        ("=== STEP 2: TeamSeason ===", "module_team_season", "01ac_TeamSeason.py"),
        ("=== STEP 3: TeamMatch ===", "module_team_match", "01ac_TeamMatch.py"),
        ("=== STEP 4: PlayerSeason ===", "module_player_season", "01ac_PlayerSeason.py"),
        ("=== STEP 5: PlayerMatch ===", "module_player_match", "01ac_PlayerMatch.py"),
    ]

    for title, module_name, filename in steps:
        run_step(title, module_name, filename, config, only_leagues, only_seasons, logger)


if __name__ == "__main__":
    main()
