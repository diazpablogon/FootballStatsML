# FootballStatsML FBref Pipeline

A command-line, file-based pipeline for downloading FBref data using [`soccerdata`](https://pypi.org/project/soccerdata/). All outputs are stored under `00_data/00a_raw/<season>/<league>/` with clear, reproducible naming.

## Requirements

- Python 3.11+
- Install dependencies: `pip install -r requirements.txt`

## Project Layout

- `01_scripts/01a_APIscrapping/`
  - `01aa_APIDownloader.py`: Orchestrator entry point
  - `01ab_LeagueInit.py`: Schedule download + ranking computation
  - `01ac_PlayerMatch.py`: Player match-level stats
  - `01ac_PlayerSeason.py`: Player season aggregates
  - `01ac_TeamMatch.py`: Team match-level stats
  - `01ac_TeamSeason.py`: Team season aggregates
  - `_fbref_utils.py`: Shared helpers (config loading, logging, saving)
- `01_scripts/01a_APIscrapping/config.yaml`: Central configuration
- `00_data/00a_raw/`: Output root created automatically

## Configuration

Adjust `01_scripts/01a_APIscrapping/config.yaml` to control the pipeline:

- `data.raw_dir`: relative path for raw outputs (default `00_data/00a_raw`).
- `fbref.leagues`: map of league keys to FBref identifiers.
- `fbref.seasons`: map of season labels to FBref season IDs.
- Toggles: `enable_league_init`, `enable_team_season`, `enable_team_match`, `enable_player_season`, `enable_player_match`.
- `logging.verbose`: enable/disable console logging.

Each script resolves `BASE_DIR` dynamically from its own location, so no absolute paths are required.

## Running the pipeline

From the repository root:

```bash
python 01_scripts/01a_APIscrapping/01aa_APIDownloader.py
```

Optional filters and runtime skip flags:

```bash
python 01_scripts/01a_APIscrapping/01aa_APIDownloader.py \
  --only-leagues LaLiga_ESP,PremierLeague_ENG \
  --only-seasons 2024-25 \
  --skip-league-init \
  --skip-team-season \
  --skip-team-match \
  --skip-player-season \
  --skip-player-match
```

### Output naming

For each `<season>/<league>` pair, files are written to:

- `Ranking.parquet`
- `Schedule.parquet`
- `PlayerMatch/*.parquet`
- `PlayerSeason/*.parquet`
- `TeamMatch/*.parquet`
- `TeamSeason/*.parquet`

Stat filenames use CamelCase, e.g., `Defense.parquet`, `GoalShotCreation.parquet`. If Parquet writing fails (e.g., missing engine), the scripts fall back to CSV with the same stem.

## Ranking robustness

The ranking step now tolerates schedule schema differences by detecting goal/team columns (e.g., `home_goals`/`away_goals`, `home_score`/`away_score`, `goals_home`/`goals_away`). When required columns are missing, the script logs the available columns and continues without crashing.
