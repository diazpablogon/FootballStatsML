# LaLiga FBref Scraper

## Introduction
This repository contains the first step of a broader football analytics and prediction project focused on LaLiga. The goal is to build models for match outcomes (1X2) using **only** FBref data, starting with team-level statistics that can be fed into later modelling stages.

## Project Structure
```
.
├── 00_data/
│   ├── 00a_raw/
│   └── 00b_clean/
└── 01_scripts/
    └── 01a_scrapping_LaLiga.py
```

## Data Source
- The only data source is [FBref](https://fbref.com/).
- Main LaLiga stats page: https://fbref.com/en/comps/12/La-Liga-Stats
- The scraper focuses on team-level ("squads") tables such as standard, shooting, passing, defense, and possession stats.

## Scraper Overview
`01_scripts/01a_scrapping_LaLiga.py`:
- Downloads the LaLiga stats HTML from FBref.
- Saves the raw HTML to `00_data/00a_raw/LaLiga_stats_raw.html`.
- Parses several team-level tables by their FBref table IDs (standard, shooting, passing, defense, possession).
- Cleans and saves each table as CSV in `00_data/00b_clean/LaLiga_stats_*.csv`.
- Designed to be modular so you can add more table IDs or swap in other leagues later.

## Installation
```bash
pip install requests beautifulsoup4 pandas pyarrow
```

## Usage
Run the scraper from the project root:
```bash
python 01_scripts/01a_scrapping_LaLiga.py
```
What happens when it runs:
- Required folders are created if missing.
- The FBref LaLiga page is downloaded.
- The specified tables are parsed and saved as CSVs.

## Output
After running, you should see files like:
- `00_data/00a_raw/LaLiga_stats_raw.html`
- `00_data/00b_clean/LaLiga_stats_standard_for.csv`
- `00_data/00b_clean/LaLiga_stats_shooting_for.csv`
- `00_data/00b_clean/LaLiga_stats_passing_for.csv`
- `00_data/00b_clean/LaLiga_stats_defense_for.csv`
- `00_data/00b_clean/LaLiga_stats_possession_for.csv`

## Extending the Project
This scraper is the base layer for the wider analytics pipeline:
- Add more FBref tables or leagues by updating the URL and table IDs.
- Future steps will include scraping fixtures and results at the match level.
- Machine learning models for match prediction (1X2) will be built on top of the cleaned datasets.
