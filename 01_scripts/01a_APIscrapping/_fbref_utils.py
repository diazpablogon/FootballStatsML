from __future__ import annotations

from pathlib import Path
from typing import Any, Callable
import importlib.util
import sys

import pandas as pd
import soccerdata as sd
import yaml

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "00_data" / "00a_raw"
CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"


class Logger:
    def __init__(self, verbose: bool = True) -> None:
        self.verbose = verbose

    def info(self, message: str) -> None:
        if self.verbose:
            print(message)

    def warning(self, message: str) -> None:
        print(f"[WARN] {message}")

    def error(self, message: str) -> None:
        print(f"[ERROR] {message}")


def load_config() -> dict[str, Any]:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_base_paths(config: dict[str, Any]) -> tuple[Path, Path]:
    _ = config  # config kept for possible future overrides
    return BASE_DIR, RAW_DIR


def build_fbref_client(league_id: str, season_id: int) -> sd.FBref:
    try:
        return sd.FBref(leagues=[league_id], seasons=[season_id])
    except Exception as exc:  # pragma: no cover - defensive
        raise RuntimeError(f"Failed to create FBref client for {league_id} {season_id}: {exc}") from exc


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = ["_".join(map(str, col)).strip("_") for col in df.columns]
    return df


def safe_call(fn: Callable[..., pd.DataFrame], *args: Any, **kwargs: Any) -> pd.DataFrame:
    try:
        return fn(*args, **kwargs)
    except Exception as exc:  # pragma: no cover - defensive
        print(f"[ERROR] Failed to fetch data: {exc}")
        return pd.DataFrame()


def save_dataframe(df: pd.DataFrame, path: Path, logger: Logger) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(path, index=False)
        logger.info(f"Saved {path}")
    except Exception as exc:
        logger.warning(f"Could not write parquet ({exc}); falling back to CSV")
        csv_path = path.with_suffix(".csv")
        df.to_csv(csv_path, index=False)
        logger.info(f"Saved {csv_path}")


def load_module(module_name: str, filename: str):
    script_dir = Path(__file__).resolve().parent
    file_path = script_dir / filename
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module {module_name} from {file_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def stat_to_camel(stat_type: str) -> str:
    return "".join(part.capitalize() for part in stat_type.split("_"))
