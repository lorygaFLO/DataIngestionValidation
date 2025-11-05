from __future__ import annotations
from dotenv import load_dotenv, find_dotenv
import os
from datetime import datetime
from typing import Optional

# Load .env from project root (or nearest parent)
load_dotenv(find_dotenv())


def _as_bool(val: Optional[str], default: bool = False) -> bool:
    """Return a boolean from a string-like env value."""
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "t", "yes", "y"}


def _as_int(val: Optional[str], default: int = 0) -> int:
    """Return an int from a string-like env value, with a safe default."""
    if val is None:
        return default
    try:
        return int(val)
    except ValueError:
        return default


def _as_str(val: Optional[str], default: Optional[str] = None) -> Optional[str]:
    """Return a stripped string or default if empty/None."""
    if val is None:
        return default
    s = str(val).strip()
    return s if s != "" else default


def _is_abs(p: Optional[str]) -> bool:
    """Check if a path is absolute (os-agnostic)."""
    if not p:
        return False
    return os.path.isabs(p)


def _resolve_under(base: str, maybe_relative: Optional[str], default_name: Optional[str] = None) -> str:
    """
    Resolve a path under a base folder if it's not absolute.
    If maybe_relative is None, use default_name under base.
    """
    if maybe_relative:
        return maybe_relative if _is_abs(maybe_relative) else os.path.abspath(os.path.join(base, maybe_relative))
    if default_name:
        return os.path.abspath(os.path.join(base, default_name))
    return os.path.abspath(base)


class SETTINGS:
    """
    Settings loaded from .env and derived values.
    - Creates a consistent run timestamp
    - Builds safe paths using os.path
    - Creates required directories
    - No singleton pattern
    """

    def __init__(self):
        # Stable per-instance timestamp for the entire run
        # Format is friendly for filenames
        self.RUN_ID = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        # Base paths from .env (strings)
        base_path = _as_str(os.getenv("BASEPATH"))
        data_path = _as_str(os.getenv("DATAPATH"))

        # Folders from .env (logical names)
        input_folder = _as_str(os.getenv("INPUT"), "input")
        staging_folder = _as_str(os.getenv("STAGING"), "staging")
        delivery_folder = _as_str(os.getenv("DELIVERY"), "output")
        reports_folder = _as_str(os.getenv("REPORTS"), "reports")
        backups_folder = _as_str(os.getenv("BACKUP"), "backup")

        # Compute BASE_PATH (fallback to cwd)
        self.BASEPATH = os.path.abspath(base_path) if base_path else os.path.abspath(os.getcwd())

        # Compute DATA_PATH
        # If DATA_PATH is absolute, keep it; if relative, place it under BASE_PATH; else use BASE_PATH/data
        if data_path:
            self.DATAPATH = data_path if _is_abs(data_path) else os.path.abspath(os.path.join(self.BASEPATH, data_path))
        else:
            self.DATAPATH = os.path.abspath(os.path.join(self.BASEPATH, "data"))

        # IO options
        self.USE_POLARS = _as_bool(os.getenv("USE_POLARS"), default=True)
        self.GENERATE_REPORTS = _as_bool(os.getenv("GENERATE_REPORTS"), default=False)
        self.CSV_DELIMITER = _as_str(os.getenv("CSV_DELIMITER"), ";")
        self.OUTPUT_FORMAT = (_as_str(os.getenv("OUTPUT_FORMAT"), "parquet") or "parquet").lower()
        if self.OUTPUT_FORMAT not in {"csv", "parquet"}:
            raise ValueError(f"Unsupported OUTPUT_FORMAT: {self.OUTPUT_FORMAT}")


        # Main derived folders
        self.INPUT = input_folder
        self.STAGING = staging_folder
        self.DELIVERY = delivery_folder
        self.REPORTS = reports_folder   
        self.PATH_BACKUP = backups_folder

        self.PATH_INPUT = _resolve_under(self.DATAPATH, self.INPUT)
        self.PATH_STAGING = _resolve_under(self.DATAPATH, self.STAGING)
        self.PATH_DELIVERY = _resolve_under(self.DATAPATH, self.DELIVERY)
        self.PATH_REPORTS = _resolve_under(self.DATAPATH, self.REPORTS)
        self.PATH_BACKUP = _resolve_under(self.DATAPATH, self.PATH_BACKUP)

        # Optional run-scoped output/report folders
        self.PATH_REPORTS_RUN = os.path.abspath(os.path.join(self.PATH_REPORTS, self.RUN_ID))
        self.PATH_STAGING_RUN = os.path.abspath(os.path.join(self.PATH_STAGING, self.RUN_ID))
        self.PATH_DELIVERY_RUN = os.path.abspath(os.path.join(self.PATH_DELIVERY, self.RUN_ID))
        self.PATH_BACKUP_RUN = os.path.abspath(os.path.join(self.PATH_BACKUP, self.RUN_ID))

        # #Ensure directories exist
        self.create_directories(
            self.PATH_INPUT,
            self.PATH_STAGING,
            self.PATH_DELIVERY,
            self.PATH_REPORTS,
            self.PATH_REPORTS_RUN,
            self.PATH_STAGING_RUN,
            self.PATH_DELIVERY_RUN,
        )

    @staticmethod
    def create_directories(*paths: str) -> None:
        """Create folders if they do not exist."""
        for p in paths:
            os.makedirs(p, exist_ok=True)



_S = None

def get_settings():
    """
    Return a lazily-initialized shared SETTINGS instance for the current process.
    Ensures a single RUN_ID and consistent paths across modules.
    """
    global _S
    if _S is None:
        _S = SETTINGS()
    return _S