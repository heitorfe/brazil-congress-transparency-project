"""
Shared utilities for Senate data extraction scripts.

Usage in extractors:
    from utils import configure_utf8, unwrap_list, month_windows, month_date_windows, save_parquet
"""

import sys
from calendar import monthrange
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl


def configure_utf8() -> None:
    """Force UTF-8 stdout on Windows to avoid cp1252 encoding errors.

    Call once at the top of every extractor's __main__ block (or module level).
    Safe to call multiple times.
    """
    if sys.stdout.encoding != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def unwrap_list(obj: list | dict | None) -> list:
    """Return a guaranteed list.

    The Senate LEGIS API uses XML-to-JSON conversion that returns a single dict
    instead of a 1-element list when there is only one child element.
    This guard normalises all three cases:
      None  → []
      dict  → [dict]
      list  → list (unchanged)
    """
    if obj is None:
        return []
    if isinstance(obj, dict):
        return [obj]
    return list(obj)


def month_windows(start: date, end: date) -> list[tuple[int, int]]:
    """Return (year, month) int tuples from start to end, capped at today.

    Used by ADM extractors whose endpoints take year/month as URL path
    segments (e.g. /remuneracoes/{ano}/{mes}).

    Example:
        month_windows(date(2024, 11, 1), date(2025, 3, 1))
        # → [(2024, 11), (2024, 12), (2025, 1), (2025, 2), (2025, 3)]
        # (capped at today if today < 2025-03)
    """
    today = date.today()
    cutoff_y, cutoff_m = min((end.year, end.month), (today.year, today.month))
    windows: list[tuple[int, int]] = []
    y, m = start.year, start.month
    while (y, m) <= (cutoff_y, cutoff_m):
        windows.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return windows


def month_date_windows(start: date, end: date) -> list[tuple[date, date]]:
    """Return (window_start, window_end) date pairs, one per calendar month.

    Used by LEGIS extractors whose endpoints take ISO date strings as query
    params (e.g. /votacao?dataInicio=...&dataFim=...).

    Each window spans from the 1st to the last day of the month.
    The final window is capped at min(end, today).

    Example:
        month_date_windows(date(2025, 1, 1), date(2025, 3, 31))
        # → [(date(2025,1,1), date(2025,1,31)),
        #    (date(2025,2,1), date(2025,2,28)),
        #    (date(2025,3,1), date(2025,3,31))]
    """
    today = date.today()
    cutoff = min(end, today)
    windows: list[tuple[date, date]] = []
    y, m = start.year, start.month
    while date(y, m, 1) <= cutoff:
        last_day = monthrange(y, m)[1]
        window_end = min(date(y, m, last_day), cutoff)
        windows.append((date(y, m, 1), window_end))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return windows


def save_parquet(
    records: list[dict],
    path: Path,
    *,
    unique_subset: list[str] | None = None,
    sort_by: list[str] | None = None,
    safe_schema: bool = False,
) -> int:
    """Build a Polars DataFrame, optionally deduplicate and sort, then write Parquet.

    Parameters
    ----------
    records : list[dict]
        Flattened records to save.
    path : Path
        Output .parquet path. Parent directory is created if it doesn't exist.
    unique_subset : list[str] | None
        If given, deduplicate rows by these columns.
    sort_by : list[str] | None
        If given, sort by these columns after deduplication.
    safe_schema : bool
        Pass ``infer_schema_length=len(records)`` to Polars. Use True for ADM
        payroll / staff data where optional string fields may be None for the
        first N records, causing Polars to infer Null type and then fail when
        a real string arrives.

    Returns
    -------
    int
        Number of rows written (after deduplication).
    """
    if not records:
        print(f"  WARNING: no records to write to {path}")
        return 0

    kwargs: dict[str, Any] = {}
    if safe_schema:
        kwargs["infer_schema_length"] = len(records)

    df = pl.DataFrame(records, **kwargs)

    if unique_subset:
        df = df.unique(subset=unique_subset)
    if sort_by:
        df = df.sort(sort_by)

    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    return len(df)


