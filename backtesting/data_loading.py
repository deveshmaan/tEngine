"""
Phase 1: Data Loading

Goal:
    Load historical OHLCV data into a normalized pandas.DataFrame suitable for
    downstream backtesting phases.

Supported sources:
    - CSV path (string or Path-like)
    - pandas.DataFrame
    - callable fetcher (API-style) returning a DataFrame

Normalized output:
    - DatetimeIndex (optionally timezone-aware)
    - Required columns: open, high, low, close, volume
    - Extra columns preserved (not dropped), and kept after the OHLCV columns

Example (placeholders):
    df = load_ohlcv(
        SAMPLE_DATA_PATH,
        timestamp_col=SAMPLE_TIMESTAMP_COL,
        column_map=SAMPLE_COLUMN_MAP,
        tz=None,
        sort=True,
        drop_duplicates=True,
    )
"""

import inspect
import os
import re
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Union, cast

import pandas as pd

# Public schema constants.
REQUIRED_OHLCV_COLUMNS: Sequence[str] = ("open", "high", "low", "close", "volume")


class OhlcvSchemaError(ValueError):
    """
    Raised when the input cannot be normalized into a valid OHLCV schema.

    Use this exception for schema-level problems:
      - missing required OHLCV columns
      - invalid / non-datetime index
      - unparseable timestamps
      - ambiguous column naming leading to duplicates
    """


def _normalize_token(name: object) -> str:
    """
    Normalize a column name into a token suitable for loose matching.

    Examples:
        " Open " -> "open"
        "Adj Close" -> "adjclose"
        "VOLUME" -> "volume"
    """

    text = str(name).strip().lower()
    # Collapse any non-alphanumeric into nothing (very tolerant matching).
    return re.sub(r"[^a-z0-9]+", "", text)


def normalize_column_names(df: pd.DataFrame, column_map: Optional[Mapping[str, str]] = None) -> pd.DataFrame:
    """
    Return a copy of `df` with normalized OHLCV column names.

    Behavior:
      - Applies an optional explicit `column_map` first (df.rename).
      - Then performs a *minimal* automatic normalization for the required OHLCV
        columns only (case/whitespace tolerant), leaving extra columns unchanged.
      - Raises OhlcvSchemaError if the mapping becomes ambiguous (two input
        columns map to the same required output column).

    Notes:
      - This function does not validate required columns existence; call
        `validate_ohlcv_frame` after normalization.
    """

    # Defensive copy: never mutate caller data.
    out = df.copy()

    # 1) Apply explicit mapping (if provided).
    if column_map:
        explicit: Dict[object, str] = {}
        for src, dst in column_map.items():
            if src is None:
                continue
            # Normalize target *only* for the required OHLCV names.
            dst_text = str(dst).strip()
            dst_token = _normalize_token(dst_text)
            if dst_token in REQUIRED_OHLCV_COLUMNS:
                explicit[src] = dst_token
            else:
                explicit[src] = dst_text
        out = out.rename(columns=explicit)

    # 2) Auto-normalize required OHLCV columns, but preserve extra column names.
    #    We only rename columns that clearly correspond to a required column.
    alias_to_canonical = {
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume",
        # Common abbreviation seen in exports.
        "vol": "volume",
    }

    # Build a list of candidates for each canonical required column.
    candidates: Dict[str, list] = {c: [] for c in REQUIRED_OHLCV_COLUMNS}
    for col in list(out.columns):
        token = _normalize_token(col)
        canonical = alias_to_canonical.get(token)
        if canonical in candidates:
            candidates[canonical].append(col)

    # Any canonical column with >1 candidates is ambiguous and should be rejected.
    for canonical, cols in candidates.items():
        if len(cols) <= 1:
            continue
        raise OhlcvSchemaError(f"Ambiguous columns for '{canonical}': {cols}")

    # Build rename map for columns that need canonical OHLCV names.
    auto_rename: Dict[object, str] = {}
    for canonical, cols in candidates.items():
        if not cols:
            continue
        current = cols[0]
        if str(current) != canonical:
            auto_rename[current] = canonical

    if auto_rename:
        out = out.rename(columns=auto_rename)

    # Final safety check: pandas allows duplicate column names; we do not.
    if out.columns.has_duplicates:
        duplicates = [str(c) for c in out.columns[out.columns.duplicated()].tolist()]
        raise OhlcvSchemaError(f"Duplicate column names after normalization: {duplicates}")

    # Keep a predictable column order: OHLCV first, then extras.
    required = [c for c in REQUIRED_OHLCV_COLUMNS if c in out.columns]
    extras = [c for c in out.columns if c not in set(required)]
    return out[required + extras]


def validate_ohlcv_frame(df: pd.DataFrame, required_cols: Sequence[str] = REQUIRED_OHLCV_COLUMNS) -> None:
    """
    Validate that `df` meets the minimal OHLCV schema requirements.

    Raises:
        OhlcvSchemaError: when required columns/index constraints are not met.
    """

    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Expected pandas.DataFrame, got: {type(df)!r}")

    # Index must be DatetimeIndex for time-series operations downstream.
    if not isinstance(df.index, pd.DatetimeIndex):
        raise OhlcvSchemaError(f"Expected DatetimeIndex, got: {type(df.index)!r}")

    # We reject NaT in the index because it breaks sorting/grouping and is almost
    # always a parsing error upstream.
    if df.index.hasnans:
        raise OhlcvSchemaError("DatetimeIndex contains NaT; check timestamp parsing.")

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise OhlcvSchemaError(f"Missing required OHLCV columns: {missing}. Available: {list(df.columns)}")


def _apply_timezone(index: pd.DatetimeIndex, tz: Optional[str]) -> pd.DatetimeIndex:
    """
    Apply timezone normalization to an index.

    - If tz is None: return unchanged.
    - If index is tz-naive: tz_localize(tz)
    - If index is tz-aware: tz_convert(tz)
    """

    if tz is None:
        return index

    try:
        if index.tz is None:
            return cast(pd.DatetimeIndex, index.tz_localize(tz))
        return cast(pd.DatetimeIndex, index.tz_convert(tz))
    except Exception as exc:
        # Treat invalid timezone / conversion failures as value errors: the
        # user-provided tz is likely incorrect or incompatible.
        raise ValueError(f"Failed to apply timezone '{tz}': {exc}") from exc


def _coerce_timestamp_index(df: pd.DataFrame, timestamp_col: str, tz: Optional[str]) -> pd.DataFrame:
    """
    Ensure df has a DatetimeIndex.

    If timestamp_col exists, parse it and set it as index.
    Otherwise require df.index to already be a DatetimeIndex.
    """

    out = df.copy()

    if timestamp_col in out.columns:
        # Parse timestamps with coercion so we can raise a clean schema error.
        parsed = pd.to_datetime(out[timestamp_col], errors="coerce")
        if parsed.isna().any():
            bad_count = int(parsed.isna().sum())
            raise OhlcvSchemaError(
                f"Failed to parse {bad_count} timestamp value(s) in column '{timestamp_col}'. "
                f"Provide a correct '{timestamp_col}' or pre-parse to datetime."
            )
        out = out.drop(columns=[timestamp_col])
        out.index = pd.DatetimeIndex(parsed)
    else:
        if not isinstance(out.index, pd.DatetimeIndex):
            raise OhlcvSchemaError(
                f"DataFrame must have a DatetimeIndex when '{timestamp_col}' column is absent. "
                f"Got index type: {type(out.index)!r}"
            )

    out.index = _apply_timezone(cast(pd.DatetimeIndex, out.index), tz)
    # Standardize index name for downstream consistency.
    out.index.name = "timestamp"
    return out


def _call_fetcher(fetcher: Callable[..., pd.DataFrame]) -> pd.DataFrame:
    """
    Call a user-provided fetcher with optional (start, end) placeholders.

    The fetcher is expected to return a pandas.DataFrame.
    """

    # Most fetchers are either:
    #   - fetcher() -> DataFrame
    #   - fetcher(start=None, end=None) -> DataFrame
    # We try to call in the most informative way first.
    try:
        sig = inspect.signature(fetcher)
    except (TypeError, ValueError):
        sig = None

    if sig is None:
        result = fetcher()
    else:
        params = sig.parameters
        accepts_kwargs = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())
        wants_start = "start" in params or accepts_kwargs
        wants_end = "end" in params or accepts_kwargs

        if wants_start or wants_end:
            kwargs: Dict[str, Any] = {}
            if wants_start:
                kwargs["start"] = None
            if wants_end:
                kwargs["end"] = None
            try:
                result = fetcher(**kwargs)
            except TypeError:
                # Fall back for positional-only or non-matching kwarg names.
                try:
                    result = fetcher(None, None)
                except TypeError:
                    result = fetcher()
        else:
            result = fetcher()

    if not isinstance(result, pd.DataFrame):
        raise TypeError(f"Fetcher must return pandas.DataFrame, got: {type(result)!r}")
    return result


def load_ohlcv(
    source: Union[str, os.PathLike, pd.DataFrame, Callable[..., pd.DataFrame]],
    timestamp_col: str = "timestamp",
    column_map: Optional[Mapping[str, str]] = None,
    tz: Optional[str] = None,
    sort: bool = True,
    drop_duplicates: bool = True,
) -> pd.DataFrame:
    """
    Load and normalize OHLCV data from multiple supported sources.

    Args:
        source:
            - CSV file path (str / PathLike)
            - pandas.DataFrame
            - callable fetcher returning a DataFrame
        timestamp_col:
            Column name containing timestamps when the data is not already indexed.
        column_map:
            Optional explicit mapping of input column names to desired output names.
            Common use: mapping vendor-specific OHLCV names to open/high/low/close/volume.
        tz:
            Optional timezone name (e.g., "UTC", "Asia/Kolkata").
            - If the index is tz-naive, it will be localized.
            - If the index is tz-aware, it will be converted.
        sort:
            If True, sort by timestamp ascending.
        drop_duplicates:
            If True, drop duplicate timestamps, keeping the last occurrence.

    Returns:
        Normalized pandas.DataFrame with DatetimeIndex and standard OHLCV columns.
    """

    # 1) Load into a DataFrame without mutating user objects.
    if isinstance(source, pd.DataFrame):
        df = source.copy()
    elif isinstance(source, (str, Path, os.PathLike)):
        path = Path(source)
        df = pd.read_csv(path)
    elif callable(source):
        df = _call_fetcher(cast(Callable[..., pd.DataFrame], source)).copy()
    else:
        raise TypeError(f"Unsupported source type: {type(source)!r}")

    # 2) Ensure a DatetimeIndex (parse timestamp column when present).
    df = _coerce_timestamp_index(df, timestamp_col=timestamp_col, tz=tz)

    # 3) Normalize OHLCV column names and validate schema.
    df = normalize_column_names(df, column_map=column_map)
    validate_ohlcv_frame(df)

    # 4) Sort and de-duplicate timestamps (keep last) if configured.
    if sort:
        # Stable sorting ensures deterministic "keep last" semantics for duplicates.
        df = df.sort_index(kind="mergesort")
    if drop_duplicates and df.index.has_duplicates:
        df = df.loc[~df.index.duplicated(keep="last")]

    return df

