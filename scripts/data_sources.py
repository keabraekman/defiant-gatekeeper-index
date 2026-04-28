"""Programmatic data-source helpers for the Defiant Gatekeeper Index."""

from __future__ import annotations

import io
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


FRED_OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"
ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"


def _missing(source: str, message: str) -> dict[str, Any]:
    return {
        "value": None,
        "date": None,
        "source": source,
        "freshness": "missing",
        "history": [],
        "error": message,
    }


def fetch_fred_series(series_id: str, api_key: str | None) -> dict[str, Any]:
    """Fetch recent FRED observations for one series.

    Returns the latest numeric value, latest date, and a numeric history list.
    Errors are returned as data instead of raised so a static build can still
    deploy with neutral/mock values.
    """

    source = f"FRED:{series_id}"
    if not api_key:
        return _missing(source, "Missing FRED_API_KEY")

    import requests

    observation_start = (datetime.utcnow().date() - timedelta(days=5 * 365)).isoformat()
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": observation_start,
        "sort_order": "asc",
    }

    try:
        response = requests.get(FRED_OBSERVATIONS_URL, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        return _missing(source, f"FRED request failed: {exc}")
    except ValueError as exc:
        return _missing(source, f"FRED JSON parse failed: {exc}")

    observations: list[dict[str, Any]] = []
    for item in payload.get("observations", []):
        raw_value = item.get("value")
        if raw_value in (None, "."):
            continue
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            continue
        observations.append({"date": item.get("date"), "value": value})

    if not observations:
        return _missing(source, "FRED returned no numeric observations")

    latest = observations[-1]
    return {
        "value": latest["value"],
        "date": latest["date"],
        "source": source,
        "freshness": "unknown",
        "history": observations,
    }


def fetch_alpha_vantage_daily_adjusted(symbol: str, api_key: str | None) -> dict[str, Any]:
    """Fetch Alpha Vantage daily adjusted close history for one symbol."""

    source = f"Alpha Vantage:{symbol}"
    if not api_key:
        return _missing(source, "Missing ALPHA_VANTAGE_API_KEY")

    import requests

    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": symbol,
        "apikey": api_key,
        "outputsize": "compact",
    }

    try:
        response = requests.get(ALPHA_VANTAGE_URL, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        return _missing(source, f"Alpha Vantage request failed: {exc}")
    except ValueError as exc:
        return _missing(source, f"Alpha Vantage JSON parse failed: {exc}")

    if "Error Message" in payload:
        return _missing(source, str(payload["Error Message"]))
    if "Note" in payload:
        return _missing(source, str(payload["Note"]))
    if "Information" in payload:
        return _missing(source, str(payload["Information"]))

    time_series = payload.get("Time Series (Daily)")
    if not isinstance(time_series, dict):
        return _missing(source, "Alpha Vantage response did not include daily time series")

    history: list[dict[str, Any]] = []
    for observed_date, values in time_series.items():
        adjusted_close = values.get("5. adjusted close")
        try:
            close = float(adjusted_close)
        except (TypeError, ValueError):
            continue
        history.append({"date": observed_date, "value": close})

    history.sort(key=lambda item: item["date"])
    if len(history) < 64:
        return _missing(source, "Alpha Vantage returned fewer than 64 trading days")

    latest = history[-1]
    return {
        "value": latest["value"],
        "date": latest["date"],
        "source": source,
        "freshness": "unknown",
        "history": history,
    }


def calculate_relative_strength(
    adjusted_price_histories: dict[str, dict[str, Any]],
    benchmark_symbol: str = "SPY",
    lookback_days: int = 63,
) -> dict[str, Any]:
    """Calculate lookback relative performance versus a benchmark.

    The output value is a map of symbol to percentage-point out/underperformance
    versus SPY over the same trading-day window.
    """

    benchmark = adjusted_price_histories.get(benchmark_symbol)
    benchmark_history = benchmark.get("history", []) if benchmark else []
    if len(benchmark_history) <= lookback_days:
        return _missing("Alpha Vantage:ETF relative strength", "Missing benchmark history")

    try:
        benchmark_return = (
            benchmark_history[-1]["value"] / benchmark_history[-1 - lookback_days]["value"] - 1
        ) * 100
    except (KeyError, TypeError, ZeroDivisionError):
        return _missing("Alpha Vantage:ETF relative strength", "Invalid benchmark history")

    relative: dict[str, float] = {}
    latest_dates: list[str] = []
    for symbol, series in adjusted_price_histories.items():
        if symbol == benchmark_symbol:
            continue
        history = series.get("history", [])
        if len(history) <= lookback_days:
            continue
        try:
            symbol_return = (history[-1]["value"] / history[-1 - lookback_days]["value"] - 1) * 100
        except (KeyError, TypeError, ZeroDivisionError):
            continue
        relative[symbol] = round(symbol_return - benchmark_return, 2)
        if history[-1].get("date"):
            latest_dates.append(history[-1]["date"])

    if not relative:
        return _missing("Alpha Vantage:ETF relative strength", "No ETF relative strength values")

    return {
        "value": relative,
        "date": max(latest_dates) if latest_dates else benchmark.get("date"),
        "source": "Alpha Vantage:SPY,QQQ,SMH,XLK,IWM",
        "freshness": "unknown",
        "history": [],
    }


def fetch_finra_margin_debt(url: str | None) -> dict[str, Any]:
    """Fetch and parse a configurable FINRA margin-debt CSV/XLS/XLSX URL."""

    source = "FINRA:Margin Debt"
    if not url:
        return _missing(source, "Missing FINRA_MARGIN_DEBT_URL")

    import requests

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        return _missing(source, f"FINRA request failed: {exc}")

    content = response.content
    suffix = Path(url.split("?")[0]).suffix.lower()

    try:
        frames = _read_finra_frames(content, suffix)
        for frame in frames:
            parsed = _parse_finra_frame(frame)
            if parsed:
                latest = parsed[-1]
                return {
                    "value": latest["value"],
                    "date": latest["date"],
                    "source": source,
                    "freshness": "unknown",
                    "history": parsed,
                }
    except Exception as exc:  # noqa: BLE001 - return as data-quality issue.
        return _missing(source, f"FINRA parse failed: {exc}")

    return _missing(source, "FINRA file did not include recognizable margin debt data")


def _read_finra_frames(content: bytes, suffix: str) -> list[Any]:
    import pandas as pd

    buffer = io.BytesIO(content)
    if suffix in {".xls", ".xlsx"}:
        sheets = pd.read_excel(buffer, sheet_name=None)
        return list(sheets.values())

    try:
        return [pd.read_csv(buffer)]
    except UnicodeDecodeError:
        buffer.seek(0)
        return [pd.read_csv(buffer, encoding="latin-1")]


def _parse_finra_frame(frame: Any) -> list[dict[str, Any]]:
    import pandas as pd

    df = frame.copy()
    df = df.dropna(how="all")
    if df.empty:
        return []

    df.columns = [str(column).strip() for column in df.columns]
    lower_columns = {column: column.lower() for column in df.columns}

    date_series = _extract_date_series(df, lower_columns)
    value_column = _find_margin_debt_column(df, lower_columns)
    if date_series is None or value_column is None:
        return []

    value_series = (
        df[value_column]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("$", "", regex=False)
        .str.replace(" ", "", regex=False)
    )
    numeric_values = pd.to_numeric(value_series, errors="coerce")
    parsed = pd.DataFrame({"date": date_series, "value": numeric_values})
    parsed = parsed.dropna(subset=["date", "value"]).sort_values("date")

    history = [
        {"date": row.date.date().isoformat(), "value": float(row.value)}
        for row in parsed.itertuples(index=False)
    ]
    return history


def _extract_date_series(
    df: Any, lower_columns: dict[str, str]
) -> Any | None:
    import pandas as pd

    year_col = next((col for col, lower in lower_columns.items() if lower == "year"), None)
    month_col = next((col for col, lower in lower_columns.items() if lower == "month"), None)
    if year_col and month_col:
        combined = df[year_col].astype(str).str.strip() + "-" + df[month_col].astype(str).str.strip()
        return pd.to_datetime(combined, errors="coerce")

    date_candidates = [
        col
        for col, lower in lower_columns.items()
        if any(token in lower for token in ["date", "month", "period"])
    ]
    if not date_candidates and len(df.columns) > 0:
        date_candidates = [df.columns[0]]

    for column in date_candidates:
        parsed = pd.to_datetime(df[column], errors="coerce")
        if parsed.notna().sum() >= 2:
            return parsed
    return None


def _find_margin_debt_column(
    df: Any, lower_columns: dict[str, str]
) -> str | None:
    named_candidates = [
        column
        for column, lower in lower_columns.items()
        if "margin" in lower and ("debt" in lower or "balance" in lower)
    ]
    if named_candidates:
        return named_candidates[0]

    numeric_candidates: list[str] = []
    for column in df.columns:
        values = (
            df[column]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("$", "", regex=False)
            .str.replace(" ", "", regex=False)
        )
        numeric = pd.to_numeric(values, errors="coerce")
        if numeric.notna().sum() >= 2:
            numeric_candidates.append(column)

    return numeric_candidates[-1] if numeric_candidates else None
