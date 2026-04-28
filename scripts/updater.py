"""CLI updater for the Defiant Gatekeeper Index static JSON files."""

from __future__ import annotations

import argparse
import json
import os
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from calculations import calculate_dashboard, parse_date
from data_sources import (
    calculate_relative_strength,
    fetch_alpha_vantage_daily_adjusted,
    fetch_finra_margin_debt,
    fetch_fred_series,
)


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
HISTORY_DIR = DATA_DIR / "history"

FRED_SERIES = {
    "vix": ("VIXCLS", 10),
    "effective_fed_funds_rate": ("DFF", 10),
    "fed_balance_sheet": ("WALCL", 21),
    "high_yield_credit_spread": ("BAMLH0A0HYM2", 14),
    "cpi": ("CPIAUCSL", 60),
    "core_cpi": ("CPILFESL", 60),
    "ppi": ("PPIACO", 60),
    "unemployment_rate": ("UNRATE", 45),
    "initial_jobless_claims": ("ICSA", 14),
    "nonfarm_payrolls": ("PAYEMS", 45),
    "oil_price": ("DCOILWTICO", 10),
}

ETF_SYMBOLS = ["SPY", "QQQ", "SMH", "XLK", "IWM"]


def main() -> int:
    parser = argparse.ArgumentParser(description="Update static dashboard JSON.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--live", action="store_true", help="Fetch live data where credentials exist.")
    mode.add_argument("--mock", action="store_true", help="Use deterministic mock/sample data.")
    args = parser.parse_args()

    try:
        if args.live:
            dashboard = run_live_update()
        else:
            dashboard = run_mock_update()
    except Exception as exc:  # noqa: BLE001 - static deploys should survive bad data days.
        issue = f"Updater failed unexpectedly; deployed mock data instead: {exc}"
        dashboard = build_mock_dashboard(extra_issues=[issue], missing_major_inputs=1)

    write_dashboard_files(dashboard)
    print_summary(dashboard)
    return 0


def run_mock_update() -> dict[str, Any]:
    return build_mock_dashboard(
        extra_issues=["Mock data in use; configure repository secrets for live updates."],
        missing_major_inputs=1,
    )


def run_live_update() -> dict[str, Any]:
    inputs: dict[str, dict[str, Any]] = {}
    issues: list[str] = []
    quality_context = {
        "missing_major_inputs": 0,
        "finra_missing_or_stale": False,
        "etf_data_failed": False,
        "fred_series_stale": False,
    }

    mock_inputs = build_mock_inputs()

    fred_api_key = os.getenv("FRED_API_KEY")
    if not fred_api_key:
        issues.append("Missing FRED_API_KEY; using mock FRED values.")
        quality_context["missing_major_inputs"] += 1
        for logical_name in FRED_SERIES:
            inputs[logical_name] = with_freshness(mock_inputs[logical_name], "mock")
    else:
        for logical_name, (series_id, max_age_days) in FRED_SERIES.items():
            item = fetch_fred_series(series_id, fred_api_key)
            if item.get("error"):
                issues.append(f"{item['source']} unavailable; using neutral score where needed. {item['error']}")
                quality_context["missing_major_inputs"] += 1
            else:
                item["freshness"] = freshness_for_date(item.get("date"), max_age_days)
                if item["freshness"] == "stale":
                    issues.append(f"{item['source']} appears stale as of {item.get('date')}.")
                    quality_context["fred_series_stale"] = True
            inputs[logical_name] = item

    finra_url = os.getenv("FINRA_MARGIN_DEBT_URL")
    if not finra_url:
        issues.append("Missing FINRA_MARGIN_DEBT_URL; using mock FINRA margin-debt value.")
        quality_context["finra_missing_or_stale"] = True
        inputs["finra_margin_debt"] = with_freshness(mock_inputs["finra_margin_debt"], "mock")
    else:
        finra_item = fetch_finra_margin_debt(finra_url)
        if finra_item.get("error"):
            issues.append(
                f"FINRA margin debt unavailable; margin component uses neutral score. {finra_item['error']}"
            )
            quality_context["missing_major_inputs"] += 1
            quality_context["finra_missing_or_stale"] = True
        else:
            finra_item["freshness"] = freshness_for_date(finra_item.get("date"), 120)
            if finra_item["freshness"] == "stale":
                issues.append(f"FINRA margin debt appears stale as of {finra_item.get('date')}.")
                quality_context["finra_missing_or_stale"] = True
        inputs["finra_margin_debt"] = finra_item

    alpha_vantage_key = os.getenv("ALPHA_VANTAGE_API_KEY")
    if not alpha_vantage_key:
        issues.append("Missing ALPHA_VANTAGE_API_KEY; using mock ETF relative-strength values.")
        quality_context["etf_data_failed"] = True
        inputs["etf_relative_strength"] = with_freshness(mock_inputs["etf_relative_strength"], "mock")
    else:
        etf_histories: dict[str, dict[str, Any]] = {}
        etf_errors: list[str] = []
        for symbol in ETF_SYMBOLS:
            item = fetch_alpha_vantage_daily_adjusted(symbol, alpha_vantage_key)
            if item.get("error"):
                etf_errors.append(f"{symbol}: {item['error']}")
            else:
                etf_histories[symbol] = item

        if etf_errors or len(etf_histories) != len(ETF_SYMBOLS):
            issues.append(
                "ETF relative strength unavailable; sector leadership uses neutral score. "
                + " | ".join(etf_errors[:3])
            )
            quality_context["etf_data_failed"] = True
            inputs["etf_relative_strength"] = {
                "value": None,
                "date": None,
                "source": "Alpha Vantage:SPY,QQQ,SMH,XLK,IWM",
                "freshness": "missing",
                "history": [],
            }
        else:
            relative_strength = calculate_relative_strength(etf_histories)
            if relative_strength.get("error"):
                issues.append(
                    "ETF relative strength unavailable; sector leadership uses neutral score. "
                    + relative_strength["error"]
                )
                quality_context["etf_data_failed"] = True
            else:
                relative_strength["freshness"] = freshness_for_date(relative_strength.get("date"), 10)
            inputs["etf_relative_strength"] = relative_strength

    return calculate_dashboard(
        inputs=inputs,
        data_quality_issues=issues,
        quality_context=quality_context,
        generated_at=utc_timestamp(),
    )


def build_mock_dashboard(
    extra_issues: list[str] | None = None,
    missing_major_inputs: int = 0,
) -> dict[str, Any]:
    return calculate_dashboard(
        inputs=build_mock_inputs(),
        data_quality_issues=extra_issues or [],
        quality_context={"missing_major_inputs": missing_major_inputs},
        generated_at=utc_timestamp(),
    )


def build_mock_inputs() -> dict[str, dict[str, Any]]:
    today = datetime.now(timezone.utc).date()

    return {
        "vix": series_input(
            "FRED:VIXCLS",
            [(7, 19.0), (3, 21.2), (0, 22.5)],
            today,
        ),
        "effective_fed_funds_rate": series_input(
            "FRED:DFF",
            [(120, 5.33), (90, 5.33), (0, 5.25)],
            today,
        ),
        "fed_balance_sheet": series_input(
            "FRED:WALCL",
            [(120, 7700.0), (91, 7700.0), (0, 7600.0)],
            today,
        ),
        "finra_margin_debt": series_input(
            "FINRA:Margin Debt",
            [(220, 800.0), (183, 800.0), (0, 760.0)],
            today,
        ),
        "high_yield_credit_spread": series_input(
            "FRED:BAMLH0A0HYM2",
            [(120, 4.9), (90, 4.9), (0, 4.8)],
            today,
        ),
        "cpi": series_input(
            "FRED:CPIAUCSL",
            [(395, 307.0), (365, 310.0), (30, 319.0), (0, 320.0)],
            today,
        ),
        "core_cpi": series_input(
            "FRED:CPILFESL",
            [(395, 312.9), (365, 314.0), (30, 324.8), (0, 325.0)],
            today,
        ),
        "ppi": series_input(
            "FRED:PPIACO",
            [(120, 258.0), (90, 257.0), (0, 255.0)],
            today,
        ),
        "unemployment_rate": series_input(
            "FRED:UNRATE",
            [(120, 3.9), (90, 3.95), (0, 4.0)],
            today,
        ),
        "initial_jobless_claims": series_input(
            "FRED:ICSA",
            [(120, 217000.0), (90, 218000.0), (0, 220000.0)],
            today,
        ),
        "nonfarm_payrolls": series_input(
            "FRED:PAYEMS",
            [(60, 157600.0), (30, 157850.0), (0, 158000.0)],
            today,
        ),
        "oil_price": series_input(
            "FRED:DCOILWTICO",
            [(120, 78.0), (90, 80.0), (0, 82.0)],
            today,
        ),
        "etf_relative_strength": {
            "value": {"QQQ": 3.8, "SMH": 6.4, "XLK": 4.9, "IWM": -1.2},
            "date": today.isoformat(),
            "source": "Alpha Vantage:SPY,QQQ,SMH,XLK,IWM",
            "freshness": "mock",
            "history": [],
        },
    }


def series_input(
    source: str,
    day_offsets_and_values: list[tuple[int, float]],
    today: date,
) -> dict[str, Any]:
    history = [
        {"date": (today - timedelta(days=days_ago)).isoformat(), "value": value}
        for days_ago, value in day_offsets_and_values
    ]
    history.sort(key=lambda item: item["date"])
    latest = history[-1]
    return {
        "value": latest["value"],
        "date": latest["date"],
        "source": source,
        "freshness": "mock",
        "history": history,
    }


def with_freshness(item: dict[str, Any], freshness: str) -> dict[str, Any]:
    copied = deepcopy(item)
    copied["freshness"] = freshness
    return copied


def freshness_for_date(value: str | None, max_age_days: int) -> str:
    observed = parse_date(value)
    if observed is None:
        return "missing"
    age = (datetime.now(timezone.utc).date() - observed).days
    return "fresh" if age <= max_age_days else "stale"


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_dashboard_files(dashboard: dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    today = datetime.now(timezone.utc).date().isoformat()
    payload = json.dumps(dashboard, indent=2, sort_keys=False) + "\n"
    (DATA_DIR / "dashboard.json").write_text(payload, encoding="utf-8")
    (HISTORY_DIR / f"{today}.json").write_text(payload, encoding="utf-8")


def print_summary(dashboard: dict[str, Any]) -> None:
    dg = dashboard["dg_index"]
    confidence = dashboard["confidence_score"]
    issues = len(dashboard.get("data_quality_issues", []))
    print(
        f"DG Index {dg['score']} ({dg['label']}); "
        f"confidence {confidence['score']} ({confidence['label']}); "
        f"data-quality issues: {issues}"
    )


if __name__ == "__main__":
    raise SystemExit(main())
