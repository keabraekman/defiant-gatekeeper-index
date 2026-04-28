"""Transparent scoring logic for the Defiant Gatekeeper Index.

The functions in this module are intentionally small and side-effect free so
the dashboard rules can be audited, tested, and adjusted without touching data
fetching or the frontend.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any


DG_WEIGHTS = {
    "fed_liquidity": 0.25,
    "vix": 0.15,
    "margin_deleveraging": 0.15,
    "credit_health": 0.15,
    "inflation_room": 0.15,
    "labor_economy": 0.10,
    "sector_leadership": 0.05,
}


def clamp(value: float, minimum: float = 0, maximum: float = 100) -> float:
    return max(minimum, min(maximum, value))


def calculate_dg_index(components: dict[str, float]) -> int:
    total = 0.0
    for key, weight in DG_WEIGHTS.items():
        total += clamp(float(components.get(key, 50))) * weight
    return round(clamp(total))


def calculate_vix_score(vix: float | None) -> int:
    if vix is None:
        return 50
    if vix >= 35:
        return 100
    if vix >= 25:
        return 85
    if vix >= 18:
        return 60
    if vix >= 12:
        return 35
    return 20


def calculate_fed_liquidity_score(
    rate_change_3m: float | None, balance_sheet_pct_13w: float | None
) -> int:
    if rate_change_3m is None or balance_sheet_pct_13w is None:
        return 50

    rates_flat_or_falling = rate_change_3m <= 0
    balance_sheet_expanding = balance_sheet_pct_13w >= 0

    if rates_flat_or_falling and balance_sheet_expanding:
        return 100
    if rates_flat_or_falling and not balance_sheet_expanding:
        return 65
    if not rates_flat_or_falling and balance_sheet_expanding:
        return 50
    return 20


def calculate_margin_deleveraging_score(
    margin_debt_change_6m_pct: float | None,
) -> int:
    if margin_debt_change_6m_pct is None:
        return 50
    if margin_debt_change_6m_pct <= -10:
        return 100
    if margin_debt_change_6m_pct <= 0:
        return 75
    if margin_debt_change_6m_pct <= 10:
        return 45
    return 20


def calculate_credit_health_score(
    high_yield_spread: float | None, spread_change_3m: float | None
) -> int:
    if high_yield_spread is None:
        return 50

    if high_yield_spread < 4 and (spread_change_3m is None or spread_change_3m <= 0):
        score = 85
    elif high_yield_spread < 6:
        score = 60
    elif high_yield_spread < 8:
        score = 35
    else:
        score = 15

    if spread_change_3m is not None and spread_change_3m >= 1:
        score -= 20

    return round(clamp(score))


def calculate_inflation_room_score(
    cpi_yoy: float | None,
    core_cpi_yoy: float | None,
    cpi_yoy_change: float | None,
    core_cpi_yoy_change: float | None,
    ppi_change: float | None,
) -> int:
    if cpi_yoy is None or core_cpi_yoy is None:
        return 50

    cpi_falling = cpi_yoy_change is not None and cpi_yoy_change <= 0
    core_falling = core_cpi_yoy_change is not None and core_cpi_yoy_change <= 0
    inflation_rising = (cpi_yoy_change or 0) > 0 or (core_cpi_yoy_change or 0) > 0

    if cpi_yoy > 5 and inflation_rising:
        score = 10
    elif cpi_yoy > 4 or inflation_rising:
        score = 35
    elif cpi_falling and core_falling and cpi_yoy < 3:
        score = 90
    elif cpi_falling and core_falling and cpi_yoy <= 4:
        score = 70
    else:
        score = 50

    if ppi_change is not None:
        score += 5 if ppi_change <= 0 else -5

    return round(clamp(score))


def calculate_labor_economy_score(
    unemployment_change_3m: float | None,
    claims_change_3m_pct: float | None,
    payrolls_change_1m: float | None,
    inflation_rising: bool = False,
) -> int:
    if (
        unemployment_change_3m is None
        or claims_change_3m_pct is None
        or payrolls_change_1m is None
    ):
        return 50

    labor_weakening_fast = (
        unemployment_change_3m >= 0.4
        or claims_change_3m_pct >= 15
        or payrolls_change_1m < 0
    )
    labor_weakening_mildly = unemployment_change_3m > 0.2 or claims_change_3m_pct > 8
    economy_too_hot = (
        inflation_rising
        and unemployment_change_3m <= 0
        and claims_change_3m_pct <= 0
        and payrolls_change_1m >= 250
    )

    if labor_weakening_fast:
        return 20
    if economy_too_hot:
        return 40
    if labor_weakening_mildly:
        return 50
    if payrolls_change_1m > 0:
        return 75
    return 50


def calculate_sector_leadership_score(relative_performance: dict[str, float] | None) -> int:
    if not relative_performance:
        return 50

    growth_symbols = ["QQQ", "SMH", "XLK"]
    growth_values = [relative_performance.get(symbol) for symbol in growth_symbols]
    growth_values = [value for value in growth_values if value is not None]
    all_risk_values = [
        relative_performance.get(symbol) for symbol in ["QQQ", "SMH", "XLK", "IWM"]
    ]
    all_risk_values = [value for value in all_risk_values if value is not None]

    if len([value for value in growth_values if value > 5]) >= 2:
        return 85
    if any(value > 0 for value in all_risk_values):
        return 60
    if all_risk_values and all(value <= -5 for value in all_risk_values):
        return 25
    return 40


def map_regime_label(score: float) -> str:
    if score < 30:
        return "Bearish / Defensive"
    if score < 45:
        return "Cautious"
    if score < 60:
        return "Neutral"
    if score < 75:
        return "Bullish"
    if score < 90:
        return "Risk-On"
    return "Panic-Buy Setup"


def calculate_confidence_score(
    missing_major_inputs: int = 0,
    finra_missing_or_stale: bool = False,
    etf_data_failed: bool = False,
    fred_series_stale: bool = False,
) -> dict[str, float | str]:
    score = 5.0
    score -= 0.5 * missing_major_inputs
    if finra_missing_or_stale:
        score -= 0.5
    if etf_data_failed:
        score -= 0.5
    if fred_series_stale:
        score -= 0.5

    score = max(1.0, round(score, 1))

    if score <= 2.0:
        label = "Low"
    elif score <= 3.5:
        label = "Medium"
    else:
        label = "High"

    return {"score": score, "label": label}


def determine_fed_liquidity_quadrant(
    rate_change_3m: float | None, balance_sheet_pct_13w: float | None
) -> dict[str, str]:
    if rate_change_3m is None or balance_sheet_pct_13w is None:
        return {
            "label": "Unknown / Neutral",
            "rate_trend": "unknown",
            "balance_sheet_trend": "unknown",
        }

    rates_flat_or_falling = rate_change_3m <= 0
    balance_sheet_expanding = balance_sheet_pct_13w >= 0
    rate_trend = "flat_or_falling" if rates_flat_or_falling else "rising"
    balance_sheet_trend = "expanding" if balance_sheet_expanding else "shrinking"

    if rates_flat_or_falling and balance_sheet_expanding:
        label = "Very Bullish Liquidity"
    elif rates_flat_or_falling and not balance_sheet_expanding:
        label = "Mixed / Improving"
    elif not rates_flat_or_falling and balance_sheet_expanding:
        label = "Mixed / Confused"
    else:
        label = "Bearish / Tight Liquidity"

    return {
        "label": label,
        "rate_trend": rate_trend,
        "balance_sheet_trend": balance_sheet_trend,
    }


def calculate_bull_trap_risk_score(
    high_yield_spread: float | None,
    spread_change_3m: float | None,
    cpi_yoy: float | None,
    cpi_yoy_change: float | None,
    core_cpi_yoy_change: float | None,
    fed_liquidity_score: float,
    labor_economy_score: float,
    oil_change_3m_pct: float | None,
) -> int:
    credit_risk = 50
    if high_yield_spread is not None:
        if high_yield_spread >= 8 or (spread_change_3m is not None and spread_change_3m >= 1):
            credit_risk = 100
        elif high_yield_spread >= 6 or (spread_change_3m is not None and spread_change_3m > 0):
            credit_risk = 65
        elif high_yield_spread >= 4:
            credit_risk = 35
        else:
            credit_risk = 15

    inflation_high_rising = (
        cpi_yoy is not None
        and cpi_yoy >= 4
        and ((cpi_yoy_change or 0) > 0 or (core_cpi_yoy_change or 0) > 0)
    )
    inflation_risk = 100 if inflation_high_rising else 25
    fed_tight_risk = 100 - clamp(fed_liquidity_score)
    labor_risk = 100 if labor_economy_score <= 20 else 50 if labor_economy_score <= 50 else 20
    oil_risk = 100 if oil_change_3m_pct is not None and oil_change_3m_pct >= 15 else 20

    score = (
        0.35 * credit_risk
        + 0.25 * inflation_risk
        + 0.20 * fed_tight_risk
        + 0.10 * labor_risk
        + 0.10 * oil_risk
    )
    return round(clamp(score))


def map_bull_trap_risk_label(score: float) -> str:
    if score < 35:
        return "Low"
    if score < 65:
        return "Medium"
    return "High"


def calculate_exit_warning_score(
    rate_change_3m: float | None,
    balance_sheet_pct_13w: float | None,
    spread_change_3m: float | None,
    cpi_yoy_change: float | None,
    core_cpi_yoy_change: float | None,
    qqq_relative_strength: float | None,
    smh_relative_strength: float | None,
    margin_debt_change_6m_pct: float | None,
) -> int:
    rates_rising = 100 if rate_change_3m is not None and rate_change_3m > 0 else 0
    balance_sheet_shrinking = 0
    if balance_sheet_pct_13w is not None:
        balance_sheet_shrinking = 100 if balance_sheet_pct_13w <= -2 else 50 if balance_sheet_pct_13w < 0 else 0

    credit_widening = 0
    if spread_change_3m is not None:
        credit_widening = 100 if spread_change_3m >= 1 else 50 if spread_change_3m > 0 else 0

    inflation_reaccelerating = (
        100 if (cpi_yoy_change is not None and cpi_yoy_change > 0)
        or (core_cpi_yoy_change is not None and core_cpi_yoy_change > 0)
        else 0
    )
    leadership_breaking = (
        100
        if (qqq_relative_strength is not None and qqq_relative_strength < -2)
        or (smh_relative_strength is not None and smh_relative_strength < -2)
        else 0
    )
    margin_releveraging = (
        100
        if margin_debt_change_6m_pct is not None and margin_debt_change_6m_pct > 10
        else 0
    )

    score = (
        0.30 * rates_rising
        + 0.20 * balance_sheet_shrinking
        + 0.20 * credit_widening
        + 0.15 * inflation_reaccelerating
        + 0.10 * leadership_breaking
        + 0.05 * margin_releveraging
    )
    return round(clamp(score))


def map_exit_warning_label(score: float) -> str:
    if score < 40:
        return "No Exit Warning"
    if score < 60:
        return "Watch Closely"
    return "Exit Warning Triggered"


def determine_asset_tilt(
    dg_index: float,
    bull_trap_risk_label: str,
    vix: float | None,
    credit_spreads_stable: bool,
    sector_leadership_score: float,
) -> dict[str, Any]:
    notes: list[str] = []
    growth_leading = sector_leadership_score >= 60

    if dg_index < 35:
        label = "Defensive / Cash / T-bills"
    elif dg_index < 55:
        label = "SPY/VOO DCA only"
    elif dg_index < 75:
        label = "SPY core + selective QQQ/XLK if leading" if growth_leading else "SPY core only"
    elif dg_index < 85:
        label = "Risk-on growth tilt: QQQ/SMH/XLK"
    elif vix is not None and vix > 30 and credit_spreads_stable:
        label = "Aggressive panic setup; small leverage sleeve possible"
    else:
        label = "Risk-on growth tilt: QQQ/SMH/XLK"

    if bull_trap_risk_label == "High":
        warning = "Avoid leverage / do not chase."
        label = f"{label}. {warning}"
        notes.append(warning)

    return {"label": label, "notes": notes}


def parse_date(value: str | date | datetime | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return datetime.fromisoformat(value[:10]).date()
    except (TypeError, ValueError):
        return None


def clean_history(history: list[dict[str, Any]] | None, value_key: str = "value") -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    for item in history or []:
        item_date = parse_date(item.get("date"))
        if item_date is None:
            continue
        try:
            value = float(item.get(value_key))
        except (TypeError, ValueError):
            continue
        points.append({"date": item_date, "value": value})
    return sorted(points, key=lambda item: item["date"])


def latest_value(input_item: dict[str, Any] | None) -> float | None:
    if not input_item:
        return None
    try:
        return float(input_item.get("value"))
    except (TypeError, ValueError):
        return None


def point_on_or_before(history: list[dict[str, Any]], target: date) -> dict[str, Any] | None:
    candidates = [point for point in history if point["date"] <= target]
    return candidates[-1] if candidates else None


def absolute_change_over_days(input_item: dict[str, Any] | None, days: int) -> float | None:
    if not input_item:
        return None
    history = clean_history(input_item.get("history"))
    if len(history) < 2:
        return None
    latest = history[-1]
    prior = point_on_or_before(history, latest["date"] - timedelta(days=days))
    if not prior:
        return None
    return latest["value"] - prior["value"]


def percent_change_over_days(input_item: dict[str, Any] | None, days: int) -> float | None:
    if not input_item:
        return None
    history = clean_history(input_item.get("history"))
    if len(history) < 2:
        return None
    latest = history[-1]
    prior = point_on_or_before(history, latest["date"] - timedelta(days=days))
    if not prior or prior["value"] == 0:
        return None
    return ((latest["value"] / prior["value"]) - 1) * 100


def absolute_change_over_observations(
    input_item: dict[str, Any] | None, observations_back: int
) -> float | None:
    if not input_item:
        return None
    history = clean_history(input_item.get("history"))
    if len(history) <= observations_back:
        return None
    latest = history[-1]
    prior = history[-1 - observations_back]
    return latest["value"] - prior["value"]


def percent_change_over_observations(
    input_item: dict[str, Any] | None, observations_back: int
) -> float | None:
    if not input_item:
        return None
    history = clean_history(input_item.get("history"))
    if len(history) <= observations_back:
        return None
    latest = history[-1]
    prior = history[-1 - observations_back]
    if prior["value"] == 0:
        return None
    return ((latest["value"] / prior["value"]) - 1) * 100


def year_over_year_percent(input_item: dict[str, Any] | None) -> float | None:
    return percent_change_over_days(input_item, 365)


def yoy_change(input_item: dict[str, Any] | None) -> float | None:
    if not input_item:
        return None
    history = clean_history(input_item.get("history"))
    if len(history) < 3:
        return None
    latest = history[-1]
    latest_prior_year = point_on_or_before(history, latest["date"] - timedelta(days=365))
    previous = point_on_or_before(history, latest["date"] - timedelta(days=30))
    if latest_prior_year is None or previous is None:
        return None
    previous_prior_year = point_on_or_before(history, previous["date"] - timedelta(days=365))
    if previous_prior_year is None or latest_prior_year["value"] == 0 or previous_prior_year["value"] == 0:
        return None
    current_yoy = ((latest["value"] / latest_prior_year["value"]) - 1) * 100
    previous_yoy = ((previous["value"] / previous_prior_year["value"]) - 1) * 100
    return current_yoy - previous_yoy


def public_input(input_item: dict[str, Any] | None) -> dict[str, Any]:
    if not input_item:
        return {
            "value": None,
            "date": None,
            "source": "Unknown",
            "freshness": "missing",
        }
    return {
        "value": input_item.get("value"),
        "date": input_item.get("date"),
        "source": input_item.get("source", "Unknown"),
        "freshness": input_item.get("freshness", "unknown"),
    }


def build_driver_lists(
    components: dict[str, int],
    bull_trap_risk: dict[str, Any],
    exit_warning: dict[str, Any],
) -> tuple[list[str], list[str]]:
    component_names = {
        "fed_liquidity": "Fed liquidity",
        "vix": "VIX panic/opportunity",
        "margin_deleveraging": "Margin deleveraging",
        "credit_health": "Credit health",
        "inflation_room": "Inflation room",
        "labor_economy": "Labor/economy",
        "sector_leadership": "Sector leadership",
    }

    bullish = [
        f"{component_names[key]} supportive ({value})"
        for key, value in sorted(components.items(), key=lambda item: item[1], reverse=True)
        if value >= 70
    ]
    bearish = [
        f"{component_names[key]} pressured ({value})"
        for key, value in sorted(components.items(), key=lambda item: item[1])
        if value <= 40
    ]

    if bull_trap_risk["label"] == "High":
        bearish.append("Bull trap risk is high")
    elif bull_trap_risk["label"] == "Low":
        bullish.append("Bull trap risk is low")

    if exit_warning["label"] == "Exit Warning Triggered":
        bearish.append("Exit warning is triggered")
    elif exit_warning["label"] == "No Exit Warning":
        bullish.append("No exit warning is active")

    return bullish[:5], bearish[:5]


def calculate_dashboard(
    inputs: dict[str, dict[str, Any]],
    data_quality_issues: list[str] | None = None,
    quality_context: dict[str, Any] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    data_quality_issues = list(data_quality_issues or [])
    quality_context = quality_context or {}

    vix = latest_value(inputs.get("vix"))
    rate_change_3m = absolute_change_over_days(inputs.get("effective_fed_funds_rate"), 90)
    balance_sheet_pct_13w = percent_change_over_days(inputs.get("fed_balance_sheet"), 91)
    margin_debt_change_6m_pct = percent_change_over_observations(
        inputs.get("finra_margin_debt"), 6
    )
    if margin_debt_change_6m_pct is None:
        margin_debt_change_6m_pct = percent_change_over_days(inputs.get("finra_margin_debt"), 183)
    high_yield_spread = latest_value(inputs.get("high_yield_credit_spread"))
    spread_change_3m = absolute_change_over_days(inputs.get("high_yield_credit_spread"), 90)
    cpi_yoy = year_over_year_percent(inputs.get("cpi"))
    core_cpi_yoy = year_over_year_percent(inputs.get("core_cpi"))
    cpi_yoy_delta = yoy_change(inputs.get("cpi"))
    core_cpi_yoy_delta = yoy_change(inputs.get("core_cpi"))
    ppi_change_3m = percent_change_over_days(inputs.get("ppi"), 90)
    unemployment_change_3m = absolute_change_over_observations(
        inputs.get("unemployment_rate"), 3
    )
    if unemployment_change_3m is None:
        unemployment_change_3m = absolute_change_over_days(inputs.get("unemployment_rate"), 90)
    claims_change_3m_pct = percent_change_over_days(inputs.get("initial_jobless_claims"), 90)
    payrolls_change_1m = absolute_change_over_observations(inputs.get("nonfarm_payrolls"), 1)
    if payrolls_change_1m is None:
        payrolls_change_1m = absolute_change_over_days(inputs.get("nonfarm_payrolls"), 30)
    oil_change_3m_pct = percent_change_over_days(inputs.get("oil_price"), 90)
    etf_relative_strength = inputs.get("etf_relative_strength", {}).get("value")
    if not isinstance(etf_relative_strength, dict):
        etf_relative_strength = {}

    inflation_rising = (cpi_yoy_delta or 0) > 0 or (core_cpi_yoy_delta or 0) > 0

    components = {
        "fed_liquidity": calculate_fed_liquidity_score(rate_change_3m, balance_sheet_pct_13w),
        "vix": calculate_vix_score(vix),
        "margin_deleveraging": calculate_margin_deleveraging_score(margin_debt_change_6m_pct),
        "credit_health": calculate_credit_health_score(high_yield_spread, spread_change_3m),
        "inflation_room": calculate_inflation_room_score(
            cpi_yoy, core_cpi_yoy, cpi_yoy_delta, core_cpi_yoy_delta, ppi_change_3m
        ),
        "labor_economy": calculate_labor_economy_score(
            unemployment_change_3m,
            claims_change_3m_pct,
            payrolls_change_1m,
            inflation_rising=inflation_rising,
        ),
        "sector_leadership": calculate_sector_leadership_score(etf_relative_strength),
    }

    dg_score = calculate_dg_index(components)
    regime_label = map_regime_label(dg_score)
    fed_liquidity_quadrant = determine_fed_liquidity_quadrant(
        rate_change_3m, balance_sheet_pct_13w
    )

    bull_trap_score = calculate_bull_trap_risk_score(
        high_yield_spread,
        spread_change_3m,
        cpi_yoy,
        cpi_yoy_delta,
        core_cpi_yoy_delta,
        components["fed_liquidity"],
        components["labor_economy"],
        oil_change_3m_pct,
    )
    bull_trap_risk = {
        "score": bull_trap_score,
        "label": map_bull_trap_risk_label(bull_trap_score),
    }

    exit_score = calculate_exit_warning_score(
        rate_change_3m,
        balance_sheet_pct_13w,
        spread_change_3m,
        cpi_yoy_delta,
        core_cpi_yoy_delta,
        etf_relative_strength.get("QQQ"),
        etf_relative_strength.get("SMH"),
        margin_debt_change_6m_pct,
    )
    exit_warning = {
        "score": exit_score,
        "label": map_exit_warning_label(exit_score),
    }

    asset_tilt = determine_asset_tilt(
        dg_score,
        bull_trap_risk["label"],
        vix,
        credit_spreads_stable=spread_change_3m is not None and spread_change_3m <= 0,
        sector_leadership_score=components["sector_leadership"],
    )
    top_bullish_drivers, top_bearish_drivers = build_driver_lists(
        components, bull_trap_risk, exit_warning
    )

    public_inputs = {key: public_input(value) for key, value in inputs.items()}

    return {
        "generated_at": generated_at or datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "dg_index": {
            "score": dg_score,
            "label": regime_label,
            "components": components,
        },
        "regime_label": regime_label,
        "confidence_score": calculate_confidence_score(
            missing_major_inputs=int(quality_context.get("missing_major_inputs", 0)),
            finra_missing_or_stale=bool(quality_context.get("finra_missing_or_stale", False)),
            etf_data_failed=bool(quality_context.get("etf_data_failed", False)),
            fred_series_stale=bool(quality_context.get("fred_series_stale", False)),
        ),
        "fed_liquidity_quadrant": fed_liquidity_quadrant,
        "asset_tilt": asset_tilt,
        "bull_trap_risk": bull_trap_risk,
        "exit_warning": exit_warning,
        "top_bullish_drivers": top_bullish_drivers,
        "top_bearish_drivers": top_bearish_drivers,
        "inputs": public_inputs,
        "data_quality_issues": data_quality_issues,
        "disclaimer": "This dashboard is for educational purposes only and is not financial advice.",
    }
