from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from calculations import (  # noqa: E402
    calculate_confidence_score,
    calculate_dg_index,
    calculate_fed_liquidity_score,
    calculate_vix_score,
    determine_fed_liquidity_quadrant,
    map_bull_trap_risk_label,
    map_exit_warning_label,
    map_regime_label,
)


def test_dg_index_weighted_average() -> None:
    components = {
        "fed_liquidity": 100,
        "vix": 80,
        "margin_deleveraging": 60,
        "credit_health": 40,
        "inflation_room": 20,
        "labor_economy": 50,
        "sector_leadership": 70,
    }

    assert calculate_dg_index(components) == 64


def test_regime_label_mapping() -> None:
    assert map_regime_label(20) == "Bearish / Defensive"
    assert map_regime_label(40) == "Cautious"
    assert map_regime_label(50) == "Neutral"
    assert map_regime_label(70) == "Bullish"
    assert map_regime_label(82) == "Risk-On"
    assert map_regime_label(95) == "Panic-Buy Setup"


def test_vix_score() -> None:
    assert calculate_vix_score(36) == 100
    assert calculate_vix_score(30) == 85
    assert calculate_vix_score(20) == 60
    assert calculate_vix_score(15) == 35
    assert calculate_vix_score(11) == 20
    assert calculate_vix_score(None) == 50


def test_fed_liquidity_score_and_quadrant() -> None:
    assert calculate_fed_liquidity_score(-0.25, 2.0) == 100
    assert calculate_fed_liquidity_score(-0.1, -1.0) == 65
    assert calculate_fed_liquidity_score(0.25, 1.0) == 50
    assert calculate_fed_liquidity_score(0.25, -1.0) == 20

    quadrant = determine_fed_liquidity_quadrant(-0.1, -1.0)
    assert quadrant["label"] == "Mixed / Improving"
    assert quadrant["rate_trend"] == "flat_or_falling"
    assert quadrant["balance_sheet_trend"] == "shrinking"


def test_bull_trap_risk_label() -> None:
    assert map_bull_trap_risk_label(10) == "Low"
    assert map_bull_trap_risk_label(50) == "Medium"
    assert map_bull_trap_risk_label(80) == "High"


def test_exit_warning_label() -> None:
    assert map_exit_warning_label(20) == "No Exit Warning"
    assert map_exit_warning_label(50) == "Watch Closely"
    assert map_exit_warning_label(70) == "Exit Warning Triggered"


def test_confidence_score_penalties() -> None:
    confidence = calculate_confidence_score(
        missing_major_inputs=2,
        finra_missing_or_stale=True,
        etf_data_failed=True,
        fred_series_stale=True,
    )

    assert confidence == {"score": 2.5, "label": "Medium"}
    assert calculate_confidence_score()["label"] == "High"
    assert calculate_confidence_score(missing_major_inputs=20)["score"] == 1.0
