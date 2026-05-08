from __future__ import annotations

import sys
from pathlib import Path

import pytest
import requests


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import data_sources  # noqa: E402


def test_fred_api_failure_uses_public_csv_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    fallback = {
        "value": 330.29,
        "date": "2026-03-01",
        "source": "FRED:CPIAUCSL",
        "freshness": "unknown",
        "history": [{"date": "2026-03-01", "value": 330.29}],
    }

    def fail_get(*_args: object, **_kwargs: object) -> None:
        raise requests.RequestException("temporary FRED failure")

    monkeypatch.setattr(requests, "get", fail_get)
    monkeypatch.setattr(data_sources, "fetch_fred_series_public", lambda _series_id: fallback)

    assert data_sources.fetch_fred_series("CPIAUCSL", "key") == fallback


def test_finra_parse_failure_uses_page_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    fallback = {
        "value": 820000.0,
        "date": "2026-03-31",
        "source": "FINRA:Margin Statistics page",
        "freshness": "unknown",
        "history": [{"date": "2026-03-31", "value": 820000.0}],
    }

    class Response:
        content = b"<html>not a csv</html>"

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr(requests, "get", lambda *_args, **_kwargs: Response())
    monkeypatch.setattr(data_sources, "_read_finra_frames", lambda *_args: (_ for _ in ()).throw(ValueError("bad parse")))
    monkeypatch.setattr(data_sources, "fetch_finra_margin_debt_page", lambda: fallback)

    assert data_sources.fetch_finra_margin_debt("https://example.com/margin.csv") == fallback


def test_etf_prices_prefer_yahoo(monkeypatch: pytest.MonkeyPatch) -> None:
    yahoo = {
        "value": 500.0,
        "date": "2026-05-06",
        "source": "Yahoo Finance:SPY",
        "freshness": "unknown",
        "history": [{"date": "2026-05-06", "value": 500.0}],
    }

    monkeypatch.setattr(data_sources, "fetch_yahoo_daily_adjusted", lambda _symbol: yahoo)
    monkeypatch.setattr(
        data_sources,
        "fetch_alpha_vantage_daily_adjusted",
        lambda *_args: pytest.fail("Alpha Vantage should not be called when Yahoo succeeds"),
    )

    assert data_sources.fetch_etf_daily_adjusted("SPY", "key") == yahoo
