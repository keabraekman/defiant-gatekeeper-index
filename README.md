# Defiant Gatekeeper Index

Defiant Gatekeeper Index is a deployable static macro-investing dashboard. It calculates a 0 to 100 risk-on/risk-off score from market volatility, Fed liquidity, credit spreads, inflation, labor data, margin debt, oil, and ETF leadership.

This dashboard is for educational purposes only and is not financial advice.

## Architecture

The project has no AWS, no live backend API, no database, and no authentication.

```text
GitHub Actions schedule
-> Python updater fetches data
-> Python calculations produce static JSON
-> workflow copies frontend and data into dist/
-> GitHub Pages deploys dist/
-> browser reads ./data/dashboard.json
```

The frontend is plain HTML, CSS, and JavaScript. The build artifact contains only static files.

## Dashboard Outputs

- `DG Index`: weighted 0 to 100 macro risk score.
- `Regime Label`: readable label derived from the DG Index.
- `Confidence Score`: 1 to 5 score based on freshness and completeness.
- `Fed Liquidity Quadrant`: rate trend plus Fed balance sheet trend.
- `Asset Tilt`: broad posture, not exact financial advice.
- `Bull Trap Risk`: Low, Medium, or High risk that a rally is fake.
- `Exit Warning`: No Exit Warning, Watch Closely, or Exit Warning Triggered.
- Drivers, data-quality warnings, input table, and component score breakdown.

## Input Sources

FRED series:

- VIX: `VIXCLS`
- Effective Fed Funds Rate: `DFF`
- Fed Balance Sheet: `WALCL`
- High-Yield Credit Spread: `BAMLH0A0HYM2`
- CPI: `CPIAUCSL`
- Core CPI: `CPILFESL`
- PPI: `PPIACO`
- Unemployment Rate: `UNRATE`
- Initial Jobless Claims: `ICSA`
- Nonfarm Payrolls: `PAYEMS`
- Oil Price: `DCOILWTICO`

Other sources:

- FINRA margin debt from `FINRA_MARGIN_DEBT_URL`
- ETF adjusted closes from Alpha Vantage for `SPY`, `QQQ`, `SMH`, `XLK`, and `IWM`

If a source fails, the updater records a data-quality issue and uses neutral scoring for the affected component. Missing credentials in live mode fall back to mock values so the site still deploys.

## Local Development

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r scripts/requirements.txt
pytest
python scripts/updater.py --mock
python -m http.server 8000
```

Then open:

```text
http://localhost:8000/frontend/
```

The frontend also has embedded sample data, so `frontend/index.html` can be opened directly from disk.

## Required GitHub Secrets

Set these repository secrets before expecting live updates:

- `FRED_API_KEY`
- `ALPHA_VANTAGE_API_KEY`
- `FINRA_MARGIN_DEBT_URL`

## GitHub Pages Deployment

Enable GitHub Pages with GitHub Actions as the source. The workflow at `.github/workflows/deploy-pages.yml` runs on:

- pushes to `main`
- manual `workflow_dispatch`
- daily schedule at 12:00 UTC

The workflow installs Python dependencies, runs tests, runs `python scripts/updater.py --live`, builds `dist/`, uploads the Pages artifact, and deploys with the official GitHub Pages actions.

Generated `dashboard.json` files are not committed back from GitHub Actions. They are included only in the Pages artifact.

## Manual Updater Commands

Use mock/sample data:

```bash
python scripts/updater.py --mock
```

Use live data where secrets are available:

```bash
export FRED_API_KEY="..."
export ALPHA_VANTAGE_API_KEY="..."
export FINRA_MARGIN_DEBT_URL="..."
python scripts/updater.py --live
```

Both modes write:

- `data/dashboard.json`
- `data/history/YYYY-MM-DD.json`

## Mock vs Live Data

`--mock` uses deterministic sample values and adds a data-quality warning.

`--live` reads environment variables, fetches real data, and only falls back when a configured source is missing or unavailable. The calculation rules live in `scripts/calculations.py` and are intentionally simple to modify.

## Disclaimer

This dashboard is for educational purposes only and is not financial advice.
