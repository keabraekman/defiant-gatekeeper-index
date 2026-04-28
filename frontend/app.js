const SAMPLE_DASHBOARD = {
  generated_at: "2026-01-01T12:00:00Z",
  dg_index: {
    score: 67,
    label: "Bullish",
    components: {
      fed_liquidity: 65,
      vix: 60,
      margin_deleveraging: 75,
      credit_health: 60,
      inflation_room: 75,
      labor_economy: 75,
      sector_leadership: 60,
    },
  },
  regime_label: "Bullish",
  confidence_score: { score: 4.5, label: "High" },
  fed_liquidity_quadrant: {
    label: "Mixed / Improving",
    rate_trend: "flat_or_falling",
    balance_sheet_trend: "shrinking",
  },
  asset_tilt: {
    label: "SPY core + selective QQQ/XLK if leading",
    notes: [],
  },
  bull_trap_risk: { score: 30, label: "Low" },
  exit_warning: { score: 10, label: "No Exit Warning" },
  top_bullish_drivers: [
    "Margin deleveraging supportive (75)",
    "Inflation room supportive (75)",
    "Labor/economy supportive (75)",
    "Bull trap risk is low",
    "No exit warning is active",
  ],
  top_bearish_drivers: [],
  inputs: {
    vix: { value: 22.5, date: "2026-04-28", source: "FRED:VIXCLS", freshness: "mock" },
    effective_fed_funds_rate: {
      value: 5.25,
      date: "2026-04-28",
      source: "FRED:DFF",
      freshness: "mock",
    },
    fed_balance_sheet: {
      value: 7600,
      date: "2026-04-28",
      source: "FRED:WALCL",
      freshness: "mock",
    },
    finra_margin_debt: {
      value: 760,
      date: "2026-04-28",
      source: "FINRA:Margin Debt",
      freshness: "mock",
    },
    high_yield_credit_spread: {
      value: 4.8,
      date: "2026-04-28",
      source: "FRED:BAMLH0A0HYM2",
      freshness: "mock",
    },
    cpi: { value: 320, date: "2026-04-28", source: "FRED:CPIAUCSL", freshness: "mock" },
    core_cpi: { value: 325, date: "2026-04-28", source: "FRED:CPILFESL", freshness: "mock" },
    ppi: { value: 255, date: "2026-04-28", source: "FRED:PPIACO", freshness: "mock" },
    unemployment_rate: {
      value: 4,
      date: "2026-04-28",
      source: "FRED:UNRATE",
      freshness: "mock",
    },
    initial_jobless_claims: {
      value: 220000,
      date: "2026-04-28",
      source: "FRED:ICSA",
      freshness: "mock",
    },
    nonfarm_payrolls: {
      value: 158000,
      date: "2026-04-28",
      source: "FRED:PAYEMS",
      freshness: "mock",
    },
    oil_price: {
      value: 82,
      date: "2026-04-28",
      source: "FRED:DCOILWTICO",
      freshness: "mock",
    },
    etf_relative_strength: {
      value: { QQQ: 3.8, SMH: 6.4, XLK: 4.9, IWM: -1.2 },
      date: "2026-04-28",
      source: "Alpha Vantage:SPY,QQQ,SMH,XLK,IWM",
      freshness: "mock",
    },
  },
  data_quality_issues: ["Embedded sample data shown because dashboard JSON could not be loaded."],
  disclaimer: "This dashboard is for educational purposes only and is not financial advice.",
};

const DATA_PATHS = [
  "./data/dashboard.json",
  "../data/dashboard.json",
  "./data/dashboard.sample.json",
  "../data/dashboard.sample.json",
];

const COMPONENT_LABELS = {
  fed_liquidity: "Fed liquidity",
  vix: "VIX panic/opportunity",
  margin_deleveraging: "Margin deleveraging",
  credit_health: "Credit health",
  inflation_room: "Inflation room",
  labor_economy: "Labor/economy",
  sector_leadership: "Sector leadership",
};

async function loadDashboard() {
  if (window.location.protocol === "file:") {
    return SAMPLE_DASHBOARD;
  }

  for (const path of DATA_PATHS) {
    try {
      const response = await fetch(path, { cache: "no-store" });
      if (response.ok) {
        return await response.json();
      }
    } catch (error) {
      // Try the next static path.
    }
  }

  return SAMPLE_DASHBOARD;
}

function renderDashboard(data) {
  document.getElementById("generatedAt").textContent = formatDateTime(data.generated_at);
  document.getElementById("disclaimer").textContent =
    data.disclaimer || "This dashboard is for educational purposes only and is not financial advice.";

  renderSummaryCards(data);
  renderComponents(data.dg_index.components);
  renderDrivers("bullishDrivers", data.top_bullish_drivers);
  renderDrivers("bearishDrivers", data.top_bearish_drivers);
  renderInputs(data.inputs);
  renderQualityIssues(data.data_quality_issues);
}

function renderSummaryCards(data) {
  const cards = [
    {
      label: "DG Index",
      value: data.dg_index.score,
      detail: data.dg_index.label,
      tone: scoreTone(data.dg_index.score),
    },
    {
      label: "Regime Label",
      value: data.regime_label,
      detail: "Derived from DG Index",
      tone: "tone-info",
    },
    {
      label: "Confidence Score",
      value: `${data.confidence_score.score} / 5`,
      detail: data.confidence_score.label,
      tone: confidenceTone(data.confidence_score.score),
    },
    {
      label: "Fed Liquidity Quadrant",
      value: data.fed_liquidity_quadrant.label,
      detail: `Rates: ${humanize(data.fed_liquidity_quadrant.rate_trend)} | Balance sheet: ${humanize(
        data.fed_liquidity_quadrant.balance_sheet_trend,
      )}`,
      tone: liquidityTone(data.fed_liquidity_quadrant.label),
    },
    {
      label: "Asset Tilt",
      value: data.asset_tilt.label,
      detail: data.asset_tilt.notes?.join(" ") || "Broad posture only",
      tone: "tone-info",
      wide: true,
    },
    {
      label: "Bull Trap Risk",
      value: data.bull_trap_risk.label,
      detail: `${data.bull_trap_risk.score} / 100`,
      tone: riskTone(data.bull_trap_risk.score),
    },
    {
      label: "Exit Warning",
      value: data.exit_warning.label,
      detail: `${data.exit_warning.score} / 100`,
      tone: riskTone(data.exit_warning.score),
    },
  ];

  const container = document.getElementById("summaryCards");
  container.replaceChildren(...cards.map(createMetricCard));
}

function createMetricCard(card) {
  const element = document.createElement("article");
  element.className = `metric-card ${card.tone}${card.wide ? " wide" : ""}`;

  const label = document.createElement("div");
  label.className = "metric-label";
  label.textContent = card.label;

  const value = document.createElement("div");
  value.className = "metric-value";
  value.textContent = card.value;

  const detail = document.createElement("div");
  detail.className = "metric-detail";
  detail.textContent = card.detail;

  element.append(label, value, detail);
  return element;
}

function renderComponents(components) {
  const container = document.getElementById("componentScores");
  const rows = Object.entries(COMPONENT_LABELS).map(([key, label]) => {
    const value = Number(components[key] ?? 0);
    const row = document.createElement("div");
    row.className = "score-row";

    const name = document.createElement("div");
    name.className = "score-name";
    name.textContent = label;

    const track = document.createElement("div");
    track.className = "bar-track";
    const fill = document.createElement("div");
    fill.className = `bar-fill ${barTone(value)}`;
    fill.style.width = `${Math.max(0, Math.min(100, value))}%`;
    track.append(fill);

    const score = document.createElement("div");
    score.className = "score-value";
    score.textContent = value;

    row.append(name, track, score);
    return row;
  });
  container.replaceChildren(...rows);
}

function renderDrivers(id, drivers) {
  const list = document.getElementById(id);
  const items = drivers?.length ? drivers : ["No major driver flagged."];
  list.replaceChildren(
    ...items.map((text) => {
      const item = document.createElement("li");
      item.textContent = text;
      return item;
    }),
  );
}

function renderInputs(inputs) {
  const body = document.getElementById("inputTable");
  const rows = Object.entries(inputs || {}).map(([key, input]) => {
    const row = document.createElement("tr");
    row.append(
      tableCell(titleize(key)),
      tableCell(formatValue(input.value)),
      tableCell(input.date || "Missing"),
      tableCell(input.source || "Unknown"),
      freshnessCell(input.freshness || "unknown"),
    );
    return row;
  });
  body.replaceChildren(...rows);
}

function renderQualityIssues(issues) {
  const list = document.getElementById("qualityIssues");
  const items = issues?.length ? issues : ["No data-quality issues detected."];
  list.replaceChildren(
    ...items.map((text) => {
      const item = document.createElement("li");
      item.textContent = text;
      return item;
    }),
  );
}

function tableCell(value) {
  const cell = document.createElement("td");
  cell.textContent = value;
  return cell;
}

function freshnessCell(value) {
  const cell = document.createElement("td");
  const badge = document.createElement("span");
  badge.className = `freshness ${value}`;
  badge.textContent = value;
  cell.append(badge);
  return cell;
}

function formatValue(value) {
  if (value === null || value === undefined) {
    return "Missing";
  }
  if (typeof value === "number") {
    return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
  }
  if (typeof value === "object") {
    return Object.entries(value)
      .map(([key, item]) => `${key}: ${Number(item) >= 0 ? "+" : ""}${item} pp`)
      .join(", ");
  }
  return String(value);
}

function formatDateTime(value) {
  if (!value) {
    return "Unknown";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function scoreTone(score) {
  if (score >= 65) return "tone-good";
  if (score >= 40) return "tone-mid";
  return "tone-bad";
}

function confidenceTone(score) {
  if (score >= 3.5) return "tone-good";
  if (score >= 2) return "tone-mid";
  return "tone-bad";
}

function riskTone(score) {
  if (score >= 65) return "tone-bad";
  if (score >= 35) return "tone-mid";
  return "tone-good";
}

function liquidityTone(label) {
  if (label.includes("Bullish")) return "tone-good";
  if (label.includes("Bearish")) return "tone-bad";
  return "tone-mid";
}

function barTone(score) {
  if (score >= 70) return "high";
  if (score >= 40) return "medium";
  return "low";
}

function humanize(value) {
  return String(value || "unknown").replaceAll("_", " ");
}

function titleize(value) {
  return value
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

loadDashboard()
  .then(renderDashboard)
  .catch(() => renderDashboard(SAMPLE_DASHBOARD));
