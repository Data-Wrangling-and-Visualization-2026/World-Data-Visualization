const express = require("express");
const cors = require("cors");
const sqlite3 = require("sqlite3").verbose();
const path = require("path");

const app = express();
const PORT = 8000;

app.use(cors());
app.use(express.json());

const dbPath = path.join(__dirname, "../../database/countries_stats.db");
const db = new sqlite3.Database(dbPath, (err) => {
  if (err) {
    console.error("DB connection error:", err.message);
  } else {
    console.log("Connected to SQLite:", dbPath);
  }
});

const METRICS = [
  "Population",
  "Yearly Change",
  "Yearly % Change",
  "Birth",
  "Death",
  "Fossil CO2 emissions (tons)",
  "CO2 emissions change",
  "CO2 emissions per capita",
  "Median Age",
  "Fertility Rate",
  "Urban Pop %",
  "Urban Population",
  "Density (P/KmÂ²)",
  "Migrants (net)",
  "Country's Share of World Pop",
  "Share of World's CO2 emissions",
];

app.get("/", (req, res) => {
  res.json({
    message: "DWaV API is running",
    endpoints: ["/years", "/metrics", "/choropleth/:metric/:year", "/country-data/:country/:year"]
  });
});

app.get("/years", (req, res) => {
  const sql = `
    SELECT DISTINCT Year
    FROM country
    WHERE Year IS NOT NULL
    ORDER BY Year ASC
  `;

  db.all(sql, [], (err, rows) => {
    if (err) {
      console.error(err.message);
      return res.status(500).json({ error: "Failed to load years" });
    }
    res.json(rows.map((r) => String(r.Year)));
  });
});

app.get("/metrics", (req, res) => {
  res.json(METRICS);
});

app.get("/country-series/:country", (req, res) => {
  const country = req.params.country.trim();

  // Build a safe SELECT list for metrics with spaces/symbols.
  const metricSelect = METRICS.map((m) => `"${m}"`).join(", ");
  const sql = `
    SELECT Year, Country, ${metricSelect}
    FROM country
    WHERE LOWER(Country) = LOWER(?)
    AND Year IS NOT NULL
    ORDER BY Year ASC
  `;

  db.all(sql, [country], (err, rows) => {
    if (err) {
      console.error(err.message);
      return res.status(500).json({ error: "Failed to load country series" });
    }
    if (!rows || rows.length === 0) {
      return res.status(404).json({ error: "No data found" });
    }
    res.json(rows);
  });
});

app.get("/choropleth/:metric/:year", (req, res) => {
  const metric = req.params.metric;
  const year = req.params.year;

  if (!METRICS.includes(metric)) {
    return res.status(400).json({ error: "Unsupported metric" });
  }

  const sql = `
    SELECT Country, "${metric}" as value
    FROM country
    WHERE Year = ?
      AND Country IS NOT NULL
      AND "${metric}" IS NOT NULL
  `;

  db.all(sql, [year], (err, rows) => {
    if (err) {
      console.error(err.message);
      return res.status(500).json({ error: "Failed to load choropleth data" });
    }

    const numericRows = rows
      .map((r) => ({
        country: r.Country,
        value: Number(r.value)
      }))
      .filter((r) => Number.isFinite(r.value));

    if (!numericRows.length) {
      return res.json({
        metric,
        year,
        min: null,
        max: null,
        items: []
      });
    }

    const values = numericRows.map((r) => r.value);
    const min = Math.min(...values);
    const max = Math.max(...values);

    res.json({
      metric,
      year,
      min,
      max,
      items: numericRows
    });
  });
});

app.get("/country-data/:country/:year", (req, res) => {
  const country = req.params.country.trim();
  const year = req.params.year;

  const sql = `
    SELECT *
    FROM country
    WHERE LOWER(Country) = LOWER(?) AND Year = ?
    LIMIT 1
  `;

  db.get(sql, [country, year], (err, row) => {
    if (err) {
      console.error(err.message);
      return res.status(500).json({ error: "Failed to load country data" });
    }

    if (!row) {
      return res.status(404).json({ error: "No data found" });
    }

    res.json(row);
  });
});

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});