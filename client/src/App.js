import React, { useEffect, useMemo, useRef, useState } from "react";
import Globe from "react-globe.gl";
import * as d3 from "d3";

const COLOR_SCALE = [
  "#1a9850",
  "#66bd63",
  "#a6d96a",
  "#fee08b",
  "#fdae61",
  "#f46d43",
  "#d73027"
];

const MIN_YEAR = 1977;
const MAX_YEAR = 2022;
const DEFAULT_YEAR = "2020";
const DEFAULT_METRIC = "Population";

/** Must match `METRICS` in `server/src/index.js` (choropleth whitelist). */
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
  "Share of World's CO2 emissions"
];

const ROSE_METRICS = [
  "Country's Share of World Pop",
  "Share of World's CO2 emissions",
  "Urban Pop %",
  "Fertility Rate",
  "Density (P/KmÂ²)"
];

const STREAM_METRICS = ["Birth", "Death", "Migrants (net)", "Yearly Change"];

function pearson(xs, ys) {
  const n = Math.min(xs.length, ys.length);
  if (n < 2) return 0;
  let sx = 0,
    sy = 0,
    sxx = 0,
    syy = 0,
    sxy = 0;
  for (let i = 0; i < n; i += 1) {
    const x = xs[i];
    const y = ys[i];
    sx += x;
    sy += y;
    sxx += x * x;
    syy += y * y;
    sxy += x * y;
  }
  const cov = sxy - (sx * sy) / n;
  const vx = sxx - (sx * sx) / n;
  const vy = syy - (sy * sy) / n;
  const denom = Math.sqrt(vx * vy);
  if (!Number.isFinite(denom) || denom === 0) return 0;
  return cov / denom;
}

function clusterOrder(dist) {
  // Very small average-link agglomerative clustering producing a leaf order.
  // dist is NxN symmetric with zeros on diagonal.
  const n = dist.length;
  let clusters = Array.from({ length: n }, (_, i) => [i]);

  const avgDist = (a, b) => {
    let sum = 0;
    let cnt = 0;
    a.forEach((i) => {
      b.forEach((j) => {
        sum += dist[i][j];
        cnt += 1;
      });
    });
    return cnt ? sum / cnt : 0;
  };

  while (clusters.length > 1) {
    let bestI = 0;
    let bestJ = 1;
    let bestD = avgDist(clusters[0], clusters[1]);
    for (let i = 0; i < clusters.length; i += 1) {
      for (let j = i + 1; j < clusters.length; j += 1) {
        const d = avgDist(clusters[i], clusters[j]);
        if (d < bestD) {
          bestD = d;
          bestI = i;
          bestJ = j;
        }
      }
    }
    const merged = clusters[bestI].concat(clusters[bestJ]);
    clusters = clusters.filter((_, idx) => idx !== bestI && idx !== bestJ);
    clusters.push(merged);
  }
  return clusters[0];
}

function useD3(render, deps) {
  const ref = useRef(null);
  useEffect(() => {
    if (!ref.current) return;
    render(d3.select(ref.current));
  }, deps);
  return ref;
}

function getCountryName(feature) {
  return (
    feature?.properties?.ADMIN ||
    feature?.properties?.NAME ||
    feature?.properties?.name ||
    feature?.properties?.COUNTRY ||
    "Unknown"
  );
}

function extractCoords(geometry) {
  if (!geometry) return [];
  if (geometry.type === "Polygon") return geometry.coordinates.flat(1);
  if (geometry.type === "MultiPolygon") return geometry.coordinates.flat(2);
  return [];
}

function getFeatureCenter(feature) {
  const coords = extractCoords(feature.geometry);
  if (!coords.length) return { lat: 20, lng: 0 };

  let sumLng = 0;
  let sumLat = 0;

  coords.forEach(([lng, lat]) => {
    sumLng += lng;
    sumLat += lat;
  });

  return {
    lng: sumLng / coords.length,
    lat: sumLat / coords.length
  };
}

function normalizeCountryName(name) {
  if (!name) return "";

  const aliases = {
    "united states of america": "united states",
    "russian federation": "russia",
    "czechia": "czech republic",
    "democratic republic of the congo": "dr congo",
    "dem rep congo": "dr congo",
    "republic of the congo": "congo",
    "united republic of tanzania": "tanzania",
    "viet nam": "vietnam",
    "syrian arab republic": "syria",
    "lao pdr": "laos",
    "korea republic of": "south korea",
    "korea democratic peoples republic of": "north korea",
    "ivory coast": "cote d'ivoire",
    "cote d ivoire": "cote d'ivoire",
    "cote divoire": "cote d'ivoire",
    "eswatini": "swaziland",
    "macedonia": "north macedonia",
    "bosnia and herz": "bosnia and herzegovina",
    "timor leste": "timor-leste",
    "bolivia plurinational state of": "bolivia",
    "venezuela bolivarian republic of": "venezuela",
    "peoples republic of china": "china",
    "people s republic of china": "china"
  };

  const cleaned = name
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/&/g, "and")
    .replace(/[^a-z0-9' -]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  return aliases[cleaned] || cleaned;
}

function getBucketIndex(value, min, max) {
  if (value === null || value === undefined || !Number.isFinite(value)) return -1;
  if (min === max) return 3;

  const step = (max - min) / 7;
  if (step === 0) return 3;

  const idx = Math.floor((value - min) / step);
  return Math.max(0, Math.min(6, idx));
}

export default function App() {
  const globeRef = useRef(null);

  const [countriesGeo, setCountriesGeo] = useState([]);
  const [selectedCountry, setSelectedCountry] = useState(null);
  const [hoveredCountry, setHoveredCountry] = useState(null);
  const [countryData, setCountryData] = useState(null);
  const [countrySeries, setCountrySeries] = useState(null);
  const [choroplethMap, setChoroplethMap] = useState({});
  const [rangeInfo, setRangeInfo] = useState({ min: null, max: null });
  const [apiItems, setApiItems] = useState([]);
  const [viewMode, setViewMode] = useState("split");

  const [selectedYear, setSelectedYear] = useState(DEFAULT_YEAR);
  const [selectedMetric, setSelectedMetric] = useState(DEFAULT_METRIC);

  useEffect(() => {
    fetch("/countries.geojson")
      .then((res) => res.json())
      .then((data) => setCountriesGeo(data.features || []))
      .catch((err) => console.error("GeoJSON load error:", err));
  }, []);

  useEffect(() => {
    fetch(
      `http://localhost:8000/choropleth/${encodeURIComponent(selectedMetric)}/${encodeURIComponent(selectedYear)}`
    )
      .then((res) => res.json())
      .then((data) => {
        const map = {};
        (data.items || []).forEach((item) => {
          map[normalizeCountryName(item.country)] = item.value;
        });

        setApiItems(data.items || []);
        setChoroplethMap(map);
        setRangeInfo({ min: data.min, max: data.max });
      })
      .catch((err) => console.error("Choropleth load error:", err));
  }, [selectedMetric, selectedYear]);

  useEffect(() => {
    if (!selectedCountry) {
      setCountryData(null);
      setCountrySeries(null);
      return;
    }

    fetch(
      `http://localhost:8000/country-data/${encodeURIComponent(
        selectedCountry
      )}/${encodeURIComponent(selectedYear)}`
    )
      .then((res) => res.json())
      .then((data) => setCountryData(data))
      .catch((err) => {
        console.error("Country data load error:", err);
        setCountryData(null);
      });
  }, [selectedCountry, selectedYear]);

  useEffect(() => {
    if (!selectedCountry) return;
    fetch(
      `http://localhost:8000/country-series/${encodeURIComponent(selectedCountry)}`
    )
      .then((res) => res.json())
      .then((rows) => setCountrySeries(Array.isArray(rows) ? rows : []))
      .catch((err) => {
        console.error("Country series load error:", err);
        setCountrySeries(null);
      });
  }, [selectedCountry]);

  useEffect(() => {
    if (!globeRef.current) return;

    globeRef.current.pointOfView({ lat: 20, lng: 0, altitude: 1.8 }, 1200);

    const controls = globeRef.current.controls();
    controls.autoRotate = true;
    controls.autoRotateSpeed = 0.35;
    controls.enableDamping = true;
    controls.dampingFactor = 0.08;
  }, [countriesGeo]);

  const legendItems = useMemo(() => {
    if (rangeInfo.min === null || rangeInfo.max === null) return [];

    const min = rangeInfo.min;
    const max = rangeInfo.max;
    const step = (max - min) / 7;

    return COLOR_SCALE.map((color, idx) => {
      const from = min + step * idx;
      const to = idx === 6 ? max : min + step * (idx + 1);

      return {
        color,
        label: `${from.toFixed(0).replace(/\B(?=(\d{3})+(?!\d))/g, ",")} – ${to.toFixed(0).replace(/\B(?=(\d{3})+(?!\d))/g, ",")}`
      };
    });
  }, [rangeInfo]);

  const metricEntries = countryData
    ? Object.entries(countryData).filter(([key]) => key !== "Country" && key !== "Year")
    : [];

  const matchedCount = useMemo(() => {
    let count = 0;
    countriesGeo.forEach((feature) => {
      const rawName = getCountryName(feature);
      const normalized = normalizeCountryName(rawName);
      if (Number.isFinite(choroplethMap[normalized])) count += 1;
    });
    return count;
  }, [countriesGeo, choroplethMap]);

  const handlePolygonHover = (feature) => {
    setHoveredCountry(feature ? getCountryName(feature) : null);
  };

  const handlePolygonClick = (feature) => {
    if (!feature || !globeRef.current) return;

    const rawName = getCountryName(feature);
    const normalizedName = normalizeCountryName(rawName);
    setSelectedCountry(normalizedName);

    const controls = globeRef.current.controls();
    controls.autoRotate = false;

    const center = getFeatureCenter(feature);
    globeRef.current.pointOfView(
      {
        lat: center.lat,
        lng: center.lng,
        altitude: 1.2
      },
      1200
    );
  };

  const polygonColor = (feature) => {
    const rawName = getCountryName(feature);
    const name = normalizeCountryName(rawName);
    const value = choroplethMap[name];

    if (!Number.isFinite(value)) {
      return "rgba(210,210,210,0.95)";
    }

    const idx = getBucketIndex(value, rangeInfo.min, rangeInfo.max);
    if (idx < 0) return "rgba(210,210,210,0.95)";
    return COLOR_SCALE[idx];
  };

  const seriesForWindow = useMemo(() => {
    if (!Array.isArray(countrySeries) || countrySeries.length === 0) return [];
    const y = Number(selectedYear);
    return countrySeries
      .filter((r) => Number(r.Year) >= MIN_YEAR && Number(r.Year) <= y)
      .map((r) => ({
        ...r,
        Year: Number(r.Year)
      }));
  }, [countrySeries, selectedYear]);

  const heatmapRef = useD3(
    (root) => {
      root.selectAll("*").remove();

      const rows = seriesForWindow;
      if (!selectedCountry || rows.length < 3) {
        root
          .append("div")
          .attr("class", "chart-empty")
          .text(selectedCountry ? "Not enough data for correlation." : "Select a country.");
        return;
      }

      const metrics = METRICS.slice();
      // Build aligned vectors (by year) to avoid different missingness per metric.
      const years = rows.map((r) => r.Year);
      const aligned = metrics.map((m) =>
        years.map((_, i) => Number(rows[i][m]))
      );

      const n = metrics.length;
      const corr = Array.from({ length: n }, () => Array(n).fill(0));
      for (let i = 0; i < n; i += 1) {
        for (let j = 0; j < n; j += 1) {
          const xs = [];
          const ys = [];
          for (let k = 0; k < years.length; k += 1) {
            const a = aligned[i][k];
            const b = aligned[j][k];
            if (Number.isFinite(a) && Number.isFinite(b)) {
              xs.push(a);
              ys.push(b);
            }
          }
          corr[i][j] = pearson(xs, ys);
        }
      }

      const dist = corr.map((row) => row.map((v) => 1 - Math.abs(v)));
      const order = clusterOrder(dist);
      const orderMeta = order.map((metricIdx, pos) => ({
        metricIdx,
        metric: metrics[metricIdx],
        number: pos + 1
      }));
      const numberByMetricIdx = new Map(orderMeta.map((d) => [d.metricIdx, d.number]));

      const size = 240;
      const pad = 72;
      const w = size + pad;
      const h = size + pad;
      const cell = size / n;

      const svg = root
        .append("svg")
        .attr("viewBox", `0 0 ${w} ${h}`)
        .attr("class", "chart-svg");

      const color = d3
        .scaleSequential()
        .domain([-1, 1])
        .interpolator(d3.interpolateRdYlBu);

      const g = svg.append("g").attr("transform", `translate(${pad},${pad / 2})`);

      // Cells
      for (let oi = 0; oi < n; oi += 1) {
        for (let oj = 0; oj < n; oj += 1) {
          const i = order[oi];
          const j = order[oj];
          g.append("rect")
            .attr("x", oj * cell)
            .attr("y", oi * cell)
            .attr("width", cell)
            .attr("height", cell)
            .attr("rx", 2)
            .attr("fill", color(corr[i][j]))
            .append("title")
            .text(
              `${numberByMetricIdx.get(i)} × ${numberByMetricIdx.get(j)}\nρ=${corr[i][j].toFixed(2)}`
            );
        }
      }

      // Labels
      const xLabels = svg.append("g").attr("transform", `translate(${pad},${pad / 2 - 6})`);
      order.forEach((idx, k) => {
        xLabels
          .append("text")
          .attr("x", k * cell + cell / 2)
          .attr("y", 0)
          .attr("text-anchor", "end")
          .attr("transform", `rotate(-45, ${k * cell + cell / 2}, 0)`)
          .attr("class", "chart-axis-label")
          .text(numberByMetricIdx.get(idx));
      });

      const yLabels = svg.append("g").attr("transform", `translate(${pad - 6},${pad / 2})`);
      order.forEach((idx, k) => {
        yLabels
          .append("text")
          .attr("x", 0)
          .attr("y", k * cell + cell / 2)
          .attr("dominant-baseline", "middle")
          .attr("text-anchor", "end")
          .attr("class", "chart-axis-label")
          .text(numberByMetricIdx.get(idx));
      });

      const legendWindow = root.append("div").attr("class", "chart-number-legend-window");
      const legend = legendWindow.append("div").attr("class", "chart-number-legend-grid");
      orderMeta.forEach((d) => {
        legend.append("div").attr("class", "chart-number-legend-cell").text(`${d.number} — ${d.metric}`);
      });
    },
    [seriesForWindow, selectedCountry]
  );

  const roseRef = useD3(
    (root) => {
      root.selectAll("*").remove();
      if (!selectedCountry || !Array.isArray(countrySeries) || countrySeries.length === 0) {
        root.append("div").attr("class", "chart-empty").text("Select a country.");
        return;
      }

      const year = Number(selectedYear);
      const row = countrySeries.find((r) => Number(r.Year) === year);
      if (!row) {
        root.append("div").attr("class", "chart-empty").text("No data for selected year.");
        return;
      }

      // Normalize each metric by min/max over the whole series for that country.
      const extents = {};
      ROSE_METRICS.forEach((m) => {
        const vals = countrySeries.map((r) => Number(r[m])).filter((v) => Number.isFinite(v));
        extents[m] = d3.extent(vals);
      });

      const data = ROSE_METRICS.map((m) => {
        const v = Number(row[m]);
        const [mn, mx] = extents[m];
        const t =
          Number.isFinite(v) && Number.isFinite(mn) && Number.isFinite(mx) && mx !== mn
            ? (v - mn) / (mx - mn)
            : 0;
        return { metric: m, value: t };
      });

      const w = 320;
      const h = 240;
      const svg = root.append("svg").attr("viewBox", `0 0 ${w} ${h}`).attr("class", "chart-svg");
      const cx = w / 2;
      const cy = h / 2 + 8;
      const inner = 28;
      const outer = 92;

      const angle = d3.scaleBand().domain(data.map((d) => d.metric)).range([0, Math.PI * 2]);
      const r = d3.scaleLinear().domain([0, 1]).range([inner, outer]);

      const g = svg.append("g").attr("transform", `translate(${cx},${cy})`);

      // rings
      g.append("circle").attr("r", inner).attr("fill", "none").attr("stroke", "rgba(255,255,255,0.12)");
      g.append("circle").attr("r", outer).attr("fill", "none").attr("stroke", "rgba(255,255,255,0.12)");

      const arc = d3
        .arc()
        .innerRadius(inner)
        .outerRadius((d) => r(d.value))
        .startAngle((d) => angle(d.metric))
        .endAngle((d) => angle(d.metric) + angle.bandwidth())
        .padAngle(0.02)
        .padRadius(inner);

      g.selectAll("path.rose")
        .data(data)
        .enter()
        .append("path")
        .attr("class", "rose")
        .attr("fill", "rgba(59, 130, 246, 0.55)")
        .attr("stroke", "rgba(96, 165, 250, 0.9)")
        .attr("d", arc)
        .append("title")
        .text((d) => `${d.metric}\nnormalized=${d.value.toFixed(2)}`);

      // labels
      g.selectAll("text.rose-label")
        .data(data)
        .enter()
        .append("text")
        .attr("class", "chart-axis-label")
        .attr("text-anchor", "middle")
        .attr("dominant-baseline", "middle")
        .attr("x", (d) => Math.cos(angle(d.metric) + angle.bandwidth() / 2 - Math.PI / 2) * (outer + 18))
        .attr("y", (d) => Math.sin(angle(d.metric) + angle.bandwidth() / 2 - Math.PI / 2) * (outer + 18))
        .text((d) => d.metric.replace("Country's Share of World Pop", "Pop share").replace("Share of World's CO2 emissions", "CO₂ share"));
    },
    [countrySeries, selectedCountry, selectedYear]
  );

  const streamRef = useD3(
    (root) => {
      root.selectAll("*").remove();
      if (!selectedCountry || !Array.isArray(countrySeries) || countrySeries.length < 3) {
        root.append("div").attr("class", "chart-empty").text("Select a country.");
        return;
      }

      const data = countrySeries
        .map((r) => ({
          Year: Number(r.Year),
          ...STREAM_METRICS.reduce((acc, m) => {
            acc[m] = Number(r[m]);
            return acc;
          }, {}),
          Population: Number(r["Population"])
        }))
        .filter((r) => Number.isFinite(r.Year));

      const w = 360;
      const h = 240;
      const margin = { top: 12, right: 12, bottom: 24, left: 36 };

      const colors = d3
        .scaleOrdinal()
        .domain(STREAM_METRICS)
        .range(["#34d399", "#fb7185", "#60a5fa", "#fbbf24"]);

      const legendTop = root.append("div").attr("class", "chart-legend-top");
      STREAM_METRICS.forEach((k) => {
        const item = legendTop.append("div").attr("class", "chart-legend-item");
        item.append("span").attr("class", "chart-legend-swatch").style("background", colors(k));
        item.append("span").attr("class", "chart-legend-text").text(k);
      });

      const svg = root.append("svg").attr("viewBox", `0 0 ${w} ${h}`).attr("class", "chart-svg");
      const innerW = w - margin.left - margin.right;
      const innerH = h - margin.top - margin.bottom;

      const x = d3
        .scaleLinear()
        .domain(d3.extent(data, (d) => d.Year))
        .range([0, innerW]);

      const stack = d3
        .stack()
        .keys(STREAM_METRICS)
        .value((d, key) => (Number.isFinite(d[key]) ? d[key] : 0))
        .offset(d3.stackOffsetWiggle);

      const layers = stack(data);

      const yExtent = d3.extent(layers.flat(2));
      const y = d3.scaleLinear().domain(yExtent).range([innerH, 0]).nice();

      const area = d3
        .area()
        .x((d, i) => x(data[i].Year))
        .y0((d) => y(d[0]))
        .y1((d) => y(d[1]))
        .curve(d3.curveCatmullRom.alpha(0.5));

      const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

      g.selectAll("path.layer")
        .data(layers)
        .enter()
        .append("path")
        .attr("class", "layer")
        .attr("d", area)
        .attr("fill", (d) => colors(d.key))
        .attr("fill-opacity", 0.55)
        .attr("stroke", "rgba(255,255,255,0.10)")
        .append("title")
        .text((d) => d.key);

      // Year marker
      const yMark = Number(selectedYear);
      if (Number.isFinite(yMark)) {
        g.append("line")
          .attr("x1", x(yMark))
          .attr("x2", x(yMark))
          .attr("y1", 0)
          .attr("y2", innerH)
          .attr("stroke", "rgba(255,255,255,0.45)")
          .attr("stroke-dasharray", "4 4");
      }

      // X axis (sparse)
      const axis = d3.axisBottom(x).ticks(5).tickFormat((d) => String(Math.round(d)));
      g.append("g")
        .attr("transform", `translate(0,${innerH})`)
        .call(axis)
        .call((sel) => sel.selectAll("text").attr("class", "chart-axis-label"));
    },
    [countrySeries, selectedCountry, selectedYear]
  );

  return (
    <div className="app">
      <header className="topbar">
        <div>
          <h1>3D Globe with Country Data</h1>
          <p>Default view: {selectedMetric} in {selectedYear}</p>
        </div>

        <div className="topbar-actions">
          <button className="toolbar-btn" onClick={() => setViewMode("split")}>
            Split View
          </button>
          <button className="toolbar-btn" onClick={() => setViewMode("globe")}>
            Expand Globe
          </button>
          <button className="toolbar-btn" onClick={() => setViewMode("panel")}>
            Expand Data Panel
          </button>
        </div>
      </header>

      <main className={`main-layout mode-${viewMode}`}>
        {viewMode !== "panel" && (
          <section className="globe-section">
            <div className="globe-wrap">
              <Globe
                ref={globeRef}
                backgroundColor="rgba(0,0,0,0)"
                globeImageUrl="//unpkg.com/three-globe/example/img/earth-blue-marble.jpg"
                bumpImageUrl="//unpkg.com/three-globe/example/img/earth-topology.png"
                polygonsData={countriesGeo}
                polygonCapColor={polygonColor}
                polygonSideColor={polygonColor}
                polygonStrokeColor={() => "#111111"}
                polygonAltitude={(feature) => {
                  const rawName = getCountryName(feature);
                  const name = normalizeCountryName(rawName);

                  if (selectedCountry && name === selectedCountry) return 0.03;
                  if (hoveredCountry && rawName === hoveredCountry) return 0.02;
                  return 0.01;
                }}
                polygonsTransitionDuration={300}
                onPolygonHover={handlePolygonHover}
                onPolygonClick={handlePolygonClick}
                polygonLabel={(feature) => {
                  const rawName = getCountryName(feature);
                  const name = normalizeCountryName(rawName);
                  const value = choroplethMap[name];

                  return `
                    <div style="padding:6px 8px;">
                      <strong>${rawName}</strong><br/>
                      <strong>${selectedMetric}:</strong> ${Number.isFinite(value) ? value.toFixed(0).replace(/\B(?=(\d{3})+(?!\d))/g, ",") : "No data"}
                    </div>
                  `;
                }}
                width={viewMode === "globe" ? 1450 : 980}
                height={760}
              />
            </div>
          </section>
        )}

        {viewMode !== "globe" && (
          <aside className="right-panel">
            <h2>Data View</h2>

            <div className="panel-block panel-block--sticky-filters">
              <h3>Default Filters</h3>
              <p>
                <strong>Year:</strong> {selectedYear}
              </p>
              <input
                className="panel-year-range"
                type="range"
                min={MIN_YEAR}
                max={MAX_YEAR}
                step={1}
                value={Number(selectedYear)}
                onChange={(e) => setSelectedYear(String(e.target.value))}
                aria-label="Year"
              />
              <div className="panel-year-ticks">
                <span>{MIN_YEAR}</span>
                <span>{MAX_YEAR}</span>
              </div>

              <div className="filter-field">
                <label htmlFor="metric-select" className="panel-block-label">
                  Metric
                </label>
                <select
                  id="metric-select"
                  className="panel-metric-select"
                  value={selectedMetric}
                  onChange={(e) => setSelectedMetric(e.target.value)}
                >
                  {METRICS.map((m) => (
                    <option key={m} value={m}>
                      {m}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            <div className="panel-block">
              <h3>Current State</h3>
              <p><strong>Hover:</strong> {hoveredCountry || "—"}</p>
              <p><strong>Selected:</strong> {selectedCountry || "—"}</p>
              <p><strong>Matched countries:</strong> {matchedCount}</p>
              <p><strong>API rows:</strong> {apiItems.length}</p>
            </div>

            <div className="panel-block">
              <h3>Legend</h3>
              <div className="legend-list">
                {legendItems.map((item) => (
                  <div key={item.label} className="legend-row">
                    <span
                      className="legend-color"
                      style={{ backgroundColor: item.color }}
                    />
                    <span className="legend-label">{item.label}</span>
                  </div>
                ))}
              </div>
            </div>

            <div className="panel-block">
              <h3>Selected Country Data</h3>
              <div className="panel-subscroll panel-subscroll--metrics">
                {metricEntries.length === 0 ? (
                  <p>No data loaded yet.</p>
                ) : (
                  <div className="metrics-list">
                    {metricEntries.map(([key, value]) => (
                      <div key={key} className="metric-row">
                        <span className="metric-key">{key}</span>
                        <span className="metric-value">
                          {value === null ? "—" : String(value)}
                        </span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>

            <div className="panel-block">
              <h3>Charts</h3>
              <div className="panel-subscroll panel-subscroll--charts">
                <div className={`charts-grid mode-${viewMode}`}>
                <div className="chart-card">
                  <div className="chart-title">1) Correlation heatmap (clustered)</div>
                  <div ref={heatmapRef} className="chart-host" />
                </div>

                <div className="chart-card">
                  <div className="chart-title">2) Nightingale rose (footprint vs share)</div>
                  <div ref={roseRef} className="chart-host" />
                </div>

                <div className="chart-card">
                  <div className="chart-title">3) Streamgraph (demographic flows)</div>
                  <div ref={streamRef} className="chart-host" />
                </div>

                <div className="chart-card chart-placeholder">
                  <div className="chart-title">4) Placeholder</div>
                  <div className="chart-empty">Coming soon</div>
                </div>

                <div className="chart-card chart-placeholder">
                  <div className="chart-title">5) Placeholder</div>
                  <div className="chart-empty">Coming soon</div>
                </div>

                <div className="chart-card chart-placeholder">
                  <div className="chart-title">6) Placeholder</div>
                  <div className="chart-empty">Coming soon</div>
                </div>
                </div>
              </div>
            </div>
          </aside>
        )}
      </main>
    </div>
  );
}