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

const VIOLIN_EMISSION_METRICS = [
  "CO2 emissions per capita",
  "Fossil CO2 emissions (tons)"
];

/** Gaussian KDE at x given samples */
function kdeAt(samples, x, bandwidth) {
  const n = samples.length;
  if (!n) return 0;
  const c = 1 / (bandwidth * Math.sqrt(2 * Math.PI));
  let s = 0;
  for (let i = 0; i < n; i += 1) {
    const u = (x - samples[i]) / bandwidth;
    s += Math.exp(-0.5 * u * u) * c;
  }
  return s / n;
}

/** Split-violin polygon: left = pre-2000, right = post-2000 (KDE width). */
function splitViolinPath(pre, post, yScale, xCenter, halfWidth, bandwidth) {
  const [r0, r1] = yScale.range();
  const yMin = Math.min(r0, r1);
  const yMax = Math.max(r0, r1);
  const steps = 48;
  const ys = d3.range(yMin, yMax + 1e-9, (yMax - yMin) / steps);
  let maxL = 0;
  let maxR = 0;
  ys.forEach((yp) => {
    const v = yScale.invert(yp);
    maxL = Math.max(maxL, kdeAt(pre, v, bandwidth));
    maxR = Math.max(maxR, kdeAt(post, v, bandwidth));
  });
  maxL = maxL || 1e-9;
  maxR = maxR || 1e-9;
  const left = [];
  const right = [];
  ys.forEach((yp) => {
    const v = yScale.invert(yp);
    left.push([xCenter - (kdeAt(pre, v, bandwidth) / maxL) * halfWidth, yp]);
    right.push([xCenter + (kdeAt(post, v, bandwidth) / maxR) * halfWidth, yp]);
  });
  if (!left.length) return null;
  let d = `M ${xCenter},${yMin}`;
  left.forEach((p) => {
    d += ` L ${p[0]},${p[1]}`;
  });
  d += ` L ${xCenter},${yMax}`;
  for (let i = right.length - 1; i >= 0; i -= 1) {
    d += ` L ${right[i][0]},${right[i][1]}`;
  }
  d += " Z";
  return d;
}

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
  const [chordHighlightMetric, setChordHighlightMetric] = useState(null);
  /** Year range [lo, hi] for parallel-coords temporal brush (subset of slider window). */
  const [parallelYearBrush, setParallelYearBrush] = useState(() => [
    MIN_YEAR,
    Number(DEFAULT_YEAR)
  ]);

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
      return "rgba(26,152,80,0.95)";
    }

    const idx = getBucketIndex(value, rangeInfo.min, rangeInfo.max);
    if (idx < 0) return "rgba(26,152,80,0.95)";
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
        .domain([1, -1])
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

  useEffect(() => {
    const y = Number(selectedYear);
    setParallelYearBrush(([lo, hi]) => [Math.min(lo, y), Math.min(Math.max(hi, lo), y)]);
  }, [selectedYear]);

  useEffect(() => {
    setChordHighlightMetric(null);
    setParallelYearBrush([MIN_YEAR, Number(selectedYear)]);
  }, [selectedCountry]);

  const violinRef = useD3(
    (root) => {
      root.selectAll("*").remove();
      /** Full country history only — not filtered by the year slider. */
      const rows = (Array.isArray(countrySeries) ? countrySeries : []).filter((r) => {
        const y = Number(r.Year);
        if (!Number.isFinite(y) || y < MIN_YEAR || y > MAX_YEAR) return false;
        return VIOLIN_EMISSION_METRICS.some((m) => Number.isFinite(Number(r[m])));
      });
      if (!selectedCountry || rows.length < 1) {
        root
          .append("div")
          .attr("class", "chart-empty")
          .text(selectedCountry ? "Not enough emissions data for violins." : "Select a country.");
        return;
      }

      function eraSplit(sub, metric) {
        const pre = sub
          .filter((r) => {
            const y = Number(r.Year);
            return y >= MIN_YEAR && y <= 1999;
          })
          .map((r) => Number(r[metric]))
          .filter(Number.isFinite);
        const post = sub
          .filter((r) => {
            const y = Number(r.Year);
            return y >= 2000 && y <= MAX_YEAR;
          })
          .map((r) => Number(r[metric]))
          .filter(Number.isFinite);
        return { pre, post };
      }

      const w = 340;
      const rowH = 112;
      const pad = { top: 20, right: 8, bottom: 18, left: 8 };
      const svg = root
        .append("svg")
        .attr("viewBox", `0 0 ${w} ${rowH + pad.top + pad.bottom}`)
        .attr("class", "chart-svg");

      const g0 = svg.append("g").attr("transform", `translate(${pad.left},${pad.top})`);

      VIOLIN_EMISSION_METRICS.forEach((metric, ci) => {
        const { pre, post } = eraSplit(rows, metric);
        const all = pre.concat(post);
        if (all.length < 1) return;
        let lo = d3.min(all);
        let hi = d3.max(all);
        if (!Number.isFinite(lo) || !Number.isFinite(hi)) return;
        const span = hi - lo || Math.abs(lo) * 0.08 || 1;
        const padY = span * 0.08;
        lo -= padY;
        hi += padY;
        if (hi === lo) {
          lo -= 1;
          hi += 1;
        }

        const bwMetric =
          ((d3.deviation(all) ?? 0) || span * 0.2 || Math.abs(lo) * 0.05 || 1) *
          Math.pow(Math.max(all.length, 2), -0.15);

        const yScale = d3.scaleLinear().domain([lo, hi]).range([rowH - 26, 8]);

        const cellW = (w - pad.left - pad.right) / 2;
        const cx = ci * cellW + cellW / 2;
        const hw = cellW * 0.38;

        const dPath = splitViolinPath(pre, post, yScale, cx, hw, bwMetric);
        g0
          .append("path")
          .attr("fill", ci === 0 ? "rgba(56, 189, 248, 0.55)" : "rgba(52, 211, 153, 0.55)")
          .attr("stroke", "rgba(255,255,255,0.45)")
          .attr("d", dPath || "");

        g0
          .append("text")
          .attr("class", "chart-axis-label")
          .attr("text-anchor", "middle")
          .attr("x", cx)
          .attr("y", rowH - 2)
          .text(metric === "CO2 emissions per capita" ? "CO₂ per capita" : "Fossil CO₂ (tons)");

        g0
          .append("text")
          .attr("class", "chart-axis-label")
          .attr("text-anchor", "middle")
          .attr("x", cx - hw * 0.35)
          .attr("y", rowH - 22)
          .text(`< 2000`);

        g0
          .append("text")
          .attr("class", "chart-axis-label")
          .attr("text-anchor", "middle")
          .attr("x", cx + hw * 0.35)
          .attr("y", rowH - 22)
          .text(`>= 2000`);
      });
    },
    [countrySeries, selectedCountry]
  );

  const chordRef = useD3(
    (root) => {
      root.selectAll("*").remove();
      const rows = seriesForWindow;
      if (!selectedCountry || rows.length < 3) {
        root
          .append("div")
          .attr("class", "chart-empty")
          .text(selectedCountry ? "Not enough data for chord." : "Select a country.");
        return;
      }

      const metrics = METRICS.slice();
      const years = rows.map((r) => r.Year);
      const aligned = metrics.map((m) => years.map((_, i) => Number(rows[i][m])));
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

      const matrix = Array.from({ length: n }, () => Array(n).fill(0));
      for (let i = 0; i < n; i += 1) {
        for (let j = 0; j < n; j += 1) {
          if (i === j) matrix[i][j] = 0.001;
          else matrix[i][j] = Math.abs(corr[i][j]);
        }
      }

      const w = 340;
      const h = 320;
      const outerRadius = Math.min(w, h) * 0.42;
      const innerRadius = outerRadius - 18;

      const chordLayout = d3.chord().padAngle(0.04).sortSubgroups(d3.descending);
      const chords = chordLayout(matrix);

      const arc = d3.arc().innerRadius(innerRadius).outerRadius(outerRadius);
      const ribbon = d3.ribbon().radius(innerRadius - 2);

      const svg = root
        .append("svg")
        .attr("viewBox", `${-w / 2} ${-h / 2} ${w} ${h}`)
        .attr("class", "chart-svg chart-chord-svg");

      const g = svg.append("g");

      const group = g.append("g").selectAll("g").data(chords.groups).join("g");

      group
        .append("path")
        .attr("fill", (_, i) => d3.interpolateSinebow(i / n))
        .attr("stroke", "rgba(0,0,0,0.35)")
        .attr("class", "chord-group-arc")
        .attr("id", (_, i) => `chord-arc-${i}`)
        .attr("d", arc)
        .style("cursor", "pointer")
        .style("opacity", (_, i) =>
          chordHighlightMetric === null || chordHighlightMetric === i ? 0.92 : 0.25
        )
        .on("click", (event, d) => {
          event.stopPropagation();
          setChordHighlightMetric((prev) => (prev === d.index ? null : d.index));
        });

      group
        .append("text")
        .each(function (d) {
          const a = (d.startAngle + d.endAngle) / 2 - Math.PI / 2;
          d3.select(this)
            .attr("transform", `translate(${Math.cos(a) * (outerRadius + 10)},${Math.sin(a) * (outerRadius + 10)})`)
            .attr("text-anchor", "middle")
            .attr("class", "chart-chord-label")
            .text(metrics[d.index].length > 14 ? metrics[d.index].slice(0, 12) + "…" : metrics[d.index]);
        });

      const ribbons = g
        .append("g")
        .attr("fill-opacity", 0.78)
        .selectAll("path")
        .data(chords)
        .join("path")
        .attr("d", ribbon)
        .attr("fill", (d) => {
          const r = corr[d.source.index][d.target.index];
          return r >= 0 ? "rgba(34, 197, 94, 0.45)" : "rgba(248, 113, 113, 0.45)";
        })
        .attr("stroke", (d) => {
          const r = corr[d.source.index][d.target.index];
          return r >= 0 ? "rgba(34, 197, 94, 0.95)" : "rgba(248, 113, 113, 0.95)";
        })
        .style("cursor", "pointer")
        .style("opacity", (d) => {
          if (chordHighlightMetric === null) return 0.55;
          return d.source.index === chordHighlightMetric || d.target.index === chordHighlightMetric
            ? 0.95
            : 0.08;
        })
        .on("mouseenter", function (event, d) {
          const i = d.source.index;
          const j = d.target.index;
          const r = corr[i][j];
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
          d3.select(this).attr("stroke-width", 2);
          root.selectAll(".chord-tooltip").remove();
          const tip = root.append("div").attr("class", "chord-tooltip");
          const [mx, my] = d3.pointer(event, root.node());
          tip.style("left", `${mx + 10}px`).style("top", `${my + 10}px`);
          tip.html(
            `<div class="chord-tip-title">r = ${r.toFixed(3)}</div>` +
              `<div class="chord-tip-sub">${metrics[i]} ↔ ${metrics[j]}</div>` +
              `<svg class="chord-scatter" viewBox="0 0 90 50" width="90" height="50"></svg>`
          );
          const mini = tip.select("svg");
          if (xs.length < 2) return;
          const xEx = d3.extent(xs);
          const yEx = d3.extent(ys);
          const sx = d3.scaleLinear().domain(xEx).range([4, 86]).nice();
          const sy = d3.scaleLinear().domain(yEx).range([44, 6]).nice();
          mini
            .selectAll("circle")
            .data(xs.map((x, idx) => [x, ys[idx]]))
            .join("circle")
            .attr("cx", (p) => sx(p[0]))
            .attr("cy", (p) => sy(p[1]))
            .attr("r", 2)
            .attr("fill", r >= 0 ? "#22c55e" : "#f87171");
        })
        .on("mouseleave", function () {
          d3.select(this).attr("stroke-width", null);
          root.selectAll(".chord-tooltip").remove();
        });

      svg.on("click", () => setChordHighlightMetric(null));
    },
    [seriesForWindow, selectedCountry, selectedYear, chordHighlightMetric]
  );

  const parallelRef = useD3(
    (root) => {
      root.selectAll("*").remove();
      const rows = seriesForWindow;
      if (!selectedCountry || rows.length < 2) {
        root
          .append("div")
          .attr("class", "chart-empty")
          .text(selectedCountry ? "Not enough data for parallel plot." : "Select a country.");
        return;
      }

      const metrics = METRICS.slice();
      const extents = {};
      metrics.forEach((m) => {
        const vals = rows.map((r) => Number(r[m])).filter(Number.isFinite);
        if (!vals.length) {
          extents[m] = { min: 0, max: 1, span: 1 };
          return;
        }
        const [a, b] = d3.extent(vals);
        const span = !Number.isFinite(a) || b === a ? 1e-9 : b - a;
        extents[m] = { min: a, max: b, span };
      });

      const [yLo, yHi] = parallelYearBrush;
      const brushLo = Math.max(MIN_YEAR, Math.min(yLo, yHi));
      const brushHi = Math.min(Number(selectedYear), Math.max(yLo, yHi));
      const filtered = rows.filter((r) => r.Year >= brushLo && r.Year <= brushHi);

      const w = 360;
      const h = 260;
      const margin = { top: 28, right: 10, bottom: 36, left: 10 };
      const innerW = w - margin.left - margin.right;
      const innerH = h - margin.top - margin.bottom;

      const denom = Math.max(1, metrics.length - 1);
      const xPos = metrics.map((_, i) => (i / denom) * innerW);
      const yNorm = d3.scaleLinear().domain([0, 1]).range([innerH, 0]);

      const yearExtent = d3.extent(rows, (r) => r.Year);
      const yearColor = d3
        .scaleSequential(d3.interpolateTurbo)
        .domain(yearExtent[0] === yearExtent[1] ? [yearExtent[0] - 1, yearExtent[1] + 1] : yearExtent);

      const svg = root.append("svg").attr("viewBox", `0 0 ${w} ${h}`).attr("class", "chart-svg");

      const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

      const lineGen = d3
        .line()
        .defined((d) => d.every((v) => Number.isFinite(v)))
        .curve(d3.curveMonotoneX);

      const selYear = Number(selectedYear);

      filtered.forEach((row) => {
        const pts = metrics.map((m, i) => {
          const v = Number(row[m]);
          const { min, span } = extents[m];
          const t = Number.isFinite(v) ? (v - min) / span : NaN;
          return [xPos[i], yNorm(t)];
        });
        const isSel = row.Year === selYear;
        g.append("path")
          .datum(pts)
          .attr("fill", "none")
          .attr("stroke", yearColor(row.Year))
          .attr("stroke-width", isSel ? 3 : 1.1)
          .attr("stroke-opacity", isSel ? 1 : 0.72)
          .attr("stroke-linecap", "round")
          .attr("stroke-linejoin", "round")
          .attr("d", lineGen);
      });

      metrics.forEach((m, i) => {
        g.append("line")
          .attr("x1", xPos[i])
          .attr("x2", xPos[i])
          .attr("y1", 0)
          .attr("y2", innerH)
          .attr("stroke", "rgba(255,255,255,0.12)");
        g.append("text")
          .attr("class", "chart-pc-axis")
          .attr("x", xPos[i])
          .attr("y", innerH + 12)
          .attr("text-anchor", "middle")
          .text(String(i + 1));
      });

      const yearScale = d3
        .scaleLinear()
        .domain([MIN_YEAR, Number(selectedYear)])
        .range([0, innerW]);

      const brushG = svg.append("g").attr("transform", `translate(${margin.left},${h - 22})`);

      const b = d3
        .brushX()
        .extent([
          [0, 0],
          [innerW, 14]
        ])
        .on("end", (event) => {
          if (!event.selection) return;
          if (!event.sourceEvent) return;
          const [x0, x1] = event.selection.map(yearScale.invert);
          const lo = Math.round(Math.min(x0, x1));
          const hi = Math.round(Math.max(x0, x1));
          setParallelYearBrush([
            Math.max(MIN_YEAR, lo),
            Math.min(Number(selectedYear), hi)
          ]);
        });

      brushG.call(b);
      brushG.call(b.move, [yearScale(brushLo), yearScale(brushHi)]);

      brushG.selectAll(".selection").attr("fill", "rgba(59, 130, 246, 0.25)");
      brushG.append("text").attr("x", 0).attr("y", -6).attr("class", "chart-axis-label").text("Brush years");
    },
    [seriesForWindow, selectedCountry, selectedYear, parallelYearBrush]
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
              <p><strong>Selected:</strong> {selectedCountry?.replace(/\b\w/g, char => char.toUpperCase()) || "—"}</p>
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

                <div className="chart-card">
                  <div className="chart-title">4) Split violin: CO₂ emissions (pre/post-2000)</div>
                  <div ref={violinRef} className="chart-host chart-host--overlay" />
                </div>

                <div className="chart-card">
                  <div className="chart-title">5) Chord: metric interdependencies (|r|)</div>
                  <div ref={chordRef} className="chart-host chart-host--overlay" />
                </div>

                <div className="chart-card">
                  <div className="chart-title">6) Parallel coords (normalized) + year brush</div>
                  <div ref={parallelRef} className="chart-host" />
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