import React, { useEffect, useMemo, useRef, useState } from "react";
import Globe from "react-globe.gl";

const COLOR_SCALE = [
  "#1a9850",
  "#66bd63",
  "#a6d96a",
  "#fee08b",
  "#fdae61",
  "#f46d43",
  "#d73027"
];

const DEFAULT_YEAR = "2020";
const DEFAULT_METRIC = "Population";

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
    "democratic republic of the congo": "democratic republic of congo",
    "dem rep congo": "democratic republic of congo",
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
  const [choroplethMap, setChoroplethMap] = useState({});
  const [rangeInfo, setRangeInfo] = useState({ min: null, max: null });
  const [apiItems, setApiItems] = useState([]);
  const [viewMode, setViewMode] = useState("split");

  const selectedYear = DEFAULT_YEAR;
  const selectedMetric = DEFAULT_METRIC;

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
        label: `${from.toFixed(0)} – ${to.toFixed(0)}`
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
                      <strong>${selectedMetric}:</strong> ${Number.isFinite(value) ? value : "No data"}
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

            <div className="panel-block">
              <h3>Default Filters</h3>
              <p><strong>Year:</strong> {selectedYear}</p>
              <p><strong>Metric:</strong> {selectedMetric}</p>
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
          </aside>
        )}
      </main>
    </div>
  );
}