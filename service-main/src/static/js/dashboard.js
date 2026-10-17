// KTDL-Group20 Airlines Lakehouse Dashboard Client
(function () {
  "use strict";

  // State
  const state = {
    currentTab: "overview",
    summary: null,
    airports: [],
    routes: [],
    runs: [],
    marts: {},
    mapInstance: null,
    mapInitialized: false,
    charts: {},
    filters: {
      month: "",
      dep_airport: "",
      aircraft_code: "",
    },
  };

  // Helper formatters
  const formatCurrency = (val) => {
    if (val === undefined || val === null) return "0 ₽";
    const num = Number(val);
    if (num >= 1e9) return (num / 1e9).toFixed(2) + "B ₽";
    if (num >= 1e6) return (num / 1e6).toFixed(2) + "M ₽";
    if (num >= 1e3) return (num / 1e3).toFixed(1) + "k ₽";
    return num.toLocaleString() + " ₽";
  };

  const formatNumber = (val) => {
    if (val === undefined || val === null) return "0";
    return Number(val).toLocaleString();
  };

  const formatPercent = (val) => {
    if (val === undefined || val === null) return "0.0%";
    return (Number(val) * 100).toFixed(1) + "%";
  };

  const formatDate = (isoStr) => {
    if (!isoStr) return "-";
    try {
      const d = new Date(isoStr);
      return d.toLocaleString("en-US", {
        timeZone: "UTC",
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      }) + " UTC";
    } catch {
      return isoStr;
    }
  };

  // API Client
  async function fetchJson(url) {
    try {
      const res = await fetch(url);
      if (!res.ok) {
        throw new Error(`HTTP error ${res.status}: ${res.statusText}`);
      }
      return await res.json();
    } catch (err) {
      console.error(`Failed to fetch ${url}:`, err);
      return null;
    }
  }

  function getQueryString(extra = {}) {
    const params = new URLSearchParams();
    if (state.filters.month) params.set("month", state.filters.month);
    if (state.filters.dep_airport) params.set("dep_airport", state.filters.dep_airport);
    if (state.filters.aircraft_code) params.set("aircraft_code", state.filters.aircraft_code);
    for (const [k, v] of Object.entries(extra)) {
      if (v !== undefined && v !== null && v !== "") {
        params.set(k, String(v));
      }
    }
    const q = params.toString();
    return q ? `?${q}` : "";
  }

  // Load all initial datasets
  async function loadAllData() {
    const q = getQueryString();

    const [
      summary,
      airports,
      routes,
      runs,
      pareto,
      revenue,
      fleet,
      delayAircraft,
      delayHeatmap,
      delayRoute,
    ] = await Promise.all([
      fetchJson("/api/summary"),
      fetchJson("/api/airports?limit=500"),
      fetchJson("/api/routes?limit=500"),
      fetchJson("/api/runs?limit=20"),
      fetchJson(`/api/marts/route_pareto${getQueryString({ limit: 50 })}`),
      fetchJson(`/api/marts/route_revenue${q}`),
      fetchJson(`/api/marts/fleet${q}`),
      fetchJson(`/api/marts/delay_by_aircraft${q}`),
      fetchJson("/api/marts/delay_heatmap"),
      fetchJson(`/api/marts/delay_by_route${getQueryString({ limit: 20, sort: "-delay_rate" })}`),
    ]);

    state.summary = summary || {};
    state.airports = airports || [];
    state.routes = routes || [];
    state.runs = runs || [];
    state.marts.route_pareto = pareto || [];
    state.marts.route_revenue = revenue || [];
    state.marts.fleet = fleet || [];
    state.marts.delay_by_aircraft = delayAircraft || [];
    state.marts.delay_heatmap = delayHeatmap || [];
    state.marts.delay_by_route = delayRoute || [];

    populateFilterOptions();
    renderAll();
  }

  function populateFilterOptions() {
    // Populate month filter if available
    const monthSelect = document.getElementById("filter-month");
    if (monthSelect && state.marts.route_revenue && monthSelect.options.length <= 1) {
      const months = Array.from(
        new Set(state.marts.route_revenue.map((r) => r.month).filter(Boolean))
      ).sort();
      months.forEach((m) => {
        const opt = document.createElement("option");
        opt.value = m;
        opt.textContent = m;
        monthSelect.appendChild(opt);
      });
    }

    // Populate airport filter
    const airportSelect = document.getElementById("filter-airport");
    if (airportSelect && state.airports && airportSelect.options.length <= 1) {
      state.airports.forEach((a) => {
        const opt = document.createElement("option");
        opt.value = a.airport_code;
        opt.textContent = `${a.airport_code} (${a.city || a.airport_name || ""})`;
        airportSelect.appendChild(opt);
      });
    }

    // Populate aircraft filter
    const aircraftSelect = document.getElementById("filter-aircraft");
    if (aircraftSelect && state.marts.fleet && aircraftSelect.options.length <= 1) {
      state.marts.fleet.forEach((f) => {
        const opt = document.createElement("option");
        opt.value = f.aircraft_code;
        opt.textContent = `${f.aircraft_code} - ${f.model || ""}`;
        aircraftSelect.appendChild(opt);
      });
    }
  }

  function renderAll() {
    renderOverview();
    renderMap();
    renderDelays();
    renderRevenue();
    renderFleet();
    renderPipeline();
  }

  // Helper to toggle empty state container
  function toggleEmptyState(containerId, isEmpty) {
    const el = document.getElementById(containerId);
    if (!el) return;
    const emptyEl = el.querySelector(".empty-state");
    const contentEl = el.querySelector(".tab-data");
    if (emptyEl && contentEl) {
      emptyEl.style.display = isEmpty ? "block" : "none";
      contentEl.style.display = isEmpty ? "none" : "block";
    }
  }

  // 1. Overview Tab
  function renderOverview() {
    const s = state.summary || {};
    const hasData = (s.total_flights || 0) > 0 || (s.total_revenue || 0) > 0;
    toggleEmptyState("tab-overview", !hasData);

    const setText = (id, val) => {
      const el = document.getElementById(id);
      if (el) el.textContent = val;
    };

    setText("kpi-revenue", formatCurrency(s.total_revenue));
    setText("kpi-flights", formatNumber(s.total_flights));
    setText("kpi-delayed", `${formatNumber(s.total_delayed)} (${formatPercent(s.delay_rate)})`);
    setText("kpi-load-factor", formatPercent(s.avg_load_factor));
    setText("kpi-fleet-count", `${formatNumber(s.total_aircraft)} models`);
    setText("kpi-airports-count", `${formatNumber(s.total_airports)} airports`);

    // Latest run badge
    const badge = document.getElementById("latest-run-badge");
    if (badge) {
      if (s.latest_run) {
        badge.className = `badge badge-${s.latest_run.status === "success" ? "success" : "danger"}`;
        badge.textContent = `Pipeline: ${s.latest_run.status} (${formatDate(s.latest_run.finished_at || s.latest_run.cutoff)})`;
      } else {
        badge.className = "badge badge-warning";
        badge.textContent = "Pipeline: No runs yet";
      }
    }
  }

  // 2. Map Tab (Leaflet)
  function renderMap() {
    const hasData = state.airports.length > 0 || state.routes.length > 0;
    toggleEmptyState("tab-map", !hasData);

    if (!hasData) return;
    if (typeof L === "undefined") {
      console.warn("Leaflet library not loaded");
      return;
    }

    const mapDiv = document.getElementById("map-container");
    if (!mapDiv) return;

    if (!state.mapInstance) {
      state.mapInstance = L.map("map-container").setView([60.0, 95.0], 3);
      L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
        attribution: "&copy; OpenStreetMap contributors",
        maxZoom: 18,
      }).addTo(state.mapInstance);
      state.mapLayerGroup = L.layerGroup().addTo(state.mapInstance);
    }

    state.mapLayerGroup.clearLayers();

    // Plot top routes first (underneath airport markers)
    state.routes.forEach((r) => {
      if (r.dep_lat && r.dep_lon && r.arr_lat && r.arr_lon) {
        const poly = L.polyline(
          [
            [r.dep_lat, r.dep_lon],
            [r.arr_lat, r.arr_lon],
          ],
          {
            color: "#38bdf8",
            weight: Math.max(1, Math.min(4, Math.log2(r.flights || 1))),
            opacity: 0.4,
          }
        );
        poly.bindPopup(`<b>${r.dep_airport} &rarr; ${r.arr_airport}</b><br>Flights: ${formatNumber(r.flights)}`);
        state.mapLayerGroup.addLayer(poly);
      }
    });

    // Plot airport circles sized by departures
    state.airports.forEach((a) => {
      if (a.lat && a.lon) {
        const deps = a.departures || 0;
        const radius = Math.max(4, Math.min(22, Math.sqrt(deps) * 0.7));
        const circle = L.circleMarker([a.lat, a.lon], {
          radius: radius,
          fillColor: "#fbbf24",
          color: "#d97706",
          weight: 1.5,
          opacity: 0.9,
          fillOpacity: 0.6,
        });
        circle.bindPopup(
          `<b>${a.airport_code} - ${a.airport_name || ""}</b><br>
           City: ${a.city || "-"}<br>
           Timezone: ${a.timezone || "-"}<br>
           Departures: ${formatNumber(deps)}`
        );
        state.mapLayerGroup.addLayer(circle);
      }
    });

    // Invalidate size if map container was hidden
    setTimeout(() => {
      if (state.mapInstance) state.mapInstance.invalidateSize();
    }, 100);
  }

  // 3. Delays Tab
  function renderDelays() {
    const heatmapData = state.marts.delay_heatmap || [];
    const aircraftData = state.marts.delay_by_aircraft || [];
    const routeData = state.marts.delay_by_route || [];
    const hasData = heatmapData.length > 0 || aircraftData.length > 0 || routeData.length > 0;
    toggleEmptyState("tab-delays", !hasData);
    if (!hasData) return;

    // A. Heatmap Grid: DOW (1..7) x Hour (0..23)
    const heatmapContainer = document.getElementById("delay-heatmap-grid");
    if (heatmapContainer) {
      heatmapContainer.innerHTML = "";
      // Map lookup key: `${dow}|${hour}`
      const lookup = new Map();
      heatmapData.forEach((d) => {
        lookup.set(`${d.dow}|${d.hour}`, d);
      });

      // Header row: empty corner + 0..23
      const corner = document.createElement("div");
      corner.className = "heatmap-header-cell";
      corner.textContent = "DOW / Hr";
      heatmapContainer.appendChild(corner);

      for (let h = 0; h < 24; h++) {
        const hc = document.createElement("div");
        hc.className = "heatmap-header-cell";
        hc.textContent = `${h}h`;
        heatmapContainer.appendChild(hc);
      }

      const dowNames = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
      for (let d = 1; d <= 7; d++) {
        const dowLabel = document.createElement("div");
        dowLabel.className = "heatmap-dow-label";
        dowLabel.textContent = dowNames[d - 1];
        heatmapContainer.appendChild(dowLabel);

        for (let h = 0; h < 24; h++) {
          const item = lookup.get(`${d}|${h}`);
          const cell = document.createElement("div");
          cell.className = "heatmap-cell";
          if (item) {
            const rate = item.delay_rate || 0;
            // Color interpolation: green (low) -> amber (med) -> red (high)
            const alpha = Math.min(1.0, Math.max(0.15, rate * 5));
            let bgColor;
            if (rate < 0.05) {
              bgColor = `rgba(52, 211, 153, ${alpha})`;
            } else if (rate < 0.15) {
              bgColor = `rgba(251, 191, 36, ${alpha})`;
            } else {
              bgColor = `rgba(248, 113, 113, ${alpha})`;
            }
            cell.style.backgroundColor = bgColor;
            cell.textContent = rate > 0 ? (rate * 100).toFixed(0) + "%" : "";
            cell.title = `${dowNames[d - 1]} ${h}:00 - Flights: ${item.flights}, Delayed: ${item.delayed} (${(rate * 100).toFixed(1)}%)`;
          } else {
            cell.title = `${dowNames[d - 1]} ${h}:00 - No data`;
          }
          heatmapContainer.appendChild(cell);
        }
      }
    }

    // B. Delay by Aircraft Chart
    if (typeof Chart !== "undefined" && document.getElementById("chart-delay-aircraft")) {
      if (state.charts.delayAircraft) state.charts.delayAircraft.destroy();
      const ctx = document.getElementById("chart-delay-aircraft").getContext("2d");
      const labels = aircraftData.map((d) => d.model || d.aircraft_code);
      const rates = aircraftData.map((d) => (d.delay_rate * 100).toFixed(1));
      const avgMins = aircraftData.map((d) => (d.avg_delay_min || 0).toFixed(1));

      state.charts.delayAircraft = new Chart(ctx, {
        type: "bar",
        data: {
          labels: labels,
          datasets: [
            {
              label: "Delay Rate (%)",
              data: rates,
              backgroundColor: "rgba(248, 113, 113, 0.7)",
              borderColor: "#f87171",
              borderWidth: 1,
              yAxisID: "yRate",
            },
            {
              label: "Avg Delay (min)",
              data: avgMins,
              backgroundColor: "rgba(251, 191, 36, 0.7)",
              borderColor: "#fbbf24",
              borderWidth: 1,
              yAxisID: "yMin",
            },
          ],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          scales: {
            yRate: {
              type: "linear",
              position: "left",
              title: { display: true, text: "Delay Rate (%)", color: "#94a3b8" },
              ticks: { color: "#94a3b8" },
              grid: { color: "#334155" },
            },
            yMin: {
              type: "linear",
              position: "right",
              title: { display: true, text: "Avg Delay (min)", color: "#94a3b8" },
              ticks: { color: "#94a3b8" },
              grid: { drawOnChartArea: false },
            },
            x: {
              ticks: { color: "#94a3b8", maxRotation: 45, minRotation: 45 },
              grid: { color: "#334155" },
            },
          },
          plugins: {
            legend: { labels: { color: "#f8fafc" } },
          },
        },
      });
    }

    // C. Top-20 Delayed Routes Table
    const routeTableBody = document.querySelector("#table-delay-routes tbody");
    if (routeTableBody) {
      routeTableBody.innerHTML = "";
      routeData.slice(0, 20).forEach((r) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td><b>${r.dep_airport}</b> (${r.dep_city || ""})</td>
          <td><b>${r.arr_airport}</b> (${r.arr_city || ""})</td>
          <td>${formatNumber(r.flights)}</td>
          <td>${formatNumber(r.delayed)}</td>
          <td><span class="badge badge-danger">${formatPercent(r.delay_rate)}</span></td>
          <td>${(r.avg_delay_min || 0).toFixed(1)} min</td>
        `;
        routeTableBody.appendChild(tr);
      });
    }
  }

  // 4. Revenue Tab
  function renderRevenue() {
    const paretoData = state.marts.route_pareto || [];
    const revenueData = state.marts.route_revenue || [];
    const hasData = paretoData.length > 0 || revenueData.length > 0;
    toggleEmptyState("tab-revenue", !hasData);
    if (!hasData) return;

    if (typeof Chart === "undefined") return;

    // A. Route Pareto Chart (Top 50)
    if (document.getElementById("chart-pareto")) {
      if (state.charts.pareto) state.charts.pareto.destroy();
      const ctx = document.getElementById("chart-pareto").getContext("2d");
      const top50 = paretoData.slice(0, 50);
      const labels = top50.map((d) => `${d.dep_airport}-${d.arr_airport}`);
      const revenues = top50.map((d) => d.revenue);
      const cumShares = top50.map((d) => ((d.cum_share || 0) * 100).toFixed(1));

      state.charts.pareto = new Chart(ctx, {
        type: "bar",
        data: {
          labels: labels,
          datasets: [
            {
              type: "line",
              label: "Cumulative Share (%)",
              data: cumShares,
              borderColor: "#38bdf8",
              backgroundColor: "#38bdf8",
              borderWidth: 2,
              fill: false,
              yAxisID: "yCum",
            },
            {
              type: "bar",
              label: "Revenue (₽)",
              data: revenues,
              backgroundColor: "rgba(129, 140, 248, 0.7)",
              borderColor: "#818cf8",
              borderWidth: 1,
              yAxisID: "yRev",
            },
          ],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          scales: {
            yRev: {
              type: "linear",
              position: "left",
              title: { display: true, text: "Revenue (₽)", color: "#94a3b8" },
              ticks: {
                color: "#94a3b8",
                callback: (v) => formatCurrency(v),
              },
              grid: { color: "#334155" },
            },
            yCum: {
              type: "linear",
              position: "right",
              title: { display: true, text: "Cum Share (%)", color: "#94a3b8" },
              ticks: { color: "#94a3b8", max: 100, min: 0 },
              grid: { drawOnChartArea: false },
            },
            x: {
              ticks: { color: "#94a3b8", display: false },
              grid: { display: false },
            },
          },
          plugins: {
            legend: { labels: { color: "#f8fafc" } },
          },
        },
      });
    }

    // B. Fare-Class Doughnut
    if (document.getElementById("chart-fare-class")) {
      if (state.charts.fareClass) state.charts.fareClass.destroy();
      const ctx = document.getElementById("chart-fare-class").getContext("2d");

      // Aggregate revenue by fare_conditions
      const fareRev = {};
      revenueData.forEach((d) => {
        const fc = d.fare_conditions || "Unknown";
        fareRev[fc] = (fareRev[fc] || 0) + (d.revenue || 0);
      });

      state.charts.fareClass = new Chart(ctx, {
        type: "doughnut",
        data: {
          labels: Object.keys(fareRev),
          datasets: [
            {
              data: Object.values(fareRev),
              backgroundColor: ["#38bdf8", "#818cf8", "#34d399", "#fbbf24"],
              borderWidth: 1,
              borderColor: "#1e293b",
            },
          ],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { position: "right", labels: { color: "#f8fafc" } },
            tooltip: {
              callbacks: {
                label: (ctx2) => `${ctx2.label}: ${formatCurrency(ctx2.raw)}`,
              },
            },
          },
        },
      });
    }

    // C. Monthly Revenue Trend Line
    if (document.getElementById("chart-monthly-rev")) {
      if (state.charts.monthlyRev) state.charts.monthlyRev.destroy();
      const ctx = document.getElementById("chart-monthly-rev").getContext("2d");

      const monthRev = {};
      revenueData.forEach((d) => {
        const m = d.month || "Unknown";
        monthRev[m] = (monthRev[m] || 0) + (d.revenue || 0);
      });
      const sortedMonths = Object.keys(monthRev).sort();

      state.charts.monthlyRev = new Chart(ctx, {
        type: "line",
        data: {
          labels: sortedMonths,
          datasets: [
            {
              label: "Monthly Revenue (₽)",
              data: sortedMonths.map((m) => monthRev[m]),
              borderColor: "#34d399",
              backgroundColor: "rgba(52, 211, 153, 0.15)",
              borderWidth: 2,
              fill: true,
              tension: 0.3,
            },
          ],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          scales: {
            y: {
              ticks: { color: "#94a3b8", callback: (v) => formatCurrency(v) },
              grid: { color: "#334155" },
            },
            x: {
              ticks: { color: "#94a3b8" },
              grid: { color: "#334155" },
            },
          },
          plugins: {
            legend: { labels: { color: "#f8fafc" } },
          },
        },
      });
    }
  }

  // 5. Fleet Tab
  function renderFleet() {
    const fleetData = state.marts.fleet || [];
    const hasData = fleetData.length > 0;
    toggleEmptyState("tab-fleet", !hasData);
    if (!hasData) return;

    if (typeof Chart !== "undefined") {
      // A. Load Factor Bars
      if (document.getElementById("chart-fleet-lf")) {
        if (state.charts.fleetLf) state.charts.fleetLf.destroy();
        const ctx = document.getElementById("chart-fleet-lf").getContext("2d");
        const labels = fleetData.map((f) => f.model || f.aircraft_code);
        const lfs = fleetData.map((f) => ((f.avg_load_factor || 0) * 100).toFixed(1));

        state.charts.fleetLf = new Chart(ctx, {
          type: "bar",
          data: {
            labels: labels,
            datasets: [
              {
                label: "Avg Load Factor (%)",
                data: lfs,
                backgroundColor: "rgba(56, 189, 248, 0.7)",
                borderColor: "#38bdf8",
                borderWidth: 1,
              },
            ],
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
              y: {
                max: 100,
                min: 0,
                ticks: { color: "#94a3b8" },
                grid: { color: "#334155" },
              },
              x: {
                ticks: { color: "#94a3b8", maxRotation: 45, minRotation: 45 },
                grid: { color: "#334155" },
              },
            },
            plugins: {
              legend: { labels: { color: "#f8fafc" } },
            },
          },
        });
      }

      // B. Seat Config Stacked Bars
      if (document.getElementById("chart-seat-config")) {
        if (state.charts.seatConfig) state.charts.seatConfig.destroy();
        const ctx = document.getElementById("chart-seat-config").getContext("2d");
        const labels = fleetData.map((f) => f.model || f.aircraft_code);

        state.charts.seatConfig = new Chart(ctx, {
          type: "bar",
          data: {
            labels: labels,
            datasets: [
              {
                label: "Economy",
                data: fleetData.map((f) => f.seats_economy || 0),
                backgroundColor: "#38bdf8",
              },
              {
                label: "Comfort",
                data: fleetData.map((f) => f.seats_comfort || 0),
                backgroundColor: "#818cf8",
              },
              {
                label: "Business",
                data: fleetData.map((f) => f.seats_business || 0),
                backgroundColor: "#fbbf24",
              },
            ],
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
              x: {
                stacked: true,
                ticks: { color: "#94a3b8", maxRotation: 45, minRotation: 45 },
                grid: { color: "#334155" },
              },
              y: {
                stacked: true,
                title: { display: true, text: "Seats", color: "#94a3b8" },
                ticks: { color: "#94a3b8" },
                grid: { color: "#334155" },
              },
            },
            plugins: {
              legend: { labels: { color: "#f8fafc" } },
            },
          },
        });
      }
    }

    // C. Fleet Details Table
    const fleetTableBody = document.querySelector("#table-fleet tbody");
    if (fleetTableBody) {
      fleetTableBody.innerHTML = "";
      fleetData.forEach((f) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td><code>${f.aircraft_code}</code></td>
          <td><b>${f.model || "-"}</b></td>
          <td>${formatNumber(f.range)} km</td>
          <td>${formatNumber(f.seats_total || ((f.seats_economy || 0) + (f.seats_comfort || 0) + (f.seats_business || 0)))}</td>
          <td>${formatNumber(f.flights)}</td>
          <td>${Number(f.flight_hours || 0).toFixed(1)} h</td>
          <td>${Number(f.avg_duration_min || 0).toFixed(0)} min</td>
          <td><span class="badge badge-info">${formatPercent(f.avg_load_factor)}</span></td>
        `;
        fleetTableBody.appendChild(tr);
      });
    }
  }

  // 6. Pipeline Tab
  function renderPipeline() {
    const runs = state.runs || [];
    const hasData = runs.length > 0;
    toggleEmptyState("tab-pipeline", !hasData);
    if (!hasData) return;

    const tbody = document.querySelector("#table-pipeline tbody");
    if (!tbody) return;
    tbody.innerHTML = "";

    runs.forEach((r) => {
      const counts = r.counts || {};
      const bronzeTotal = Object.values(counts.bronze || {}).reduce((a, b) => a + Number(b), 0);
      const silverTotal = Object.values(counts.silver || {}).reduce((a, b) => a + Number(b), 0);
      const goldTotal = Object.values(counts.gold || {}).reduce((a, b) => a + Number(b), 0);
      const quarantine = r.quarantine || 0;

      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td><code>${r.run_id || r._id}</code></td>
        <td>${formatDate(r.cutoff)}</td>
        <td>${formatDate(r.started_at)}</td>
        <td>${formatDate(r.finished_at)}</td>
        <td><span class="badge badge-${r.status === "success" ? "success" : "danger"}">${r.status}</span></td>
        <td>${formatNumber(bronzeTotal)}</td>
        <td>${formatNumber(silverTotal)}</td>
        <td>${formatNumber(goldTotal)}</td>
        <td><span class="badge badge-${quarantine === 0 ? "success" : "warning"}">${formatNumber(quarantine)}</span></td>
      `;
      tbody.appendChild(tr);
    });
  }

  // Event Listeners & Tab Switching
  function setupEvents() {
    document.querySelectorAll(".tab-btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        const tabName = btn.dataset.tab;
        switchTab(tabName);
      });
    });

    const refreshBtn = document.getElementById("btn-refresh");
    if (refreshBtn) {
      refreshBtn.addEventListener("click", () => {
        loadAllData();
      });
    }

    const monthSelect = document.getElementById("filter-month");
    if (monthSelect) {
      monthSelect.addEventListener("change", (e) => {
        state.filters.month = e.target.value;
        loadAllData();
      });
    }

    const airportSelect = document.getElementById("filter-airport");
    if (airportSelect) {
      airportSelect.addEventListener("change", (e) => {
        state.filters.dep_airport = e.target.value;
        loadAllData();
      });
    }

    const aircraftSelect = document.getElementById("filter-aircraft");
    if (aircraftSelect) {
      aircraftSelect.addEventListener("change", (e) => {
        state.filters.aircraft_code = e.target.value;
        loadAllData();
      });
    }
  }

  function switchTab(tabName) {
    state.currentTab = tabName;

    document.querySelectorAll(".tab-btn").forEach((btn) => {
      btn.classList.toggle("active", btn.dataset.tab === tabName);
    });

    document.querySelectorAll(".tab-content").forEach((sec) => {
      sec.classList.toggle("active", sec.id === `tab-${tabName}`);
    });

    if (tabName === "map") {
      renderMap();
    }
  }

  // Init
  document.addEventListener("DOMContentLoaded", () => {
    const dbtLink = document.getElementById("dbt-docs-link");
    if (dbtLink && window.location.hostname) {
      dbtLink.href = `${window.location.protocol}//${window.location.hostname}:8085`;
    }
    setupEvents();
    loadAllData();
  });
})();
