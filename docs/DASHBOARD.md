# Dashboard Walkthrough & Business Intelligence

What each tab of the dashboard shows and where the numbers come from. See [PIPELINE.md](PIPELINE.md) for how the underlying Gold marts are built.

`http://localhost:8005`

## Table of Contents

- [Dashboard Walkthrough \& Business Intelligence](#dashboard-walkthrough--business-intelligence)
  - [Table of Contents](#table-of-contents)
  - [Tab 1: Overview — Executive KPIs](#tab-1-overview--executive-kpis)
  - [Tab 2: Map — Geospatial Network \& Route Corridors](#tab-2-map--geospatial-network--route-corridors)
  - [Tab 3: Delay Analysis — Operational Bottlenecks](#tab-3-delay-analysis--operational-bottlenecks)
  - [Tab 4: Revenue Analysis — Financial Concentration](#tab-4-revenue-analysis--financial-concentration)
  - [Tab 5: Fleet \& Utilization — Asset Efficiency](#tab-5-fleet--utilization--asset-efficiency)
  - [Tab 6: Pipeline — Data Refresh History](#tab-6-pipeline--data-refresh-history)
  - [Benchmark Acceptance Metrics at Final Cutoff](#benchmark-acceptance-metrics-at-final-cutoff)

---

## Tab 1: Overview — Executive KPIs

| KPI | Value | Source |
|---|---|---|
| Total Revenue | 47.0B ₽ | `sum(revenue)` from `gold_route_pareto` |
| Total Flights | 49,235 | `sum(flights)` from `gold_delay_by_aircraft` (Arrived only) |
| Delayed Flights | 2,394 (4.9%) | `sum(delayed)` from `gold_delay_by_aircraft` |
| Avg Load Factor | 42.8% | avg of `avg_load_factor` across `gold_fleet` |
| Active Fleet | 9 models | row count of `gold_fleet` |
| Airports Network | 104 | row count of `dim_airports` |

Header also shows a data-freshness badge: when the Gold marts were last refreshed.

Also on this tab: a **Live Bookings** panel - bookings and revenue in the last 20 minutes, polled every 5s from `GET /api/live/bookings`, fed by the streaming layer (see [PIPELINE.md](PIPELINE.md#streaming-speed-layer-kafka--spark-structured-streaming)).

---

## Tab 2: Map — Geospatial Network & Route Corridors

Leaflet map of the network.

- **Airports**: circle markers at WGS84 coordinates, radius ∝ √(departures). Major hubs: SVO, DME, VKO (Moscow), LED (St. Petersburg), OVB (Novosibirsk).
- **Routes**: top 100 routes from `dim_routes`, drawn as great-circle polylines; thickness/opacity scale with volume.
- Clicking a marker shows airport code, names, city, timezone, departures.

---

## Tab 3: Delay Analysis — Operational Bottlenecks

1. **Delay heatmap** (7 × 24 grid, day-of-week × local departure hour): cell color scales from green (low delay rate) through amber to red (high).
2. **Delay by aircraft model** (bar chart): delay rate % and avg delay minutes per model.
3. **Top 20 most delayed routes** (table): directional city pairs, `HAVING count(*) >= 20` arrived flights for a minimum sample size. Worst in this dataset: Voronezh (VOZ) → Pulkovo (LED), 11.1% delay rate (10/90 flights).

---

## Tab 4: Revenue Analysis — Financial Concentration

1. **Route Pareto chart** (dual-axis): bars = route revenue, line = cumulative % of total. Of 457 revenue-generating routes, the top 39 account for 50% of total revenue (47.0B ₽).
2. **Revenue by fare class** (doughnut): Economy ≈ 71% of revenue; Comfort is offered only on select wide-body long-haul aircraft (Boeing 777-300).
3. **Monthly revenue trend** (line): May-September 2017 (May/September partial), peaking in June-August.

---

## Tab 5: Fleet & Utilization — Asset Efficiency

1. **Load factor by aircraft model** (bar chart):
   - Boeing 777-300 (`773`): 72.8% - wide-body on high-density long routes.
   - Boeing 737-300 (`733`): 63.0%; Airbus A321-200 (`321`): 37.7% - narrow-body trunk routes.
   - Sukhoi Superjet 100 (`SU9`): 53.7% - regional jet.
   - Cessna 208 (`CN1`): 16.0% - 12-seat aircraft on low-volume regional routes.
2. **Seat configuration by cabin** (stacked bar):
   - Boeing 777-300: 324 economy / 30 business / 48 comfort (402 total).
   - Boeing 767-300: 192 economy / 30 business (222 total).
   - Airbus A319: 96 economy / 20 business (116 total).
   - Cessna 208: 12 economy only.
3. **Fleet table**: aircraft code, model, range (km), total seats, flights operated, flight hours, avg duration, avg load factor.

---

## Tab 6: Pipeline — Data Refresh History

**Data refresh runs** (last 20, from `pipeline_runs`): run id, data as-of cutoff, started/finished (UTC), status badge, bronze/silver/gold row counts, and anomalies caught (0 = clean run).

---

## Benchmark Acceptance Metrics at Final Cutoff

At the final archive cutoff (`2017-08-15 18:00:00+03`):

| Dimension / Metric | Benchmark Value | Verification Query / Endpoint |
|---|---|---|
| Data integrity invariant | All 3 `raw` tables equal `archive` (`EXCEPT ALL` both ways = 0 rows) | `source/simulate.sh` output & Postgres verification |
| Arrived flight volume | 49,235 | `GET /api/summary` → `total_flights` |
| Delayed flight volume | 2,394 (>15 min departure delay) | `GET /api/summary` → `total_delayed` |
| Overall delay rate | 4.86% (2,394 / 49,235) | `GET /api/summary` → `delay_rate` |
| Gross network revenue | 47,004,388,200 ₽ (≈47.0B ₽) | `GET /api/summary` → `total_revenue` |
| Revenue routes | 457 distinct directional city-pair routes | `GET /api/marts/route_pareto` record count |
| Pareto 50% concentration | Top 39 routes = 50.0% of revenue | `gold_route_pareto` where `cum_share <= 0.50` |
| Pareto 80% concentration | Top 129 routes = 80.0% of revenue | `gold_route_pareto` where `cum_share <= 0.80` |
| Boeing 777-300 load factor | 72.8% | `GET /api/marts/fleet?aircraft_code=773` |
| Cessna 208 load factor | 16.0% | `GET /api/marts/fleet?aircraft_code=CN1` |
| Most delayed route | Voronezh (VOZ) → Pulkovo (LED): 11.1% (10/90 flights) | `GET /api/marts/delay_by_route` |
| Active airports | 104 | `GET /api/airports` record count |
| Active fleet models | 9 (8 with arrived flights; Airbus A320-200 has none) | `GET /api/marts/fleet` record count |
| Quarantine anomalies | 0 in a clean baseline run | `GET /api/runs` → `quarantine: 0` |
