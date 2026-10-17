# service-main

FastAPI dashboard and Lakehouse Marts API for KTDL-Group20 Airlines Lakehouse.

## Features

- **Dashboard UI** served at `/` with six interactive tabs:
  1. **Overview**: Key platform KPIs (Total Revenue, Flights, Delays, Avg Load Factor, Fleet, Airports, and pipeline status).
  2. **Map**: Leaflet map plotting airports (circle markers scaled by departure volume) and the top 100 flight routes.
  3. **Delays**: Day-of-week &times; hour-of-day delay rate heatmap grid, by-aircraft delay rates and durations, and top-20 most delayed routes table.
  4. **Revenue**: Route Pareto chart (Top 50 routes by revenue + cumulative share line), fare-class revenue doughnut chart, and monthly revenue trend line.
  5. **Fleet**: Average load factor by aircraft model, seat configuration stacked bar chart (Economy / Comfort / Business), and fleet operational utilization table.
  6. **Pipeline**: Execution history table tracking Airflow medallion runs (`pipeline_runs`) with per-layer counts (Bronze, Silver, Gold) and quarantine records.
  - Interactive filters by Month, Departure Airport, and Aircraft Code.
  - Chart.js and Leaflet loaded via CDN.
  - Friendly empty state when collections have no data yet.

- **Marts API** served under `/api`:
  - `GET /api`: API status.
  - `GET /api/marts/{name}`: Gold mart query endpoint (`name` &in; `route_revenue`, `route_pareto`, `flight_occupancy`, `fleet`, `delay_heatmap`, `delay_by_aircraft`, `delay_by_route`). Supports filters (`month`, `dep_airport`, `aircraft_code`), sorting (`sort` with `-` prefix for desc), pagination limit (capped at 5000), and automatic stripping of internal `_run_id`.
  - `GET /api/airports`: Airport dimension from `dim_airports`.
  - `GET /api/routes`: Top 100 route dimension from `dim_routes`.
  - `GET /api/runs`: Latest 20 medallion pipeline runs from `pipeline_runs`.
  - `GET /api/summary`: Aggregated executive KPIs.

- **Health Checks** at `/health` (`/health/postgres`, `/health/mongo`, `/health/hdfs`, `/health/all`).

## Environment Variables

Configured via environment or `.env` file:

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_HOST` | `db` | PostgreSQL host |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_USER` | `postgres` | PostgreSQL username |
| `POSTGRES_PASSWORD` | `123456` | PostgreSQL password |
| `POSTGRES_DB` | `postgres` | PostgreSQL database name |
| `MONGO_HOST` | `mongo` | MongoDB host |
| `MONGO_PORT` | `27017` | MongoDB port |
| `MONGO_USER` | `root` | MongoDB root username |
| `MONGO_PASSWORD` | `123456` | MongoDB root password |
| `MONGO_DB` | `airlines` | MongoDB database name |
| `HDFS_URL` | `http://namenode:9870` | HDFS WebHDFS URL |
| `HDFS_USER` | `root` | HDFS user |

## Run

### Local development

```bash
uv sync
uv run uvicorn src.main:app --reload --host 0.0.0.0 --port 8000
```

- Dashboard: `http://localhost:8000/`
- API Root: `http://localhost:8000/api`
- OpenAPI Docs: `http://localhost:8000/docs`

### Tests

```bash
pytest service-main/tests/test_api.py -v
```
