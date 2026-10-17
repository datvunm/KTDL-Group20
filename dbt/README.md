# Airlines Lakehouse dbt Project

This directory contains the **dbt (data build tool)** project for the Airlines Medallion Lakehouse platform. It provides declarative data transformations, schema testing, and interactive column-level data lineage across Bronze, Silver, and Gold layers.

---

## Architecture & Lineage Graph

The dbt project mirrors the Iceberg medallion architecture on Spark / HDFS:

```
[Bronze Sources] (lake.bronze.*)
       │
       ├──> silver_airports
       ├──> silver_aircrafts
       ├──> silver_seats
       ├──> silver_bookings
       ├──> silver_tickets (SHA-256 PII masking)
       │       │
       │       ▼
       ├──> silver_flights_enriched ──┐
       │       │                      │
       ├──> silver_ticket_flights     │
       └──> silver_boarding_passes    │
               │                      │
               ▼                      ▼
        [Gold Analytical Marts & Dimensions] (lake.gold.*)
        ├── gold_route_revenue (Route × Month × Class)
        ├── gold_route_pareto (80/20 Revenue Concentration)
        ├── gold_flight_occupancy (Flight-level Load Factor)
        ├── gold_fleet (Aircraft Utilization & Hours)
        ├── gold_delay_heatmap (Day-of-Week × Departure Hour)
        ├── gold_delay_by_aircraft (Reliability by Model)
        ├── gold_delay_by_route (>= 20 Arrived Flights Threshold)
        ├── dim_airports (Airport Geodata & Departures)
        └── dim_routes (Top 100 Flight Route Corridors)
```

---

## Directory Structure

```
dbt/
├── dbt_project.yml          # Core dbt project definition
├── profiles.yml             # Connection profiles (Spark Thrift & local fallback)
├── Dockerfile               # Ultra-lightweight Nginx container for dbt docs UI
├── docker-compose.dev.yaml  # Docker Compose service exposing port 8085
├── nginx.conf               # Nginx server configuration with gzip enabled
├── generate_docs.sh         # Helper script to compile models & generate docs
├── serve_docs.sh            # Helper script to serve docs locally without Docker
├── models/
│   ├── sources.yml          # Raw Bronze sources (8 tables with tests & docs)
│   ├── silver/              # Conformed Silver models & data quality tests
│   │   ├── schema.yml
│   │   ├── silver_airports.sql
│   │   ├── silver_aircrafts.sql
│   │   ├── silver_seats.sql
│   │   ├── silver_bookings.sql
│   │   ├── silver_tickets.sql
│   │   ├── silver_flights_enriched.sql
│   │   ├── silver_ticket_flights.sql
│   │   ├── silver_boarding_passes.sql
│   │   └── silver_quarantine.sql
│   └── gold/                # Analytical Gold marts & dimensions
│       ├── schema.yml
│       ├── gold_route_revenue.sql
│       ├── gold_route_pareto.sql
│       ├── gold_flight_occupancy.sql
│       ├── gold_fleet.sql
│       ├── gold_delay_heatmap.sql
│       ├── gold_delay_by_aircraft.sql
│       ├── gold_delay_by_route.sql
│       ├── dim_airports.sql
│       └── dim_routes.sql
├── target/                  # Compiled artifacts & static docs site
│   ├── index.html           # Interactive dbt docs SPA
│   ├── manifest.json        # Complete lineage DAG graph & node definitions
│   └── catalog.json         # Table schemas & column metadata
└── tests/
    └── test_dbt_project.py  # Pytest suite validating models, DAG, and UI
```

---

## Quick Start

### 1. View Interactive Lineage in Docker Compose
The `dbt-docs` container is automatically started with `./start-all.sh`.
- Open **[http://localhost:8085](http://localhost:8085)** in your browser.
- Click the teal icon in the lower-right corner to open the **Lineage Graph (DAG)**.

### 2. Run Locally Without Docker
You can compile and serve documentation directly on your host machine:
```bash
# Generate documentation artifacts
./dbt/generate_docs.sh

# Serve locally on port 8085
./dbt/serve_docs.sh
```

### 3. Verify Models & Lineage with Pytest
```bash
pytest dbt/tests/test_dbt_project.py -v
```
