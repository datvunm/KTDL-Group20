from typing import Any
from fastapi import APIRouter, Depends, HTTPException, Query
from pymongo.database import Database

from src.clients.mongo import get_db

router = APIRouter(prefix="/api", tags=["marts"])

ALLOWED_MARTS = {
    "route_revenue": "gold_route_revenue",
    "route_pareto": "gold_route_pareto",
    "flight_occupancy": "gold_flight_occupancy",
    "fleet": "gold_fleet",
    "delay_heatmap": "gold_delay_heatmap",
    "delay_by_aircraft": "gold_delay_by_aircraft",
    "delay_by_route": "gold_delay_by_route",
}


def parse_sort_param(sort: str | None) -> list[tuple[str, int]]:
    if not sort:
        return []
    sort_fields: list[tuple[str, int]] = []
    for item in sort.split(","):
        item = item.strip()
        if not item:
            continue
        if item.startswith("-"):
            sort_fields.append((item[1:], -1))
        elif item.startswith("+"):
            sort_fields.append((item[1:], 1))
        else:
            sort_fields.append((item, 1))
    return sort_fields


def sanitize_doc(doc: dict[str, Any]) -> dict[str, Any]:
    doc.pop("_run_id", None)
    if "_id" in doc and not isinstance(doc["_id"], (str, int, float, bool)):
        doc["_id"] = str(doc["_id"])
    return doc


@router.get("/marts/{name}")
def get_mart(
    name: str,
    month: str | None = None,
    dep_airport: str | None = None,
    aircraft_code: str | None = None,
    limit: int = Query(default=500, description="Max records to return, capped at 5000"),
    sort: str | None = None,
    db: Database[Any] = Depends(get_db),
) -> list[dict[str, Any]]:
    if name not in ALLOWED_MARTS:
        raise HTTPException(
            status_code=404,
            detail=f"Mart '{name}' not found. Allowed marts: {', '.join(sorted(ALLOWED_MARTS.keys()))}",
        )

    collection_name = ALLOWED_MARTS[name]
    coll = db[collection_name]

    # Filters
    query: dict[str, Any] = {}
    if month is not None:
        query["month"] = month
    if dep_airport is not None:
        query["dep_airport"] = dep_airport
    if aircraft_code is not None:
        query["aircraft_code"] = aircraft_code

    # Cap limit at 5000
    capped_limit = min(max(1, limit), 5000)

    # Sort
    sort_spec = parse_sort_param(sort)

    cursor = coll.find(query, projection={"_run_id": 0})
    if sort_spec:
        cursor = cursor.sort(sort_spec)
    cursor = cursor.limit(capped_limit)

    return [sanitize_doc(doc) for doc in cursor]


@router.get("/airports")
def get_airports(
    limit: int = Query(default=500, description="Max records to return, capped at 5000"),
    sort: str | None = None,
    db: Database[Any] = Depends(get_db),
) -> list[dict[str, Any]]:
    coll = db["dim_airports"]
    capped_limit = min(max(1, limit), 5000)
    sort_spec = parse_sort_param(sort) or [("departures", -1)]

    cursor = coll.find({}, projection={"_run_id": 0}).sort(sort_spec).limit(capped_limit)
    return [sanitize_doc(doc) for doc in cursor]


@router.get("/routes")
def get_routes(
    limit: int = Query(default=500, description="Max records to return, capped at 5000"),
    sort: str | None = None,
    db: Database[Any] = Depends(get_db),
) -> list[dict[str, Any]]:
    coll = db["dim_routes"]
    capped_limit = min(max(1, limit), 5000)
    sort_spec = parse_sort_param(sort) or [("flights", -1)]

    cursor = coll.find({}, projection={"_run_id": 0}).sort(sort_spec).limit(capped_limit)
    return [sanitize_doc(doc) for doc in cursor]


@router.get("/runs")
def get_runs(
    limit: int = Query(default=20, le=100, description="Max runs to return, default 20"),
    db: Database[Any] = Depends(get_db),
) -> list[dict[str, Any]]:
    coll = db["pipeline_runs"]
    capped_limit = min(max(1, limit), 100)
    cursor = (
        coll.find({}, projection={"_run_id": 0})
        .sort([("finished_at", -1), ("started_at", -1), ("_id", -1)])
        .limit(capped_limit)
    )
    return [sanitize_doc(doc) for doc in cursor]


@router.get("/summary")
def get_summary(db: Database[Any] = Depends(get_db)) -> dict[str, Any]:
    # 1. Total revenue
    total_revenue = 0.0
    pareto_pipe = [{"$group": {"_id": None, "total": {"$sum": "$revenue"}}}]
    pareto_agg = list(db["gold_route_pareto"].aggregate(pareto_pipe))
    if pareto_agg and pareto_agg[0].get("total") is not None:
        total_revenue = float(pareto_agg[0]["total"])
    else:
        # fallback to gold_route_revenue if pareto not populated
        rev_agg = list(db["gold_route_revenue"].aggregate(pareto_pipe))
        if rev_agg and rev_agg[0].get("total") is not None:
            total_revenue = float(rev_agg[0]["total"])

    # 2. Total arrived flights and delayed flights (Arrived flights only per contract)
    total_flights = 0
    total_delayed = 0
    delay_pipe = [
        {
            "$group": {
                "_id": None,
                "delayed": {"$sum": "$delayed"},
                "flights": {"$sum": "$flights"},
            }
        }
    ]
    delay_agg = list(db["gold_delay_by_aircraft"].aggregate(delay_pipe))
    if delay_agg and delay_agg[0].get("flights") is not None:
        total_flights = int(delay_agg[0]["flights"])
        if delay_agg[0].get("delayed") is not None:
            total_delayed = int(delay_agg[0]["delayed"])
    else:
        heatmap_agg = list(db["gold_delay_heatmap"].aggregate(delay_pipe))
        if heatmap_agg and heatmap_agg[0].get("flights") is not None:
            total_flights = int(heatmap_agg[0]["flights"])
            if heatmap_agg[0].get("delayed") is not None:
                total_delayed = int(heatmap_agg[0]["delayed"])

    # 3. Fleet load factor & fallback for flights if delay marts were empty
    fleet_pipe = [
        {
            "$group": {
                "_id": None,
                "total_flights": {"$sum": "$flights"},
                "avg_lf": {"$avg": "$avg_load_factor"},
            }
        }
    ]
    fleet_agg = list(db["gold_fleet"].aggregate(fleet_pipe))
    avg_load_factor = 0.0
    if fleet_agg:
        if total_flights == 0 and fleet_agg[0].get("total_flights") is not None:
            total_flights = int(fleet_agg[0]["total_flights"])
        if fleet_agg[0].get("avg_lf") is not None:
            avg_load_factor = round(float(fleet_agg[0]["avg_lf"]), 4)

    # If load factor not computed from gold_fleet, check gold_flight_occupancy
    if avg_load_factor == 0.0:
        occ_pipe = [{"$group": {"_id": None, "avg_lf": {"$avg": "$load_factor"}}}]
        occ_agg = list(db["gold_flight_occupancy"].aggregate(occ_pipe))
        if occ_agg and occ_agg[0].get("avg_lf") is not None:
            avg_load_factor = round(float(occ_agg[0]["avg_lf"]), 4)

    delay_rate = round(total_delayed / total_flights, 4) if total_flights > 0 else 0.0

    # 3. Counts of dimensions / entities
    total_airports = db["dim_airports"].count_documents({})
    total_routes = db["dim_routes"].count_documents({})
    if total_routes == 0:
        total_routes = db["gold_route_pareto"].count_documents({})
    total_aircraft = db["gold_fleet"].count_documents({})
    if total_aircraft == 0:
        total_aircraft = db["gold_delay_by_aircraft"].count_documents({})

    # 4. Latest run info
    latest_run_doc = db["pipeline_runs"].find_one(
        {},
        projection={"_run_id": 0},
        sort=[("finished_at", -1), ("started_at", -1), ("_id", -1)],
    )
    latest_run = sanitize_doc(latest_run_doc) if latest_run_doc else None

    return {
        "total_revenue": total_revenue,
        "total_flights": total_flights,
        "total_delayed": total_delayed,
        "delay_rate": delay_rate,
        "avg_load_factor": avg_load_factor,
        "total_airports": total_airports,
        "total_routes": total_routes,
        "total_aircraft": total_aircraft,
        "latest_run": latest_run,
    }
