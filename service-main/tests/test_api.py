import mongomock
import pytest
from fastapi.testclient import TestClient

from src.clients.mongo import get_db
from src.main import app


@pytest.fixture
def mock_mongo_db():
    client = mongomock.MongoClient()
    db = client["airlines"]
    return db


@pytest.fixture
def client(mock_mongo_db):
    app.dependency_overrides[get_db] = lambda: mock_mongo_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def test_api_root(client):
    res = client.get("/api")
    assert res.status_code == 200
    assert res.json() == {"message": "KTDL-Group20 API is running"}


def test_unknown_mart_404(client):
    res = client.get("/api/marts/unknown_nonexistent_mart")
    assert res.status_code == 404
    assert "not found" in res.json()["detail"].lower()


def test_empty_collections_return_empty(client):
    # Marts
    for mart in [
        "route_revenue",
        "route_pareto",
        "flight_occupancy",
        "fleet",
        "delay_heatmap",
        "delay_by_aircraft",
        "delay_by_route",
    ]:
        res = client.get(f"/api/marts/{mart}")
        assert res.status_code == 200
        assert res.json() == []

    # Airports
    res = client.get("/api/airports")
    assert res.status_code == 200
    assert res.json() == []

    # Routes
    res = client.get("/api/routes")
    assert res.status_code == 200
    assert res.json() == []

    # Runs
    res = client.get("/api/runs")
    assert res.status_code == 200
    assert res.json() == []

    # Summary with empty collections
    res = client.get("/api/summary")
    assert res.status_code == 200
    summary = res.json()
    assert summary["total_revenue"] == 0.0
    assert summary["total_flights"] == 0
    assert summary["total_delayed"] == 0
    assert summary["delay_rate"] == 0.0
    assert summary["avg_load_factor"] == 0.0
    assert summary["total_airports"] == 0
    assert summary["total_routes"] == 0
    assert summary["total_aircraft"] == 0
    assert summary["latest_run"] is None


def test_mart_filters_sort_limit_and_strip_run_id(client, mock_mongo_db):
    # Seed flight_occupancy docs with _run_id and different filter fields
    docs = [
        {
            "_id": "1",
            "flight_id": 1,
            "flight_no": "PG0001",
            "dep_airport": "SVO",
            "arr_airport": "LED",
            "aircraft_code": "773",
            "model": "Boeing 777-300",
            "month": "2017-06",
            "scheduled_departure": "2017-06-01T08:00:00Z",
            "boarded": 320,
            "seats": 400,
            "load_factor": 0.80,
            "_run_id": "run_01",
        },
        {
            "_id": "2",
            "flight_id": 2,
            "flight_no": "PG0002",
            "dep_airport": "SVO",
            "arr_airport": "LED",
            "aircraft_code": "773",
            "model": "Boeing 777-300",
            "month": "2017-07",
            "scheduled_departure": "2017-07-01T08:00:00Z",
            "boarded": 240,
            "seats": 400,
            "load_factor": 0.60,
            "_run_id": "run_01",
        },
        {
            "_id": "3",
            "flight_id": 3,
            "flight_no": "PG0003",
            "dep_airport": "DME",
            "arr_airport": "AER",
            "aircraft_code": "319",
            "model": "Airbus A319-100",
            "month": "2017-07",
            "scheduled_departure": "2017-07-02T10:00:00Z",
            "boarded": 108,
            "seats": 120,
            "load_factor": 0.90,
            "_run_id": "run_02",
        },
        {
            "_id": "4",
            "flight_id": 4,
            "flight_no": "PG0004",
            "dep_airport": "LED",
            "arr_airport": "KZN",
            "aircraft_code": "SU9",
            "model": "Sukhoi Superjet-100",
            "month": "2017-08",
            "scheduled_departure": "2017-08-03T12:00:00Z",
            "boarded": 45,
            "seats": 90,
            "load_factor": 0.50,
            "_run_id": "run_02",
        },
    ]
    mock_mongo_db["gold_flight_occupancy"].insert_many(docs)

    # 1. Filter: month
    res = client.get("/api/marts/flight_occupancy?month=2017-07")
    assert res.status_code == 200
    month_data = res.json()
    assert len(month_data) == 2
    assert {d["flight_id"] for d in month_data} == {2, 3}

    # 2. Filter: dep_airport
    res = client.get("/api/marts/flight_occupancy?dep_airport=SVO")
    assert res.status_code == 200
    dep_data = res.json()
    assert len(dep_data) == 2
    assert {d["flight_id"] for d in dep_data} == {1, 2}

    # 3. Filter: aircraft_code
    res = client.get("/api/marts/flight_occupancy?aircraft_code=319")
    assert res.status_code == 200
    ac_data = res.json()
    assert len(ac_data) == 1
    assert ac_data[0]["flight_id"] == 3

    # 4. Sort: -load_factor (descending)
    res = client.get("/api/marts/flight_occupancy?sort=-load_factor")
    assert res.status_code == 200
    desc_data = res.json()
    assert [d["load_factor"] for d in desc_data] == [0.90, 0.80, 0.60, 0.50]

    # 5. Sort: +load_factor (ascending)
    res = client.get("/api/marts/flight_occupancy?sort=load_factor")
    assert res.status_code == 200
    asc_data = res.json()
    assert [d["load_factor"] for d in asc_data] == [0.50, 0.60, 0.80, 0.90]

    # 6. Limit capping at 5000: passing 10000 is accepted and does not fail
    res = client.get("/api/marts/flight_occupancy?limit=10000")
    assert res.status_code == 200
    assert len(res.json()) == 4

    # 7. Verify _run_id is stripped from all responses
    for item in desc_data:
        assert "_run_id" not in item
        assert "flight_id" in item


def test_summary_kpis(client, mock_mongo_db):
    # Seed route pareto (revenue)
    mock_mongo_db["gold_route_pareto"].insert_many([
        {
            "_id": "SVO|LED",
            "dep_airport": "SVO",
            "arr_airport": "LED",
            "revenue": 2000000.0,
            "rank": 1,
            "cum_share": 0.6667,
            "_run_id": "run_01",
        },
        {
            "_id": "SVO|AER",
            "dep_airport": "SVO",
            "arr_airport": "AER",
            "revenue": 1000000.0,
            "rank": 2,
            "cum_share": 1.0,
            "_run_id": "run_01",
        },
    ])

    # Seed fleet
    mock_mongo_db["gold_fleet"].insert_many([
        {
            "_id": "773",
            "aircraft_code": "773",
            "model": "Boeing 777-300",
            "range": 11100,
            "flights": 50,
            "flight_hours": 120.0,
            "avg_duration_min": 144.0,
            "avg_load_factor": 0.75,
            "_run_id": "run_01",
        },
        {
            "_id": "319",
            "aircraft_code": "319",
            "model": "Airbus A319-100",
            "range": 6900,
            "flights": 30,
            "flight_hours": 60.0,
            "avg_duration_min": 120.0,
            "avg_load_factor": 0.65,
            "_run_id": "run_01",
        },
    ])

    # Seed delay by aircraft
    mock_mongo_db["gold_delay_by_aircraft"].insert_many([
        {
            "_id": "773",
            "aircraft_code": "773",
            "model": "Boeing 777-300",
            "flights": 50,
            "delayed": 5,
            "delay_rate": 0.10,
            "avg_delay_min": 25.0,
            "_run_id": "run_01",
        },
        {
            "_id": "319",
            "aircraft_code": "319",
            "model": "Airbus A319-100",
            "flights": 30,
            "delayed": 3,
            "delay_rate": 0.10,
            "avg_delay_min": 20.0,
            "_run_id": "run_01",
        },
    ])

    # Seed airports & routes
    mock_mongo_db["dim_airports"].insert_many([
        {"_id": "SVO", "airport_code": "SVO", "departures": 80, "_run_id": "run_01"},
        {"_id": "LED", "airport_code": "LED", "departures": 50, "_run_id": "run_01"},
        {"_id": "AER", "airport_code": "AER", "departures": 30, "_run_id": "run_01"},
    ])
    mock_mongo_db["dim_routes"].insert_many([
        {"_id": "SVO|LED", "flights": 50, "_run_id": "run_01"},
        {"_id": "SVO|AER", "flights": 30, "_run_id": "run_01"},
    ])

    # Seed pipeline_runs
    mock_mongo_db["pipeline_runs"].insert_one({
        "_id": "run_01",
        "run_id": "run_01",
        "cutoff": "2017-08-15T18:00:00Z",
        "started_at": "2017-08-15T18:01:00Z",
        "finished_at": "2017-08-15T18:05:00Z",
        "status": "success",
        "counts": {"bronze": {"flights": 80}, "silver": {"flights": 80}, "gold": {"fleet": 2}},
        "quarantine": 0,
        "_run_id": "run_01",
    })

    res = client.get("/api/summary")
    assert res.status_code == 200
    summary = res.json()

    assert summary["total_revenue"] == 3000000.0
    assert summary["total_flights"] == 80
    assert summary["total_delayed"] == 8
    assert summary["delay_rate"] == 0.10
    assert summary["avg_load_factor"] == 0.70
    assert summary["total_airports"] == 3
    assert summary["total_routes"] == 2
    assert summary["total_aircraft"] == 2
    assert summary["latest_run"] is not None
    assert summary["latest_run"]["run_id"] == "run_01"
    assert "_run_id" not in summary["latest_run"]


def test_dashboard_html_contains_tabs(client):
    res = client.get("/")
    assert res.status_code == 200
    assert "text/html" in res.headers.get("content-type", "")

    html_content = res.text
    required_tabs = ["Overview", "Map", "Delays", "Revenue", "Fleet", "Pipeline"]
    for tab in required_tabs:
        assert tab in html_content, f"Tab '{tab}' not found in dashboard HTML"
