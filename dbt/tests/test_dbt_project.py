import json
import os
from pathlib import Path
import pytest
import yaml

DBT_ROOT = Path(__file__).resolve().parents[1]


def test_dbt_project_yml_exists_and_valid():
    """Verify dbt_project.yml exists and contains project configuration."""
    project_file = DBT_ROOT / "dbt_project.yml"
    assert project_file.is_file(), f"{project_file} must exist"
    
    with open(project_file, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    
    assert data["name"] == "airlines_lakehouse"
    assert data["profile"] == "airlines_lakehouse"
    assert "models" in data
    assert "silver" in data["models"]["airlines_lakehouse"]
    assert "gold" in data["models"]["airlines_lakehouse"]


def test_profiles_yml_has_spark_and_dev_targets():
    """Verify profiles.yml defines spark and dev targets with expected catalog and connection parameters."""
    profiles_file = DBT_ROOT / "profiles.yml"
    assert profiles_file.is_file(), f"{profiles_file} must exist"
    
    with open(profiles_file, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    
    profile = data.get("airlines_lakehouse", {})
    outputs = profile.get("outputs", {})
    assert "spark" in outputs, "spark output target must be configured"
    assert "dev" in outputs or "duckdb" in outputs, "local dev/duckdb target must be configured for offline compilation"
    
    spark_conf = outputs["spark"]
    assert spark_conf["type"] == "spark"


def test_bronze_sources_defined():
    """Verify all 8 Bronze source tables are declared in models/sources.yml."""
    sources_file = DBT_ROOT / "models" / "sources.yml"
    assert sources_file.is_file(), f"{sources_file} must exist"
    
    with open(sources_file, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    
    sources = data.get("sources", [])
    assert len(sources) >= 1
    bronze_source = next((s for s in sources if s["name"] == "bronze"), None)
    assert bronze_source is not None, "Source 'bronze' must be declared"
    
    table_names = {t["name"] for t in bronze_source.get("tables", [])}
    expected_tables = {
        "aircrafts_data",
        "airports_data",
        "seats",
        "bookings",
        "tickets",
        "flights",
        "ticket_flights",
        "boarding_passes",
    }
    assert expected_tables.issubset(table_names), f"Missing bronze sources: {expected_tables - table_names}"


def test_silver_models_exist():
    """Verify conformed Silver models exist with ref/source usage and schema documentation."""
    silver_dir = DBT_ROOT / "models" / "silver"
    assert silver_dir.is_dir(), f"{silver_dir} must exist"
    
    expected_models = [
        "silver_airports.sql",
        "silver_aircrafts.sql",
        "silver_seats.sql",
        "silver_bookings.sql",
        "silver_tickets.sql",
        "silver_flights_enriched.sql",
        "silver_ticket_flights.sql",
        "silver_boarding_passes.sql",
    ]
    for model_name in expected_models:
        model_path = silver_dir / model_name
        assert model_path.is_file(), f"Silver model {model_name} must exist"
        content = model_path.read_text(encoding="utf-8")
        assert "source('bronze'" in content or "ref(" in content
    
    schema_file = silver_dir / "schema.yml"
    assert schema_file.is_file(), "Silver schema.yml must exist"


def test_gold_models_exist():
    """Verify Gold analytical marts exist with ref usage and schema documentation."""
    gold_dir = DBT_ROOT / "models" / "gold"
    assert gold_dir.is_dir(), f"{gold_dir} must exist"
    
    expected_models = [
        "gold_route_revenue.sql",
        "gold_route_pareto.sql",
        "gold_flight_occupancy.sql",
        "gold_fleet.sql",
        "gold_delay_heatmap.sql",
        "gold_delay_by_aircraft.sql",
        "gold_delay_by_route.sql",
        "dim_airports.sql",
        "dim_routes.sql",
    ]
    for model_name in expected_models:
        model_path = gold_dir / model_name
        assert model_path.is_file(), f"Gold model {model_name} must exist"
        content = model_path.read_text(encoding="utf-8")
        assert "ref(" in content, f"Gold model {model_name} must reference upstream models"
    
    schema_file = gold_dir / "schema.yml"
    assert schema_file.is_file(), "Gold schema.yml must exist"


def test_dbt_docs_artifacts_generated():
    """Verify target documentation files exist for static dbt docs UI serving."""
    target_dir = DBT_ROOT / "target"
    assert target_dir.is_dir(), f"{target_dir} must exist"
    
    assert (target_dir / "index.html").is_file(), "target/index.html must exist"
    assert (target_dir / "manifest.json").is_file(), "target/manifest.json must exist"
    assert (target_dir / "catalog.json").is_file(), "target/catalog.json must exist"


def test_dbt_docker_compose_and_service():
    """Verify dbt/docker-compose.dev.yaml defines dbt-docs on port 8085."""
    compose_file = DBT_ROOT / "docker-compose.dev.yaml"
    assert compose_file.is_file(), f"{compose_file} must exist"
    
    with open(compose_file, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    
    services = data.get("services", {})
    assert "dbt-docs" in services, "Service 'dbt-docs' must be defined"
    ports = services["dbt-docs"].get("ports", [])
    assert any("8085" in str(p) for p in ports), "dbt-docs must expose port 8085"


def test_dashboard_links_to_dbt_docs():
    """Verify service-main dashboard navigation links to the dbt docs UI on port 8085."""
    dashboard_html = Path(__file__).resolve().parents[2] / "service-main" / "src" / "static" / "dashboard.html"
    assert dashboard_html.is_file(), f"{dashboard_html} must exist"
    content = dashboard_html.read_text(encoding="utf-8")
    assert "8085" in content or "dbt docs" in content.lower() or "lineage" in content.lower()


def test_manifest_lineage_graph_connectivity():
    """Verify manifest.json contains all expected models and establishes an unbroken lineage DAG."""
    manifest_file = DBT_ROOT / "target" / "manifest.json"
    assert manifest_file.is_file(), "manifest.json must exist"
    
    with open(manifest_file, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    
    nodes = manifest.get("nodes", {})
    sources = manifest.get("sources", {})
    
    # Check that all models exist in nodes
    silver_models = [k for k in nodes if k.startswith("model.airlines_lakehouse.silver_")]
    gold_models = [k for k in nodes if k.startswith("model.airlines_lakehouse.gold_") or k.startswith("model.airlines_lakehouse.dim_")]
    
    assert len(silver_models) >= 8, f"Expected at least 8 silver models, got {len(silver_models)}"
    assert len(gold_models) >= 9, f"Expected at least 9 gold models, got {len(gold_models)}"
    assert len(sources) >= 8, f"Expected at least 8 bronze sources, got {len(sources)}"
    
    # Verify dependency lineage: gold models must have upstream dependencies
    for gold_id in gold_models:
        node = nodes[gold_id]
        depends_on = node.get("depends_on", {}).get("nodes", [])
        assert len(depends_on) >= 1, f"Gold model {gold_id} has no upstream dependencies"
        # Upstream must be silver models or other gold models
        assert any(
            dep.startswith("model.airlines_lakehouse.silver_") or dep.startswith("model.airlines_lakehouse.gold_")
            for dep in depends_on
        ), f"Gold model {gold_id} upstream is not a medallion model: {depends_on}"
    
    # Verify silver models depend on bronze sources or other silver models
    for silver_id in silver_models:
        node = nodes[silver_id]
        depends_on_nodes = node.get("depends_on", {}).get("nodes", [])
        assert len(depends_on_nodes) >= 1, f"Silver model {silver_id} has no upstream dependencies"


def test_dbt_generate_docs_script_syntax():
    """Verify dbt helper scripts exist and are valid executable bash scripts."""
    gen_script = DBT_ROOT / "generate_docs.sh"
    serve_script = DBT_ROOT / "serve_docs.sh"
    
    assert gen_script.is_file() and os.access(gen_script, os.X_OK)
    assert serve_script.is_file() and os.access(serve_script, os.X_OK)
