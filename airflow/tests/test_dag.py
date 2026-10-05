import os
import re
from pathlib import Path
import pytest
from airflow.models import DagBag

DAGS_FOLDER = str(Path(__file__).resolve().parent.parent / "dags")


@pytest.fixture(scope="session", autouse=True)
def setup_airflow_env():
    """Ensure Airflow DB is initialized for testing."""
    from airflow.utils import db

    db.initdb()


def test_dagbag_no_import_errors():
    dagbag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
    assert len(dagbag.import_errors) == 0, f"DagBag import errors: {dagbag.import_errors}"
    assert "airlines_medallion" in dagbag.dags
    dag = dagbag.dags["airlines_medallion"]
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_task_order():
    dagbag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
    dag = dagbag.dags.get("airlines_medallion") or dagbag.get_dag("airlines_medallion")
    assert dag is not None

    tasks = ["check_source", "prepare_raw_source", "bronze", "silver", "gold", "publish"]
    for task_id in tasks:
        assert dag.has_task(task_id), f"Missing task {task_id}"

    # Verify linear dependency chain:
    # check_source -> prepare_raw_source -> bronze -> silver -> gold -> publish
    assert dag.get_task("prepare_raw_source") in dag.get_task("check_source").downstream_list
    assert dag.get_task("bronze") in dag.get_task("prepare_raw_source").downstream_list
    assert dag.get_task("silver") in dag.get_task("bronze").downstream_list
    assert dag.get_task("gold") in dag.get_task("silver").downstream_list
    assert dag.get_task("publish") in dag.get_task("gold").downstream_list

    # Ensure no unintended upstream/downstream connections
    assert dag.get_task("check_source").upstream_list == []
    assert dag.get_task("publish").downstream_list == []


def test_bronze_rendered_application_args_sanitization():
    dagbag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
    dag = dagbag.dags.get("airlines_medallion") or dagbag.get_dag("airlines_medallion")
    bronze_task = dag.get_task("bronze")

    raw_run_id = "manual__2026-10-04T10:00:00+00:00"
    context = {
        "dag": dag,
        "run_id": raw_run_id,
    }
    rendered_args = bronze_task.render_template(bronze_task.application_args, context)
    assert rendered_args[0] == "--run-id"
    sanitized_id = rendered_args[1]

    # Verify sanitized run_id contains strictly [A-Za-z0-9_]
    assert re.fullmatch(r"[A-Za-z0-9_]+", sanitized_id) is not None
    assert "-" not in sanitized_id
    assert ":" not in sanitized_id
    assert "+" not in sanitized_id
    assert sanitized_id == "manual__2026_10_04T10_00_00_00_00"


def test_raw_source_sql_splits_into_ddl_and_checks():
    import sys

    sys.path.insert(0, DAGS_FOLDER)
    from airlines_medallion import split_sql_statements

    sql_path = Path(__file__).resolve().parents[2] / "pipeline" / "sql" / "bronze" / "airline_data_source.sql"
    statements = split_sql_statements(sql_path.read_text(encoding="utf-8"))
    heads = [s.split()[0].upper() for s in statements]

    # 3 x (DROP, CREATE), 3 validation SELECTs, 1 recovery WITH; export lines are comments
    assert heads == ["DROP", "CREATE"] * 3 + ["SELECT"] * 3 + ["WITH"]
    assert all(not s.endswith(";") for s in statements)


def test_raw_validation_checks():
    import sys

    sys.path.insert(0, DAGS_FOLDER)
    from airlines_medallion import check_raw_validation

    check_raw_validation(["ticket_flights", "booked_rows", "empty_seat_rows"], [(10, 10, 5)])
    check_raw_validation(["airports_src", "airports_raw", "seats_src", "seats_raw"], [(3, 3, 7, 7)])
    check_raw_validation(["missing_dep_airport", "missing_arr_airport", "missing_seat"], [(0, 0, 0)])
    check_raw_validation(["table_name", "lost", "extra"], [("bookings", 0, 0), ("seats", 0, 0)])

    with pytest.raises(ValueError):
        check_raw_validation(["ticket_flights", "booked_rows", "empty_seat_rows"], [(10, 9, 5)])
    with pytest.raises(ValueError):
        check_raw_validation(["airports_src", "airports_raw", "seats_src", "seats_raw"], [(3, 2, 7, 7)])
    with pytest.raises(ValueError):
        check_raw_validation(["missing_dep_airport", "missing_arr_airport", "missing_seat"], [(0, 1, 0)])
    with pytest.raises(ValueError):
        check_raw_validation(["table_name", "lost", "extra"], [("bookings", 0, 2)])


def test_spark_submit_hook_master_resolution(monkeypatch):
    contract_conn = '{"conn_type":"spark","host":"spark://spark","port":7077,"extra":{"deploy-mode":"client"}}'
    monkeypatch.setenv("AIRFLOW_CONN_SPARK_DEFAULT", contract_conn)

    from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook

    hook = SparkSubmitHook(conn_id="spark_default")
    assert hook._connection["master"] == "spark://spark:7077"
