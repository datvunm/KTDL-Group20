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

    tasks = ["check_source", "bronze", "silver", "gold", "publish"]
    for task_id in tasks:
        assert dag.has_task(task_id), f"Missing task {task_id}"
    assert sorted(dag.task_ids) == sorted(tasks)

    # Verify linear dependency chain:
    # check_source -> bronze -> silver -> gold -> publish
    assert dag.get_task("bronze") in dag.get_task("check_source").downstream_list
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


def test_spark_submit_hook_master_resolution(monkeypatch):
    contract_conn = '{"conn_type":"spark","host":"spark://spark","port":7077,"extra":{"deploy-mode":"client"}}'
    monkeypatch.setenv("AIRFLOW_CONN_SPARK_DEFAULT", contract_conn)

    from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook

    hook = SparkSubmitHook(conn_id="spark_default")
    assert hook._connection["master"] == "spark://spark:7077"
