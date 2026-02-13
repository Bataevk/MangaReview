from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from remanga_ods_utils import extract_top_titles, transform_to_rows, load_rows_to_postgres


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _extract(**_context):
    # >=100 items for each section: 5 pages * 20 items (API cap)
    return extract_top_titles(
        periods=("new", "monthly", "year"),
        sections=("new", "manga", "manhwa", "manhua", "comics"),
        tag="all",
        count=20,
        pages=5,
        sleep_seconds=0.2,
    )


def _transform(**context):
    ti = context["ti"]
    extracted = ti.xcom_pull(task_ids="extract_remanga_top") or []
    return transform_to_rows(extracted)


def _load(**context):
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="transform_to_rows") or []
    return load_rows_to_postgres(rows, postgres_conn_id="postgres_ods")


with DAG(
    dag_id="remanga_top_titles_to_postgres_ods",
    default_args=default_args,
    description="ETL: ReManga top titles -> ODS (PostgreSQL)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["remanga", "ods", "postgres"],
) as dag:
    extract_task = PythonOperator(
        task_id="extract_remanga_top",
        python_callable=_extract,
    )

    transform_task = PythonOperator(
        task_id="transform_to_rows",
        python_callable=_transform,
    )

    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=_load,
    )

    extract_task >> transform_task >> load_task

