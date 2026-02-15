import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

ODS_TABLE = "ods_remanga_titles_top_pg"

def _get_latest_batch_ts(**_context) -> str:
    ods = PostgresHook(postgres_conn_id="postgres_ods")
    latest_batch = ods.get_first(f"SELECT MAX(fetched_at) FROM {ODS_TABLE}")
    batch_ts = latest_batch[0] if latest_batch else None
    if batch_ts is None:
        raise ValueError("DQ: ODS is empty (no batch_ts)")
    return batch_ts.isoformat() if hasattr(batch_ts, "isoformat") else str(batch_ts)


def _dq_checks(**_context) -> None:
    ods = PostgresHook(postgres_conn_id="postgres_ods")
    dds = PostgresHook(postgres_conn_id="postgres_dds")

    latest_batch = ods.get_first(f"SELECT MAX(fetched_at) FROM {ODS_TABLE}")
    batch_ts = latest_batch[0] if latest_batch else None
    if batch_ts is None:
        raise ValueError("DQ: ODS is empty")

    # 1) Volume checks: expect >=100 per (period, section) for latest batch
    groups = ods.get_records(
        f"""
        SELECT period, COALESCE(section,'unknown') AS section, COUNT(*) AS cnt
        FROM {ODS_TABLE}
        WHERE fetched_at = %s
        GROUP BY period, COALESCE(section,'unknown')
        ORDER BY period, section;
        """,
        parameters=(batch_ts,),
    )
    if not groups:
        raise ValueError(f"DQ: no rows for batch {batch_ts}")

    min_cnt = min(int(r[2]) for r in groups)
    if min_cnt < 100:
        raise ValueError(f"DQ: expected >=100 rows per (period,section), got min={min_cnt}, batch={batch_ts}")

    # 2) Null / range checks (ODS)
    null_titles = ods.get_first(
        f"SELECT COUNT(*) FROM {ODS_TABLE} WHERE fetched_at=%s AND title_id IS NULL;",
        parameters=(batch_ts,),
    )[0]
    if int(null_titles) > 0:
        raise ValueError(f"DQ: null title_id rows={null_titles} for batch={batch_ts}")

    bad_rating = ods.get_first(
        f"""
        SELECT COUNT(*)
        FROM {ODS_TABLE}
        WHERE fetched_at=%s
          AND rating IS NOT NULL
          AND (rating < 0 OR rating > 10);
        """,
        parameters=(batch_ts,),
    )[0]
    if int(bad_rating) > 0:
        raise ValueError(f"DQ: bad rating rows={bad_rating} for batch={batch_ts}")

    # 3) Consistency: facts should exist for latest batch
    ods_cnt = ods.get_first(f"SELECT COUNT(*) FROM {ODS_TABLE} WHERE fetched_at=%s;", parameters=(batch_ts,))[0]
    fct_cnt = dds.get_first(
        "SELECT COUNT(*) FROM dds.fct_top_titles_snapshot WHERE batch_ts=%s;",
        parameters=(batch_ts,),
    )[0]

    if int(fct_cnt) < int(ods_cnt) * 0.9:
        raise ValueError(f"DQ: too few fact rows. ods={ods_cnt} facts={fct_cnt} batch={batch_ts}")

    logging.info("DQ OK for batch=%s: groups=%d ods=%s facts=%s", batch_ts, len(groups), ods_cnt, fct_cnt)


with DAG(
    dag_id="remanga_dds_data_quality_postgres",
    default_args=DEFAULT_ARGS,
    description="DDS: data quality checks (ODS vs DDS)",
    start_date=datetime(2025, 1, 1),
    # Runs periodically; dependencies handled by SqlSensor.
    schedule_interval="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["remanga", "dds", "dq", "postgres"],
) as dag:
    get_batch = PythonOperator(
        task_id="get_latest_batch_ts",
        python_callable=_get_latest_batch_ts,
    )

    wait_for_fact_table = SqlSensor(
        task_id="wait_for_fact_table",
        conn_id="postgres_dds",
        sql="SELECT 1 WHERE to_regclass('dds.fct_top_titles_snapshot') IS NOT NULL;",
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60,
    )

    wait_for_fact_batch = SqlSensor(
        task_id="wait_for_fact_batch",
        conn_id="postgres_dds",
        sql=(
            "SELECT 1 FROM dds.fct_top_titles_snapshot "
            "WHERE batch_ts = '{{ ti.xcom_pull(task_ids=\"get_latest_batch_ts\") }}'::timestamptz "
            "LIMIT 1;"
        ),
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60,
    )

    dq = PythonOperator(
        task_id="dq_checks",
        python_callable=_dq_checks,
    )

    get_batch >> wait_for_fact_table >> wait_for_fact_batch >> dq

