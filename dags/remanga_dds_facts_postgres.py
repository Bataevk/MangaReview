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


def _create_fact_table(**_context) -> None:
    dds = PostgresHook(postgres_conn_id="postgres_dds")
    dds.run(
        """
        CREATE SCHEMA IF NOT EXISTS dds;

        CREATE TABLE IF NOT EXISTS dds.fct_top_titles_snapshot (
          batch_ts TIMESTAMPTZ NOT NULL,
          period TEXT NOT NULL,
          section TEXT NOT NULL,
          rank INT NOT NULL,
          title_sk BIGINT NOT NULL REFERENCES dds.dim_title_scd2(title_sk),

          avg_rating NUMERIC NULL,
          total_votes BIGINT NULL,
          total_views BIGINT NULL,
          count_bookmarks BIGINT NULL,
          count_moments BIGINT NULL,
          count_chapters BIGINT NULL,

          source_url TEXT NULL,

          PRIMARY KEY (batch_ts, period, section, rank)
        );
        """
    )


def _load_facts(**_context) -> None:
    dds = PostgresHook(postgres_conn_id="postgres_dds")
    ods = PostgresHook(postgres_conn_id="postgres_ods")

    latest_batch = ods.get_first(f"SELECT MAX(fetched_at) FROM {ODS_TABLE}")
    batch_ts = latest_batch[0] if latest_batch else None
    if batch_ts is None:
        logging.info("ODS is empty, skip facts load")
        return

    # ODS and DDS are separate databases, so we extract from ODS and load into DDS in Python.
    title_map = {
        int(tid): int(sk)
        for (tid, sk) in dds.get_records("SELECT title_id, title_sk FROM dds.dim_title_scd2 WHERE is_current = TRUE;")
        if tid is not None and sk is not None
    }

    ods_rows = ods.get_records(
        f"""
        SELECT
          period,
          COALESCE(section, 'unknown') AS section,
          rank,
          title_id,
          rating AS avg_rating,
          NULLIF(raw->>'total_votes', '')::bigint AS total_votes,
          NULLIF(raw->>'total_views', '')::bigint AS total_views,
          NULLIF(raw->>'count_bookmarks', '')::bigint AS count_bookmarks,
          NULLIF(raw->>'count_moments', '')::bigint AS count_moments,
          NULLIF(raw->>'count_chapters', '')::bigint AS count_chapters,
          source_url
        FROM {ODS_TABLE}
        WHERE fetched_at = %s AND title_id IS NOT NULL
        """,
        parameters=(batch_ts,),
    )

    insert_sql = """
    INSERT INTO dds.fct_top_titles_snapshot (
      batch_ts, period, section, rank, title_sk,
      avg_rating, total_votes, total_views, count_bookmarks, count_moments, count_chapters,
      source_url
    ) VALUES (
      %s,%s,%s,%s,%s,
      %s,%s,%s,%s,%s,%s,
      %s
    )
    ON CONFLICT (batch_ts, period, section, rank) DO NOTHING;
    """

    inserted = 0
    skipped_no_dim = 0
    for (
        period,
        section,
        rank,
        title_id,
        avg_rating,
        total_votes,
        total_views,
        count_bookmarks,
        count_moments,
        count_chapters,
        source_url,
    ) in ods_rows:
        tid = int(title_id) if title_id is not None else None
        if tid is None:
            continue
        sk = title_map.get(tid)
        if sk is None:
            skipped_no_dim += 1
            continue
        dds.run(
            insert_sql,
            parameters=(
                batch_ts,
                period,
                section,
                rank,
                sk,
                avg_rating,
                total_votes,
                total_views,
                count_bookmarks,
                count_moments,
                count_chapters,
                source_url,
            ),
        )
        inserted += 1

    cnt = dds.get_first(
        "SELECT COUNT(*) FROM dds.fct_top_titles_snapshot WHERE batch_ts = %s;",
        parameters=(batch_ts,),
    )[0]
    logging.info(
        "Facts loaded for batch=%s. ods_rows=%d inserted=%d skipped_no_dim=%d total_in_fact=%s",
        batch_ts,
        len(ods_rows),
        inserted,
        skipped_no_dim,
        cnt,
    )


with DAG(
    dag_id="remanga_dds_facts_postgres",
    default_args=DEFAULT_ARGS,
    description="DDS: load fact snapshot from ODS into Postgres",
    start_date=datetime(2025, 1, 1),
    # Runs periodically; dependencies handled by SqlSensor.
    schedule_interval="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["remanga", "dds", "facts", "postgres"],
) as dag:
    # Data-driven dependency: wait until dimensions table exists in DDS.
    wait_for_dims = SqlSensor(
        task_id="wait_for_dds_dimensions_ready",
        conn_id="postgres_dds",
        sql="SELECT 1 WHERE to_regclass('dds.dim_title_scd2') IS NOT NULL;",
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60,
    )

    create_fact = PythonOperator(
        task_id="create_fact_table",
        python_callable=_create_fact_table,
    )

    load_fact = PythonOperator(
        task_id="load_fact_snapshot",
        python_callable=_load_facts,
    )

    wait_for_dims >> create_fact >> load_fact

