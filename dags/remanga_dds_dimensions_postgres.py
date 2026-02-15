import json
import logging
from decimal import Decimal, InvalidOperation
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


def _create_dds_tables(**_context) -> None:
    hook = PostgresHook(postgres_conn_id="postgres_dds")
    sql = """
    CREATE SCHEMA IF NOT EXISTS dds;

    CREATE TABLE IF NOT EXISTS dds.dim_title_type (
      type_id BIGINT PRIMARY KEY,
      type_name TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dds.dim_title_status (
      status_id BIGINT PRIMARY KEY,
      status_name TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dds.dim_translate_status (
      translate_status_id BIGINT PRIMARY KEY,
      translate_status_name TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dds.dim_genre (
      genre_id BIGINT PRIMARY KEY,
      genre_name TEXT NOT NULL,
      genre_dir TEXT NULL
    );

    -- SCD2 dimension for title attributes
    CREATE TABLE IF NOT EXISTS dds.dim_title_scd2 (
      title_sk BIGSERIAL PRIMARY KEY,
      title_id BIGINT NOT NULL,

      main_name TEXT NULL,
      secondary_name TEXT NULL,
      dir TEXT NULL,
      issue_year INT NULL,

      type_id BIGINT NULL REFERENCES dds.dim_title_type(type_id),
      status_id BIGINT NULL REFERENCES dds.dim_title_status(status_id),
      translate_status_id BIGINT NULL REFERENCES dds.dim_translate_status(translate_status_id),

      avg_rating NUMERIC NULL,
      total_votes BIGINT NULL,
      total_views BIGINT NULL,
      count_bookmarks BIGINT NULL,
      count_moments BIGINT NULL,
      count_chapters BIGINT NULL,
      is_licensed BOOLEAN NULL,
      is_yaoi BOOLEAN NULL,
      is_erotic BOOLEAN NULL,
      is_forbidden BOOLEAN NULL,

      cover_low TEXT NULL,
      cover_mid TEXT NULL,
      cover_high TEXT NULL,

      another_name TEXT NULL,

      valid_from TIMESTAMPTZ NOT NULL,
      valid_to TIMESTAMPTZ NULL,
      is_current BOOLEAN NOT NULL DEFAULT TRUE,

      UNIQUE (title_id, valid_from)
    );

    CREATE INDEX IF NOT EXISTS idx_dim_title_scd2_title_current
      ON dds.dim_title_scd2(title_id)
      WHERE is_current;

    -- Bridge table title <-> genre (current snapshot of genres for current title_sk)
    CREATE TABLE IF NOT EXISTS dds.bridge_title_genre (
      title_sk BIGINT NOT NULL REFERENCES dds.dim_title_scd2(title_sk),
      genre_id BIGINT NOT NULL REFERENCES dds.dim_genre(genre_id),
      PRIMARY KEY (title_sk, genre_id)
    );
    """
    hook.run(sql)


def _load_reference_dims(**_context) -> None:
    """
    Loads type/status/translate_status/genres from the ODS raw JSON.
    """
    dds = PostgresHook(postgres_conn_id="postgres_dds")
    ods = PostgresHook(postgres_conn_id="postgres_ods")

    rows = ods.get_records(
        f"""
        SELECT raw
        FROM {ODS_TABLE}
        WHERE raw IS NOT NULL AND title_id IS NOT NULL
        """
    )

    types = {}
    statuses = {}
    tr_statuses = {}
    genres = {}

    for (raw_obj,) in rows:
        if raw_obj is None:
            continue
        if isinstance(raw_obj, str):
            raw = json.loads(raw_obj)
        else:
            raw = raw_obj

        t = raw.get("type") or {}
        if isinstance(t, dict) and t.get("id") is not None:
            types[int(t["id"])] = str(t.get("name") or "")

        s = raw.get("status") or {}
        if isinstance(s, dict) and s.get("id") is not None:
            statuses[int(s["id"])] = str(s.get("name") or "")

        ts = raw.get("translate_status") or {}
        if isinstance(ts, dict) and ts.get("id") is not None:
            tr_statuses[int(ts["id"])] = str(ts.get("name") or "")

        gs = raw.get("genres") or []
        if isinstance(gs, list):
            for g in gs:
                if not isinstance(g, dict) or g.get("id") is None:
                    continue
                gid = int(g["id"])
                genres[gid] = (str(g.get("name") or ""), str(g.get("dir") or "") or None)

    logging.info(
        "Reference extracted: types=%d statuses=%d translate_statuses=%d genres=%d",
        len(types),
        len(statuses),
        len(tr_statuses),
        len(genres),
    )

    # Upserts
    for type_id, type_name in types.items():
        dds.run(
            """
            INSERT INTO dds.dim_title_type(type_id, type_name)
            VALUES (%s, %s)
            ON CONFLICT (type_id) DO UPDATE SET type_name = EXCLUDED.type_name;
            """,
            parameters=(type_id, type_name),
        )

    for status_id, status_name in statuses.items():
        dds.run(
            """
            INSERT INTO dds.dim_title_status(status_id, status_name)
            VALUES (%s, %s)
            ON CONFLICT (status_id) DO UPDATE SET status_name = EXCLUDED.status_name;
            """,
            parameters=(status_id, status_name),
        )

    for ts_id, ts_name in tr_statuses.items():
        dds.run(
            """
            INSERT INTO dds.dim_translate_status(translate_status_id, translate_status_name)
            VALUES (%s, %s)
            ON CONFLICT (translate_status_id) DO UPDATE SET translate_status_name = EXCLUDED.translate_status_name;
            """,
            parameters=(ts_id, ts_name),
        )

    for gid, (gname, gdir) in genres.items():
        dds.run(
            """
            INSERT INTO dds.dim_genre(genre_id, genre_name, genre_dir)
            VALUES (%s, %s, %s)
            ON CONFLICT (genre_id) DO UPDATE
            SET genre_name = EXCLUDED.genre_name,
                genre_dir = EXCLUDED.genre_dir;
            """,
            parameters=(gid, gname, gdir),
        )


def _load_title_scd2_and_bridge(**_context) -> None:
    """
    Loads/updates SCD2 dim_title_scd2.

    Strategy (simple SCD2):
    - Take latest batch per (period, section) from ODS and build a "current" view of title attributes.
    - For each title_id:
      - If no current record exists -> insert new current version
      - If exists but attributes hash differ -> close previous (valid_to=batch_ts, is_current=false), insert new.
    """
    dds = PostgresHook(postgres_conn_id="postgres_dds")
    ods = PostgresHook(postgres_conn_id="postgres_ods")

    # Build latest batch rows per title from ODS using the max fetched_at (batch_ts)
    # Note: fetched_at is the same batch_ts across pages in our extractor.
    latest_batch = ods.get_first(f"SELECT MAX(fetched_at) FROM {ODS_TABLE}")
    batch_ts = latest_batch[0] if latest_batch else None
    if batch_ts is None:
        logging.info("ODS is empty, nothing to load into DDS")
        return

    src_rows = ods.get_records(
        f"""
        SELECT title_id, raw
        FROM {ODS_TABLE}
        WHERE fetched_at = %s AND title_id IS NOT NULL AND raw IS NOT NULL
        """,
        parameters=(batch_ts,),
    )

    def _to_int(v):
        if v is None:
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    def _to_decimal(v):
        if v is None:
            return None
        try:
            return Decimal(str(v))
        except (InvalidOperation, TypeError, ValueError):
            return None

    # helper to map raw -> flat attrs (normalized for stable comparisons)
    def _flat(raw: dict) -> dict:
        cover = raw.get("cover") or {}
        t = raw.get("type") or {}
        s = raw.get("status") or {}
        ts = raw.get("translate_status") or {}
        return {
            "main_name": raw.get("main_name"),
            "secondary_name": raw.get("secondary_name"),
            "dir": raw.get("dir"),
            "issue_year": _to_int(raw.get("issue_year")),
            "type_id": _to_int(t.get("id")) if isinstance(t, dict) else None,
            "status_id": _to_int(s.get("id")) if isinstance(s, dict) else None,
            "translate_status_id": _to_int(ts.get("id")) if isinstance(ts, dict) else None,
            "avg_rating": _to_decimal(raw.get("avg_rating")),
            "total_votes": _to_int(raw.get("total_votes")),
            "total_views": _to_int(raw.get("total_views")),
            "count_bookmarks": _to_int(raw.get("count_bookmarks")),
            "count_moments": _to_int(raw.get("count_moments")),
            "count_chapters": _to_int(raw.get("count_chapters")),
            "is_licensed": raw.get("is_licensed"),
            "is_yaoi": raw.get("is_yaoi"),
            "is_erotic": raw.get("is_erotic"),
            "is_forbidden": raw.get("is_forbidden"),
            "cover_low": (cover.get("low") if isinstance(cover, dict) else None),
            "cover_mid": (cover.get("mid") if isinstance(cover, dict) else None),
            "cover_high": (cover.get("high") if isinstance(cover, dict) else None),
            "another_name": raw.get("another_name"),
            "genres": raw.get("genres") or [],
        }

    # Load current SCD2 for comparison
    current = dds.get_records(
        """
        SELECT title_id, title_sk,
               main_name, secondary_name, dir, issue_year,
               type_id, status_id, translate_status_id,
               avg_rating, total_votes, total_views, count_bookmarks, count_moments, count_chapters,
               is_licensed, is_yaoi, is_erotic, is_forbidden,
               cover_low, cover_mid, cover_high, another_name
        FROM dds.dim_title_scd2
        WHERE is_current = TRUE
        """
    )
    current_by_title = {int(r[0]): r for r in current}

    def _attrs_tuple(f: dict):
        return (
            f.get("main_name"),
            f.get("secondary_name"),
            f.get("dir"),
            f.get("issue_year"),
            f.get("type_id"),
            f.get("status_id"),
            f.get("translate_status_id"),
            f.get("avg_rating"),
            f.get("total_votes"),
            f.get("total_views"),
            f.get("count_bookmarks"),
            f.get("count_moments"),
            f.get("count_chapters"),
            f.get("is_licensed"),
            f.get("is_yaoi"),
            f.get("is_erotic"),
            f.get("is_forbidden"),
            f.get("cover_low"),
            f.get("cover_mid"),
            f.get("cover_high"),
            f.get("another_name"),
        )

    close_prev_sql = """
    UPDATE dds.dim_title_scd2
    SET valid_to = %s, is_current = FALSE
    WHERE title_id = %s AND is_current = TRUE AND valid_from < %s;
    """

    upsert_sql = """
    INSERT INTO dds.dim_title_scd2 (
      title_id, main_name, secondary_name, dir, issue_year,
      type_id, status_id, translate_status_id,
      avg_rating, total_votes, total_views, count_bookmarks, count_moments, count_chapters,
      is_licensed, is_yaoi, is_erotic, is_forbidden,
      cover_low, cover_mid, cover_high, another_name,
      valid_from, valid_to, is_current
    ) VALUES (
      %s,%s,%s,%s,%s,
      %s,%s,%s,
      %s,%s,%s,%s,%s,%s,
      %s,%s,%s,%s,
      %s,%s,%s,%s,
      %s,NULL,TRUE
    )
    ON CONFLICT (title_id, valid_from) DO UPDATE SET
      main_name = EXCLUDED.main_name,
      secondary_name = EXCLUDED.secondary_name,
      dir = EXCLUDED.dir,
      issue_year = EXCLUDED.issue_year,
      type_id = EXCLUDED.type_id,
      status_id = EXCLUDED.status_id,
      translate_status_id = EXCLUDED.translate_status_id,
      avg_rating = EXCLUDED.avg_rating,
      total_votes = EXCLUDED.total_votes,
      total_views = EXCLUDED.total_views,
      count_bookmarks = EXCLUDED.count_bookmarks,
      count_moments = EXCLUDED.count_moments,
      count_chapters = EXCLUDED.count_chapters,
      is_licensed = EXCLUDED.is_licensed,
      is_yaoi = EXCLUDED.is_yaoi,
      is_erotic = EXCLUDED.is_erotic,
      is_forbidden = EXCLUDED.is_forbidden,
      cover_low = EXCLUDED.cover_low,
      cover_mid = EXCLUDED.cover_mid,
      cover_high = EXCLUDED.cover_high,
      another_name = EXCLUDED.another_name,
      valid_to = NULL,
      is_current = TRUE;
    """

    for (title_id, raw_obj) in src_rows:
        if title_id is None:
            continue
        tid = int(title_id)
        raw = json.loads(raw_obj) if isinstance(raw_obj, str) else raw_obj
        f = _flat(raw)
        new_tuple = _attrs_tuple(f)

        prev = current_by_title.get(tid)
        prev_tuple = tuple(prev[2:]) if prev is not None else None  # attrs excluding title_id, title_sk

        if prev_tuple != new_tuple:
            # close any older current version (never closes current batch row)
            dds.run(
                close_prev_sql,
                parameters=(batch_ts, tid, batch_ts),
            )
            # upsert current batch row (idempotent per (title_id, valid_from))
            dds.run(
                upsert_sql,
                parameters=(
                    tid,
                    f.get("main_name"),
                    f.get("secondary_name"),
                    f.get("dir"),
                    f.get("issue_year"),
                    f.get("type_id"),
                    f.get("status_id"),
                    f.get("translate_status_id"),
                    f.get("avg_rating"),
                    f.get("total_votes"),
                    f.get("total_views"),
                    f.get("count_bookmarks"),
                    f.get("count_moments"),
                    f.get("count_chapters"),
                    f.get("is_licensed"),
                    f.get("is_yaoi"),
                    f.get("is_erotic"),
                    f.get("is_forbidden"),
                    f.get("cover_low"),
                    f.get("cover_mid"),
                    f.get("cover_high"),
                    f.get("another_name"),
                    batch_ts,
                ),
            )

    # Refresh bridge for current titles in this batch:
    # 1) Delete all bridge rows for current title_sks (safe)
    dds.run(
        """
        DELETE FROM dds.bridge_title_genre
        WHERE title_sk IN (SELECT title_sk FROM dds.dim_title_scd2 WHERE is_current = TRUE);
        """
    )

    # 2) Insert genres
    current_sks = dds.get_records("SELECT title_id, title_sk FROM dds.dim_title_scd2 WHERE is_current = TRUE;")
    sk_by_title = {int(tid): int(sk) for (tid, sk) in current_sks}

    inserted = 0
    for (title_id, raw_obj) in src_rows:
        if title_id is None:
            continue
        tid = int(title_id)
        title_sk = sk_by_title.get(tid)
        if title_sk is None:
            continue
        raw = json.loads(raw_obj) if isinstance(raw_obj, str) else raw_obj
        gs = raw.get("genres") or []
        if not isinstance(gs, list):
            continue
        for g in gs:
            if not isinstance(g, dict) or g.get("id") is None:
                continue
            gid = int(g["id"])
            dds.run(
                """
                INSERT INTO dds.bridge_title_genre(title_sk, genre_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING;
                """,
                parameters=(title_sk, gid),
            )
            inserted += 1

    logging.info("DDS title SCD2 loaded for batch=%s, bridge rows inserted=%d", batch_ts, inserted)


with DAG(
    dag_id="remanga_dds_dimensions_postgres",
    default_args=DEFAULT_ARGS,
    description="DDS: load dimensions (SCD2 title + reference dims) into Postgres",
    start_date=datetime(2025, 1, 1),
    # Runs periodically; dependencies handled by SqlSensor.
    schedule_interval="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["remanga", "dds", "postgres"],
) as dag:
    # NOTE: ExternalTaskSensor may hang forever for manual runs due to logical_date mismatch.
    # Data-driven sensor: wait until ODS table has any rows.
    wait_for_ods = SqlSensor(
        task_id="wait_for_ods_postgres_data",
        conn_id="postgres_ods",
        sql=f"SELECT 1 FROM {ODS_TABLE} LIMIT 1;",
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60,
    )

    create_tables = PythonOperator(
        task_id="create_dds_tables",
        python_callable=_create_dds_tables,
    )

    load_refs = PythonOperator(
        task_id="load_reference_dimensions",
        python_callable=_load_reference_dims,
    )

    load_titles = PythonOperator(
        task_id="load_title_scd2_and_bridge",
        python_callable=_load_title_scd2_and_bridge,
    )

    wait_for_ods >> create_tables >> load_refs >> load_titles

