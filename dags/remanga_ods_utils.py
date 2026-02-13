import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, List, Optional, Tuple, Dict

import requests

from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

try:
    from pymongo import MongoClient
except Exception:  # pragma: no cover - installed in container via _PIP_ADDITIONAL_REQUIREMENTS
    MongoClient = None  # type: ignore[assignment]

try:
    import boto3
    from botocore.client import Config as BotoConfig
except Exception:  # pragma: no cover - installed in container via _PIP_ADDITIONAL_REQUIREMENTS
    boto3 = None  # type: ignore[assignment]
    BotoConfig = None  # type: ignore[assignment]


REMANGA_TOP_URL = "https://api.remanga.org/api/v2/titles/top/"
DEFAULT_PERIODS = ("new", "monthly", "year")
DEFAULT_SECTIONS = ("new", "manga", "manhwa", "manhua", "comics")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _stable_item_key(period: str, section: str, item: Any) -> str:
    """
    Возвращает стабильный ключ элемента для ODS.
    Предпочитаем числовой id, иначе строим хэш по JSON-представлению.
    """
    if isinstance(item, dict):
        for k in ("id", "title_id", "pk", "slug"):
            v = item.get(k)
            if v is not None and v != "":
                # IMPORTANT: keep uniqueness across sections
                return f"{section}:{v}"
    raw = json.dumps(item, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(f"{period}:{section}:{raw}".encode("utf-8")).hexdigest()


def _pick(d: Dict[str, Any], keys: Iterable[str]) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def _extract_items(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for k in ("titles", "results", "items", "content", "data"):
            v = payload.get(k)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
    return []


def fetch_remanga_top(
    period: str,
    page: int = 1,
    count: int = 20,
    section: str = "new",
    tag: str = "all",
    timeout_seconds: int = 30,
    max_retries: int = 5,
    backoff_seconds: float = 0.5,
    sleep_seconds: float = 0.2,
) -> Dict[str, Any]:
    """
    Extract: забирает JSON ReManga API (top titles).

    В `idea.md` приведены URL, здесь мы формируем их параметрами:
    - period: new/monthly/year
    - count/page: размер страницы/страница
    - section/tag: фильтры
    """
    params = {
        "count": count,
        "page": page,
        "period": period,
        "section": section,
        "tag": tag,
    }
    headers = {
        "User-Agent": "airflow-remanga-ods/1.0",
        "Accept": "application/json",
    }

    last_exc: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(
                REMANGA_TOP_URL,
                params=params,
                headers=headers,
                timeout=timeout_seconds,
            )
            if resp.status_code == 429:
                sleep_for = min(10.0, backoff_seconds * attempt * 2)
                logging.warning("Rate limited (429). Sleeping %.2fs", sleep_for)
                time.sleep(sleep_for)
                continue
            resp.raise_for_status()
            if sleep_seconds and sleep_seconds > 0:
                time.sleep(sleep_seconds)
            return {
                "period": period,
                "section": section,
                "tag": tag,
                "page": page,
                "count": count,
                "fetched_at": _now_utc().isoformat(),
                "url": resp.url,
                "payload": resp.json(),
            }
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            sleep_for = min(10.0, backoff_seconds * attempt * 2)
            logging.warning("Fetch failed attempt=%s/%s: %s", attempt, max_retries, exc)
            time.sleep(sleep_for)

    raise RuntimeError(f"Failed to fetch ReManga top for period={period}: {last_exc}")


def extract_all_periods(
    periods: Tuple[str, ...] = DEFAULT_PERIODS,
    page: int = 1,
    count: int = 20,
    section: str = "new",
    tag: str = "all",
) -> List[Dict[str, Any]]:
    """
    Extract: забирает топ по нескольким периодам. Возвращает список объектов:
    {period, fetched_at, url, payload}
    """
    out: List[Dict[str, Any]] = []
    for p in periods:
        out.append(fetch_remanga_top(period=p, page=page, count=count, section=section, tag=tag))
    logging.info("Fetched %d periods from ReManga API", len(out))
    return out


def extract_top_titles(
    periods: Tuple[str, ...] = DEFAULT_PERIODS,
    sections: Tuple[str, ...] = DEFAULT_SECTIONS,
    tag: str = "all",
    count: int = 20,
    pages: int = 5,
    sleep_seconds: float = 0.2,
) -> List[Dict[str, Any]]:
    """
    Extract: вытаскивает топы по матрице (period x section x page).

    Требование "минимум 100 элементов" выполняется как pages=5, count=20 (API капает 20).
    """
    out: List[Dict[str, Any]] = []
    for section in sections:
        for period in periods:
            for page in range(1, pages + 1):
                out.append(
                    fetch_remanga_top(
                        period=period,
                        section=section,
                        tag=tag,
                        page=page,
                        count=count,
                        sleep_seconds=sleep_seconds,
                    )
                )
    logging.info(
        "Fetched %d payloads (sections=%d, periods=%d, pages=%d)",
        len(out),
        len(sections),
        len(periods),
        pages,
    )
    return out


def transform_to_rows(extracted: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Transform: минимальная нормализация + сохранение raw JSON.
    Делает строки для загрузки в ODS (Postgres/Mongo).
    """
    rows: List[Dict[str, Any]] = []
    for entry in extracted:
        period = entry.get("period")
        section = entry.get("section") or "unknown"
        page = entry.get("page")
        count = entry.get("count")
        tag = entry.get("tag")
        fetched_at = entry.get("fetched_at")
        url = entry.get("url")
        payload = entry.get("payload")

        items = _extract_items(payload)
        for idx, item in enumerate(items, start=1):
            page_int = int(page) if page is not None else 1
            count_int = int(count) if count is not None else 20
            global_rank = (page_int - 1) * count_int + idx
            item_key = _stable_item_key(str(period), str(section), item)
            title_id = None
            title_name = None
            rating = None
            if isinstance(item, dict):
                title_id = _pick(item, ("id", "title_id", "pk"))
                title_name = _pick(item, ("name", "rus_name", "en_name", "title"))
                rating = _pick(item, ("rating", "score", "avg_rating"))

            rows.append(
                {
                    "period": str(period),
                    "section": str(section),
                    "tag": str(tag) if tag is not None else None,
                    "item_key": str(item_key),
                    "page": page_int,
                    "rank": global_rank,
                    "title_id": title_id,
                    "title_name": title_name,
                    "rating": rating,
                    "fetched_at": fetched_at,
                    "source_url": url,
                    "raw": item,
                }
            )
    logging.info("Transformed %d rows", len(rows))
    return rows


def load_rows_to_postgres(rows: List[Dict[str, Any]], postgres_conn_id: str = "postgres_ods") -> None:
    """
    Load: upsert в PostgreSQL ODS.
    """
    if not rows:
        logging.info("No rows to load into Postgres ODS")
        return

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ods_remanga_titles_top_pg (
            period TEXT NOT NULL,
            section TEXT NULL,
            tag TEXT NULL,
            item_key TEXT NOT NULL,
            page INT,
            rank INT,
            title_id BIGINT NULL,
            title_name TEXT NULL,
            rating NUMERIC NULL,
            fetched_at TIMESTAMPTZ NULL,
            source_url TEXT NULL,
            raw JSONB NOT NULL,
            PRIMARY KEY (period, item_key)
        );
        """
    )

    # Backward/forward compatible schema evolution
    cur.execute("ALTER TABLE ods_remanga_titles_top_pg ADD COLUMN IF NOT EXISTS section TEXT;")
    cur.execute("ALTER TABLE ods_remanga_titles_top_pg ADD COLUMN IF NOT EXISTS tag TEXT;")
    cur.execute("ALTER TABLE ods_remanga_titles_top_pg ADD COLUMN IF NOT EXISTS page INT;")

    insert_sql = """
        INSERT INTO ods_remanga_titles_top_pg
            (period, section, tag, item_key, page, rank, title_id, title_name, rating, fetched_at, source_url, raw)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (period, item_key) DO UPDATE
        SET section = EXCLUDED.section,
            tag = EXCLUDED.tag,
            page = EXCLUDED.page,
            rank = EXCLUDED.rank,
            title_id = EXCLUDED.title_id,
            title_name = EXCLUDED.title_name,
            rating = EXCLUDED.rating,
            fetched_at = EXCLUDED.fetched_at,
            source_url = EXCLUDED.source_url,
            raw = EXCLUDED.raw;
    """

    for r in rows:
        cur.execute(
            insert_sql,
            (
                r["period"],
                r.get("section"),
                r.get("tag"),
                r["item_key"],
                r.get("page"),
                r.get("rank"),
                r.get("title_id"),
                r.get("title_name"),
                r.get("rating"),
                r.get("fetched_at"),
                r.get("source_url"),
                json.dumps(r["raw"], ensure_ascii=False, default=str),
            ),
        )

    conn.commit()
    cur.close()
    conn.close()
    logging.info("Loaded %d rows into Postgres ODS", len(rows))


def load_rows_to_mongo(rows: List[Dict[str, Any]], mongo_conn_id: str = "mongo_ods") -> None:
    """
    Load: upsert в MongoDB ODS (документы, _id = period:item_key).
    """
    if not rows:
        logging.info("No rows to load into Mongo ODS")
        return
    if MongoClient is None:
        raise RuntimeError("pymongo is not available in the environment")

    conn = BaseHook.get_connection(mongo_conn_id)
    extras = conn.extra_dejson or {}
    db_name = extras.get("database", "ods_manga")

    host = conn.host or "mongo_ods"
    port = conn.port or 27017

    auth = ""
    if conn.login:
        auth = conn.login
        if conn.password:
            auth += f":{conn.password}"
        auth += "@"

    uri = f"mongodb://{auth}{host}:{port}"
    client = MongoClient(uri)
    db = client[db_name]
    collection = db["ods_remanga_titles_top"]

    for r in rows:
        doc = dict(r)
        doc["_id"] = f"{r['period']}:{r['item_key']}"
        collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)

    logging.info("Loaded %d rows into Mongo ODS", len(rows))


@dataclass(frozen=True)
class MinioConfig:
    endpoint_url: str
    access_key: str
    secret_key: str
    bucket: str


def _get_minio_config() -> MinioConfig:
    # Prefer explicit Airflow envs (set in docker-compose). Fallback to common names.
    endpoint = os.getenv("REMANGA_MINIO_ENDPOINT") or os.getenv("MINIO_ENDPOINT") or "http://minio:9000"
    access = os.getenv("REMANGA_MINIO_ACCESS_KEY") or os.getenv("MINIO_ACCESS_KEY") or "minioadmin"
    secret = os.getenv("REMANGA_MINIO_SECRET_KEY") or os.getenv("MINIO_SECRET_KEY") or "minioadmin"
    bucket = os.getenv("REMANGA_MINIO_BUCKET") or os.getenv("MINIO_BUCKET") or "ods-remanga"
    return MinioConfig(endpoint_url=endpoint, access_key=access, secret_key=secret, bucket=bucket)


def upload_extracted_to_minio(
    extracted: List[Dict[str, Any]],
    object_prefix: str = "titles_top",
    include_raw_payload: bool = True,
) -> List[str]:
    """
    Load: сохраняет сырые данные в MinIO (S3).
    Пишем один объект на период, в key включаем fetched_at.
    """
    if not extracted:
        logging.info("No extracted payloads to upload to MinIO")
        return []
    if boto3 is None or BotoConfig is None:
        raise RuntimeError("boto3 is not available in the environment")

    cfg = _get_minio_config()
    s3 = boto3.client(
        "s3",
        endpoint_url=cfg.endpoint_url,
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
        region_name="us-east-1",
        config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
    )

    uploaded: List[str] = []
    for entry in extracted:
        period = str(entry.get("period"))
        section = str(entry.get("section") or "unknown")
        page = str(entry.get("page") or "1")
        fetched_at = str(entry.get("fetched_at") or _now_utc().isoformat())
        safe_ts = fetched_at.replace(":", "").replace("+", "").replace("-", "")
        key = f"{object_prefix}/section={section}/period={period}/page={page}/fetched_at={safe_ts}.json"

        body_obj = dict(entry)
        if not include_raw_payload:
            body_obj.pop("payload", None)

        data = json.dumps(body_obj, ensure_ascii=False, default=str).encode("utf-8")
        s3.put_object(
            Bucket=cfg.bucket,
            Key=key,
            Body=data,
            ContentType="application/json",
        )
        uploaded.append(f"s3://{cfg.bucket}/{key}")

    logging.info("Uploaded %d objects into MinIO bucket=%s", len(uploaded), cfg.bucket)
    return uploaded

