from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from remanga_ods_utils import extract_top_titles, upload_extracted_to_minio


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _extract(**_context):
    return extract_top_titles(
        periods=("new", "monthly", "year"),
        sections=("new", "manga", "manhwa", "manhua", "comics"),
        tag="all",
        count=20,
        pages=5,
        sleep_seconds=0.2,
    )


def _upload(**context):
    ti = context["ti"]
    extracted = ti.xcom_pull(task_ids="extract_remanga_top") or []
    # Store raw payloads per period
    return upload_extracted_to_minio(extracted, object_prefix="titles_top", include_raw_payload=True)


with DAG(
    dag_id="remanga_top_titles_to_minio_ods",
    default_args=default_args,
    description="ETL: ReManga top titles -> ODS (MinIO/S3, raw JSON)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="30 */6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["remanga", "ods", "minio", "s3"],
) as dag:
    extract_task = PythonOperator(
        task_id="extract_remanga_top",
        python_callable=_extract,
    )

    upload_task = PythonOperator(
        task_id="upload_to_minio",
        python_callable=_upload,
    )

    extract_task >> upload_task

