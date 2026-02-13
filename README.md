# MangaReview
ETL/ODS стенд для курса “Технологии хранения больших данных” (ЛР1).

## Источник данных

ReManga public API:

- `https://api.remanga.org/api/v2/titles/top/?count=20&page=1&period=new&section=new&tag=all`
- `https://api.remanga.org/api/v2/titles/top/?count=20&page=1&period=monthly&section=new&tag=all`
- `https://api.remanga.org/api/v2/titles/top/?count=20&page=1&period=year&section=new&tag=all`

## Что реализовано (ЛР1)

- **Airflow в Docker**: `docker-compose.yaml` (CeleryExecutor).
- **3 ODS хранилища**:
  - **PostgreSQL** (`postgres_ods`) — нормализованные поля + `raw JSONB`.
  - **MongoDB** (`mongo_ods`) — сырые документы JSON без жёсткой схемы.
  - **MinIO/S3** (`minio`) — data lake для сырых JSON-файлов.
- **3 DAG (ETL)**, которые извлекают “top titles” по периодам `new/monthly/year` и грузят в ODS:
  - `remanga_top_titles_to_postgres_ods`
  - `remanga_top_titles_to_mongo_ods`
  - `remanga_top_titles_to_minio_ods`

## Запуск (на машине для проверки)

Все команды ниже предполагают, что вы находитесь **в директории `MangaReview/`** (важно для корректных volume-монтов `dags/`, `logs/` и т.д.).

```bash
cd MangaReview
```

1) Скопировать env и указать UID (Linux):

```bash
cp .env.example .env
id -u
```

2) Инициализация Airflow:

```bash
docker compose up airflow-init
```

3) Запуск стенда:

```bash
docker compose up -d
```

- **Airflow UI**: `http://localhost:8080` (логин/пароль из `.env`, по умолчанию `airflow/airflow`)
- **MinIO Console**: `http://localhost:9001` (по умолчанию `minioadmin/minioadmin`)

## Отчёт

См. `reports/Лаба 1. MangaReview.md`.
