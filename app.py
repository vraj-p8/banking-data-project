import hashlib
import logging
import os
from urllib.parse import unquote_plus

import requests
from fastapi import FastAPI, HTTPException, Request

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("minio_event_receiver")

AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1")
AIRFLOW_DAG_ID = os.getenv("AIRFLOW_DAG_ID", "minio_to_snowflake_banking")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD")

app = FastAPI()


def _build_run_id(bucket: str, keys: list[str], event_time: str | None) -> str:
    base = f"{bucket}|{'|'.join(keys)}|{event_time or ''}"
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:12]
    if event_time:
        safe_time = event_time.replace(":", "").replace(".", "")
        return f"minio__{digest}__{safe_time}"
    return f"minio__{digest}"


def _trigger_airflow_dag(bucket: str, keys: list[str], event_time: str | None) -> None:
    if not AIRFLOW_USERNAME or not AIRFLOW_PASSWORD:
        raise RuntimeError("AIRFLOW_USERNAME/AIRFLOW_PASSWORD not set")

    run_id = _build_run_id(bucket, keys, event_time)
    payload = {
        "dag_run_id": run_id,
        "conf": {
            "bucket": bucket,
            "keys": keys,
            "event_time": event_time,
        },
    }
    url = f"{AIRFLOW_API_URL}/dags/{AIRFLOW_DAG_ID}/dagRuns"
    resp = requests.post(
        url,
        json=payload,
        auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
        timeout=10,
    )
    if resp.status_code >= 300:
        raise RuntimeError(f"Airflow trigger failed: {resp.status_code} {resp.text}")


@app.get("/health")
def healthcheck():
    return {"status": "ok"}


@app.post("/minio/events")
async def handle_minio_events(request: Request):
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON") from exc

    records = payload.get("Records", [])
    if not records:
        return {"status": "ignored", "reason": "no records"}

    keys: list[str] = []
    bucket_name = None
    event_time = None

    for record in records:
        s3 = record.get("s3", {})
        bucket = s3.get("bucket", {}).get("name")
        key = s3.get("object", {}).get("key")
        if not bucket or not key:
            continue
        if bucket_name is None:
            bucket_name = bucket
        event_time = event_time or record.get("eventTime")
        decoded_key = unquote_plus(key)
        if decoded_key.endswith("/"):
            continue
        keys.append(decoded_key)

    if not bucket_name or not keys:
        return {"status": "ignored", "reason": "no usable keys"}

    try:
        _trigger_airflow_dag(bucket_name, keys, event_time)
        logger.info("Triggered DAG for %s keys in bucket %s", len(keys), bucket_name)
    except Exception as exc:
        logger.error("Failed to trigger DAG: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to trigger Airflow DAG") from exc

    return {"status": "ok", "bucket": bucket_name, "keys": keys}
