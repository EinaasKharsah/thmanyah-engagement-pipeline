import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import Optional, Any, Mapping

import faust
import psycopg2
import httpx

from redis_utils import get_redis_client, update_bucket
from clickhouse_utils import get_client, insert_enriched_event


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("processor")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_DB = os.environ.get("PG_DB", "engagement")
PG_USER = os.environ.get("PG_USER", "eng")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "eng")

EXTERNAL_API_URL = os.environ.get("EXTERNAL_API_URL", "http://external_api:8000/ingest")

app = faust.App(
    "engagement-processor",
    broker=f"kafka://{KAFKA_BOOTSTRAP_SERVERS}",
    value_serializer="raw",
)


def to_mapping(obj: Any) -> Mapping[str, Any]:
    """
    Best-effort conversion of an object into a plain dict.
    Works for dicts, Faust Records, and generic Python objects.
    """
    if isinstance(obj, dict):
        return obj
    # Faust Records typically have .to_representation()
    if hasattr(obj, "to_representation"):
        try:
            rep = obj.to_representation()
            if isinstance(rep, dict):
                return rep
        except Exception:
            pass
    # Some objects may provide a direct .asdict()
    if hasattr(obj, "asdict"):
        try:
            rep = obj.asdict()  # type: ignore[call-arg]
            if isinstance(rep, dict):
                return rep
        except Exception:
            pass
    # Fallback to __dict__ minus private attributes
    if hasattr(obj, "__dict__"):
        return {k: v for k, v in obj.__dict__.items() if not k.startswith("_")}
    # Last resort: wrap as value
    return {"value": obj}


class EngagementEvent(faust.Record, serializer="json"):
    id: int
    event_id: str
    user_id: str
    content_id: str
    duration_ms: Optional[int]
    created_at: str


class EnrichedEvent(faust.Record, serializer="json"):
    event_id: str
    event_time: str
    user_id: str
    content_id: str
    content_type: Optional[str]
    length_seconds: Optional[int]
    duration_ms: Optional[int]
    engagement_seconds: Optional[float]
    engagement_pct: Optional[float]


topic = app.topic("engagement_events", value_type=EngagementEvent)


def pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def fetch_content(conn, content_id: str):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT content_type, length_seconds FROM content WHERE id = %s",
            (content_id,),
        )
        row = cur.fetchone()
        if row:
            return {"content_type": row[0], "length_seconds": row[1]}
        return {"content_type": None, "length_seconds": None}


def compute_engagement(duration_ms: Optional[int], length_seconds: Optional[int]):
    if duration_ms is None:
        return None, None
    engagement_seconds = duration_ms / 1000.0
    if length_seconds is None:
        return engagement_seconds, None
    if length_seconds == 0:
        return engagement_seconds, None
    engagement_pct = round(engagement_seconds / float(length_seconds), 2)
    return engagement_seconds, engagement_pct


async def post_with_retries(payload: dict, max_retries: int = 5):
    backoff = 0.5
    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.post(EXTERNAL_API_URL, json=payload)
            if resp.status_code == 200:
                return
        except Exception:
            pass
        await app.sleep(backoff)
        backoff *= 2

    # dead-letter
    with open("/app/dead_letter.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(payload) + "\n")


@app.agent(topic)
async def process(stream):
    r = get_redis_client()
    ch_client = get_client()

    # keep a single Postgres connection per worker
    conn = None
    while conn is None:
        try:
            conn = pg_conn()
        except Exception as e:
            print("Waiting for Postgres in processor...", e)
            time.sleep(2)

    async for event in stream:
        consume_ts = datetime.now(timezone.utc)
        logger.info(
            "Event consumed from Kafka",
            extra={
                "event_id": getattr(event, "event_id", None),
                "event_ts": getattr(event, "created_at", None),
                "consume_ts": consume_ts.isoformat(),
            },
        )

        # Enrich from Postgres
        content_meta = fetch_content(conn, event.content_id)

        # Transform fields
        event_time = datetime.fromisoformat(event.created_at)
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)

        engagement_seconds, engagement_pct = compute_engagement(
            event.duration_ms, content_meta["length_seconds"]
        )

        enriched = EnrichedEvent(
            event_id=event.event_id,
            event_time=event_time.isoformat(),
            user_id=event.user_id,
            content_id=event.content_id,
            content_type=content_meta["content_type"],
            length_seconds=content_meta["length_seconds"],
            duration_ms=event.duration_ms,
            engagement_seconds=engagement_seconds,
            engagement_pct=engagement_pct,
        )

        enriched_mapping = to_mapping(enriched)

        # Sink 1: ClickHouse
        insert_enriched_event(ch_client, enriched_mapping)

        # Sink 2: Redis buckets (score: engagement_seconds or 1 if None)
        score = engagement_seconds if engagement_seconds is not None else 1.0
        update_bucket(r, event_time, enriched.content_id, score)
        redis_ts = datetime.now(timezone.utc)
        logger.info(
            "Redis updated for event",
            extra={
                "event_id": enriched_mapping.get("event_id"),
                "event_ts": enriched_mapping.get("event_time"),
                "redis_ts": redis_ts.isoformat(),
                "latency_seconds": (redis_ts - consume_ts).total_seconds(),
            },
        )

        # Sink 3: external HTTP system (async with retries)
        await post_with_retries(enriched_mapping)


