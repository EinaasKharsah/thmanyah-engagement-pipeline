import os
import time
from typing import Optional

import psycopg2
from kafka import KafkaProducer
import json


PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_DB = os.environ.get("PG_DB", "engagement")
PG_USER = os.environ.get("PG_USER", "eng")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "eng")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "engagement_events"

OFFSET_FILE = "/app/last_published_id.txt"


def get_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def load_last_id() -> int:
    try:
        with open(OFFSET_FILE, "r", encoding="utf-8") as f:
            return int(f.read().strip())
    except Exception:
        return 0


def save_last_id(last_id: int) -> None:
    with open(OFFSET_FILE, "w", encoding="utf-8") as f:
        f.write(str(last_id))


def fetch_new_events(conn, last_id: int, batch_size: int = 100):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, event_id, user_id, content_id, duration_ms, created_at
            FROM engagement_events
            WHERE id > %s
            ORDER BY id ASC
            LIMIT %s
            """,
            (last_id, batch_size),
        )
        rows = cur.fetchall()
    return rows


def row_to_message(row) -> dict:
    (
        id_,
        event_id,
        user_id,
        content_id,
        duration_ms,
        created_at,
    ) = row
    return {
        "id": id_,
        "event_id": str(event_id),
        "user_id": str(user_id),
        "content_id": str(content_id),
        "duration_ms": duration_ms,
        "created_at": created_at.isoformat(),
    }


def publish_loop():
    conn = None
    while conn is None:
        try:
            conn = get_conn()
        except Exception as e:
            print("Waiting for Postgres...", e)
            time.sleep(2)

    producer: Optional[KafkaProducer] = None
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except Exception as e:
            print("Waiting for Kafka...", e)
            time.sleep(2)

    last_id = load_last_id()
    print(f"Starting publisher from last_id={last_id}")

    try:
        while True:
            rows = fetch_new_events(conn, last_id)
            if not rows:
                time.sleep(1.0)
                continue

            for row in rows:
                msg = row_to_message(row)
                producer.send(TOPIC, value=msg, key=msg["event_id"].encode("utf-8"))
                last_id = msg["id"]

            producer.flush()
            save_last_id(last_id)
            print(f"Published up to id={last_id}")
    finally:
        if conn:
            conn.close()
        if producer:
            producer.close()


if __name__ == "__main__":
    publish_loop()


