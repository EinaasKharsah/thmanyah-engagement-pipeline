import argparse
import os
from datetime import datetime

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


def get_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


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


def backfill_by_id_range(conn, producer, start_id: int, end_id: int):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, event_id, user_id, content_id, duration_ms, created_at
            FROM engagement_events
            WHERE id BETWEEN %s AND %s
            ORDER BY id ASC
            """,
            (start_id, end_id),
        )
        for row in cur:
            msg = row_to_message(row)
            producer.send(TOPIC, value=msg, key=msg["event_id"].encode("utf-8"))
    producer.flush()


def backfill_by_ts_range(conn, producer, start_ts: datetime, end_ts: datetime):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, event_id, user_id, content_id, duration_ms, created_at
            FROM engagement_events
            WHERE created_at BETWEEN %s AND %s
            ORDER BY created_at ASC
            """,
            (start_ts, end_ts),
        )
        for row in cur:
            msg = row_to_message(row)
            producer.send(TOPIC, value=msg, key=msg["event_id"].encode("utf-8"))
    producer.flush()


def parse_args():
    parser = argparse.ArgumentParser(description="Backfill engagement_events into Kafka.")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--id-range", nargs=2, metavar=("START_ID", "END_ID"), help="Backfill by id range.")
    group.add_argument(
        "--ts-range",
        nargs=2,
        metavar=("START_TS", "END_TS"),
        help="Backfill by timestamp range (ISO 8601).",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    conn = get_conn()
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    try:
        if args.id_range:
            start_id, end_id = map(int, args.id_range)
            print(f"Backfilling id range [{start_id}, {end_id}]")
            backfill_by_id_range(conn, producer, start_id, end_id)
        elif args.ts_range:
            start_ts = datetime.fromisoformat(args.ts_range[0])
            end_ts = datetime.fromisoformat(args.ts_range[1])
            print(f"Backfilling ts range [{start_ts}, {end_ts}]")
            backfill_by_ts_range(conn, producer, start_ts, end_ts)
    finally:
        conn.close()
        producer.close()


if __name__ == "__main__":
    main()


