import os
import time
import random
from datetime import datetime, timezone

import psycopg2
from faker import Faker


PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_DB = os.environ.get("PG_DB", "engagement")
PG_USER = os.environ.get("PG_USER", "eng")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "eng")

fake = Faker()


def get_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def seed_content(conn, num_rows: int = 50):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM content;")
        (count,) = cur.fetchone()
        if count >= num_rows:
            print(f"Content already seeded ({count} rows)")
            return

        print(f"Seeding {num_rows - count} content rows")
        content_types = ["video", "article", "podcast", "short"]
        for _ in range(num_rows - count):
            title = fake.sentence(nb_words=6)
            ctype = random.choice(content_types)
            length_seconds = random.randint(30, 3600)
            cur.execute(
                """
                INSERT INTO content (title, content_type, length_seconds)
                VALUES (%s, %s, %s)
                """,
                (title, ctype, length_seconds),
            )
    conn.commit()


def stream_engagement_events(conn, sleep_seconds: float = 1.0):
    print("Starting engagement_events generator loop")
    while True:
        with conn.cursor() as cur:
            # choose random content
            cur.execute("SELECT id, length_seconds FROM content ORDER BY random() LIMIT 1;")
            row = cur.fetchone()
            if not row:
                time.sleep(sleep_seconds)
                continue

            content_id, length_seconds = row
            user_id = fake.uuid4()

            # ~20% null duration to exercise NULL logic
            if random.random() < 0.2:
                duration_ms = None
            else:
                max_ms = length_seconds * 1000
                duration_ms = random.randint(1_000, max_ms)

            created_at = datetime.now(tz=timezone.utc)
            cur.execute(
                """
                INSERT INTO engagement_events (user_id, content_id, duration_ms, created_at)
                VALUES (%s, %s, %s, %s)
                """,
                (user_id, content_id, duration_ms, created_at),
            )
        conn.commit()
        time.sleep(sleep_seconds)


def main():
    conn = None
    while conn is None:
        try:
            conn = get_conn()
        except Exception as e:
            print("Waiting for Postgres...", e)
            time.sleep(2)

    seed_content(conn)
    try:
        stream_engagement_events(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()


