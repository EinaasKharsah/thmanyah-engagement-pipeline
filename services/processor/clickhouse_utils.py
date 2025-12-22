import os
from typing import Any, Dict
from datetime import datetime

from clickhouse_driver import Client


CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "analytics")


def get_client() -> Client:
    return Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=CLICKHOUSE_DB)


def insert_enriched_event(client: Client, event: Dict[str, Any]) -> None:
    client.execute(
        """
        INSERT INTO enriched_events (
            event_id, event_time, user_id, content_id,
            content_type, length_seconds, duration_ms,
            engagement_seconds, engagement_pct
        )
        VALUES
        """,
        [
            (
                event["event_id"],
                datetime.fromisoformat(event["event_time"]),
                event["user_id"],
                event["content_id"],
                event["content_type"],
                int(event["length_seconds"]),
                event.get("duration_ms"),
                event.get("engagement_seconds"),
                event.get("engagement_pct"),
            )
        ],
    )


