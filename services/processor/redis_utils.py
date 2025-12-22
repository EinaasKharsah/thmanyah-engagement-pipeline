import os
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

import redis


REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))


def get_redis_client() -> redis.Redis:
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def bucket_key(event_time: datetime) -> str:
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=timezone.utc)
    minute = event_time.astimezone(timezone.utc).replace(second=0, microsecond=0)
    return f"zset:top:{minute.isoformat(timespec='minutes')}"


def update_bucket(r: redis.Redis, event_time: datetime, content_id: str, engagement_seconds_score: float) -> None:
    key = bucket_key(event_time)
    r.zincrby(key, engagement_seconds_score, content_id)
    # TTL 11 minutes
    r.expire(key, 11 * 60)


def top_n_last_10_minutes(r: redis.Redis, now: datetime, n: int = 10) -> List[Tuple[str, float]]:
    """
    Compute top-N content across last 10 minute buckets.
    Returns list of (content_id, total_score) sorted desc.
    """
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now_minute = now.astimezone(timezone.utc).replace(second=0, microsecond=0)

    keys = []
    for i in range(10):
        minute = now_minute - timedelta(minutes=i)
        keys.append(bucket_key(minute))

    # Aggregate scores in memory
    agg = {}
    for key in keys:
        scores = r.zrevrange(key, 0, -1, withscores=True)
        for member, score in scores:
            agg[member] = agg.get(member, 0.0) + float(score)

    sorted_items = sorted(agg.items(), key=lambda kv: kv[1], reverse=True)
    return sorted_items[:n]


