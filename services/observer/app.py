import os
from datetime import datetime, timezone
from typing import List, Dict, Any

import httpx
import redis
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="Pipeline Observer")

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))


async def get_clickhouse_count() -> int:
    """Query ClickHouse for total enriched events count."""
    try:
        url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
        query = "SELECT count() FROM analytics.enriched_events"
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(url, params={"query": query})
            if resp.status_code == 200:
                return int(resp.text.strip())
    except Exception:
        pass
    return 0


def get_redis_buckets() -> List[str]:
    """List all Redis bucket keys matching zset:top:*."""
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        keys = r.keys("zset:top:*")
        return sorted(keys, reverse=True)  # Latest first
    except Exception:
        pass
    return []


def get_top_content_from_latest_bucket() -> List[Dict[str, Any]]:
    """Get top content from the latest Redis bucket."""
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        buckets = get_redis_buckets()
        if not buckets:
            return []
        
        latest_key = buckets[0]
        # Get top 10 with scores
        items = r.zrevrange(latest_key, 0, 9, withscores=True)
        return [
            {"content_id": content_id, "score": round(float(score), 2)}
            for content_id, score in items
        ]
    except Exception:
        pass
    return []


@app.get("/status")
async def status():
    """Return pipeline status as JSON."""
    return {
        "clickhouse_count": await get_clickhouse_count(),
        "redis_bucket_keys": get_redis_buckets(),
        "latest_top_content": get_top_content_from_latest_bucket(),
    }


@app.get("/")
async def index():
    """Serve the observer UI."""
    return FileResponse("static/index.html")

