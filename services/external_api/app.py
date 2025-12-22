from fastapi import FastAPI, Request
from typing import Any, Dict
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("external_api")

app = FastAPI(title="Mock External Ingestion API")


@app.post("/ingest")
async def ingest(payload: Dict[str, Any], request: Request):
    logger.info("Received payload from %s: %s", request.client.host if request.client else "unknown", payload)
    return {"status": "ok"}


