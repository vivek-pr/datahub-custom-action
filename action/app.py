"""FastAPI entrypoint for the tokenization action."""
from __future__ import annotations

import logging
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .mcl_consumer import MetadataChangeLogConsumer
from .run_manager import RunManager

LOGGER = logging.getLogger(__name__)

app = FastAPI(title="DataHub Tokenization Action", version="0.1.0")
run_manager = RunManager()
consumer = MetadataChangeLogConsumer(run_manager)


class TriggerRequest(BaseModel):
    dataset: str = Field(..., description="Dataset URN to tokenize")
    columns: Optional[List[str]] = Field(None, description="Optional list of column names to tokenize")


@app.on_event("startup")
async def startup_event() -> None:
    LOGGER.info("Starting tokenization service")
    consumer.start()


@app.on_event("shutdown")
async def shutdown_event() -> None:
    LOGGER.info("Stopping tokenization service")
    consumer.stop()
    consumer.join(timeout=5.0)


@app.get("/healthz")
async def health() -> dict:
    return {
        "status": "ok",
        "consumer_running": consumer.is_alive(),
    }


@app.post("/trigger")
async def trigger(request: TriggerRequest) -> dict:
    try:
        result = run_manager.trigger(request.dataset, columns=request.columns)
        return result
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("Manual trigger failed for %s", request.dataset)
        raise HTTPException(status_code=500, detail=str(exc))
