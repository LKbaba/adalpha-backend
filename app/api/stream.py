"""
SSE Streaming API Endpoints.

Provides Server-Sent Events (SSE) endpoints for real-time
data streaming to frontend clients.

Usage:
```javascript
const eventSource = new EventSource('/api/stream/vks');
eventSource.addEventListener('vks_update', (event) => {
    const data = JSON.parse(event.data);
    updateVKSChart(data);
});
```
"""

import logging
import uuid
from typing import Optional

from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.services.stream_manager import (
    stream_manager,
    sse_event_generator
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stream", tags=["Real-time Streaming"])


# === Response Models ===

class StreamStatusResponse(BaseModel):
    """Response model for stream status."""
    running: bool = Field(description="Whether stream manager is running")
    client_count: int = Field(description="Number of connected clients")
    clients: list = Field(default_factory=list, description="List of connected clients")


class StreamStartResponse(BaseModel):
    """Response for starting stream manager."""
    success: bool
    message: str
    status: Optional[StreamStatusResponse] = None


# === SSE Endpoints ===

@router.get("/vks")
async def stream_vks_scores(request: Request):
    """
    SSE endpoint for VKS score updates.

    Streams real-time Viral Kinetic Score updates from Kafka.
    Connect using EventSource:

    ```javascript
    const es = new EventSource('/api/stream/vks');
    es.addEventListener('vks_update', (e) => {
        const data = JSON.parse(e.data);
        console.log('VKS Update:', data);
    });
    ```

    Events:
    - `connected`: Initial connection confirmation
    - `vks_update`: VKS score update from Flink SQL
    - `heartbeat`: Keep-alive ping (every 15s)
    """
    client_id = f"vks-{uuid.uuid4().hex[:8]}"

    logger.info(f"SSE client connected: {client_id}")

    return StreamingResponse(
        sse_event_generator(client_id, topics={"vks-scores"}),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable nginx buffering
            "Access-Control-Allow-Origin": "*"
        }
    )


@router.get("/trends")
async def stream_trends(request: Request):
    """
    SSE endpoint for trend updates.

    Streams real-time market/trend updates from the market-stream topic.

    Events:
    - `connected`: Initial connection confirmation
    - `trend_update`: New trend data from TikHub or demo generator
    - `heartbeat`: Keep-alive ping (every 15s)
    """
    client_id = f"trends-{uuid.uuid4().hex[:8]}"

    logger.info(f"SSE trends client connected: {client_id}")

    return StreamingResponse(
        sse_event_generator(client_id, topics={"market-stream"}),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "Access-Control-Allow-Origin": "*"
        }
    )


@router.get("/all")
async def stream_all(request: Request):
    """
    SSE endpoint for all events.

    Streams all real-time updates including VKS scores and trend data.

    Events:
    - `connected`: Initial connection confirmation
    - `vks_update`: VKS score updates
    - `trend_update`: New trend data
    - `heartbeat`: Keep-alive ping (every 15s)
    """
    client_id = f"all-{uuid.uuid4().hex[:8]}"

    logger.info(f"SSE all-events client connected: {client_id}")

    return StreamingResponse(
        sse_event_generator(client_id, topics={"vks-scores", "market-stream"}),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "Access-Control-Allow-Origin": "*"
        }
    )


# === Management Endpoints ===

@router.get("/status", response_model=StreamStatusResponse)
async def get_stream_status():
    """
    Get stream manager status.

    Returns information about the stream manager including
    number of connected clients.
    """
    stats = stream_manager.get_stats()
    return StreamStatusResponse(**stats)


@router.post("/start", response_model=StreamStartResponse)
async def start_stream_manager():
    """
    Manually start the stream manager.

    The stream manager starts automatically when the first client
    connects, but this endpoint allows manual control.
    """
    if stream_manager.is_running:
        return StreamStartResponse(
            success=False,
            message="Stream manager is already running",
            status=StreamStatusResponse(**stream_manager.get_stats())
        )

    await stream_manager.start()

    return StreamStartResponse(
        success=True,
        message="Stream manager started",
        status=StreamStatusResponse(**stream_manager.get_stats())
    )


@router.post("/stop", response_model=StreamStartResponse)
async def stop_stream_manager():
    """
    Stop the stream manager.

    Warning: This will disconnect all SSE clients.
    """
    if not stream_manager.is_running:
        return StreamStartResponse(
            success=False,
            message="Stream manager is not running"
        )

    await stream_manager.stop()

    return StreamStartResponse(
        success=True,
        message="Stream manager stopped. All clients disconnected."
    )


@router.post("/broadcast")
async def broadcast_test_message(
    event_type: str = Query(default="test", description="Event type"),
    message: str = Query(default="Test broadcast", description="Message content")
):
    """
    Broadcast a test message to all connected clients.

    Useful for testing SSE connections.
    """
    if not stream_manager.is_running or stream_manager.client_count == 0:
        return {
            "success": False,
            "message": "No clients connected"
        }

    stream_manager.broadcast(event_type, {
        "message": message,
        "source": "manual_broadcast"
    })

    return {
        "success": True,
        "message": f"Broadcasted to {stream_manager.client_count} clients"
    }


@router.get("/kafka-debug")
async def kafka_debug():
    """
    Debug Kafka connection and topic status.
    """
    from app.services.kafka_client import kafka_client
    
    try:
        metadata = kafka_client.get_cluster_metadata(timeout=5.0)
        return {
            "kafka_connected": metadata.get("connected", False),
            "cluster_id": metadata.get("cluster_id"),
            "topics": metadata.get("topics", []),
            "broker_count": metadata.get("broker_count", 0),
            "stream_manager_running": stream_manager.is_running,
            "sse_client_count": stream_manager.client_count,
        }
    except Exception as e:
        return {
            "kafka_connected": False,
            "error": str(e),
            "stream_manager_running": stream_manager.is_running,
            "sse_client_count": stream_manager.client_count,
        }


@router.post("/test-vks")
async def send_test_vks():
    """
    Send a test VKS update to all connected clients.
    Useful for testing the SSE pipeline without Kafka.
    """
    import random
    
    if not stream_manager.is_running or stream_manager.client_count == 0:
        return {
            "success": False,
            "message": "No clients connected"
        }
    
    test_data = {
        "hashtag": "#TestTrend",
        "vks_score": random.uniform(30, 90),
        "trend_score": random.uniform(30, 90),
        "platform": random.choice(["TIKTOK", "TWITTER", "REDDIT", "YOUTUBE"]),
        "source": "test_endpoint",
        "dimensions": {
            "H": random.uniform(0, 100),
            "V": random.uniform(0, 100),
            "D": random.uniform(0, 100),
            "F": random.uniform(0, 100),
            "M": random.uniform(0, 100),
            "R": random.uniform(0, 100),
        }
    }
    
    stream_manager.broadcast("vks_update", test_data, "vks-scores")
    
    return {
        "success": True,
        "message": f"Test VKS sent to {stream_manager.client_count} clients",
        "data": test_data
    }
