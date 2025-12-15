"""
Kafka test API endpoints.
Used to verify Confluent Cloud connectivity and basic produce/consume operations.
"""

import logging
from fastapi import APIRouter, HTTPException

from app.models.schemas import (
    KafkaStatusResponse,
    KafkaProduceRequest,
    KafkaProduceResponse,
)
from app.services.kafka_client import kafka_client

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/test/kafka", tags=["Kafka Test"])


@router.get(
    "/status",
    response_model=KafkaStatusResponse,
    summary="Kafka Connection Status",
    description="Check if the backend can connect to Confluent Cloud."
)
async def get_kafka_status() -> KafkaStatusResponse:
    """
    Check Kafka connection status.

    Returns:
        KafkaStatusResponse: Connection status and cluster info
    """
    try:
        metadata = kafka_client.get_cluster_metadata()

        return KafkaStatusResponse(
            connected=metadata["connected"],
            cluster_id=metadata.get("cluster_id"),
            topics=metadata.get("topics", []),
            broker_count=metadata.get("broker_count", 0),
            message="Connected to Confluent Cloud" if metadata["connected"]
                    else f"Connection failed: {metadata.get('error', 'Unknown error')}"
        )
    except Exception as e:
        logger.error(f"Error checking Kafka status: {e}")
        return KafkaStatusResponse(
            connected=False,
            cluster_id=None,
            topics=[],
            broker_count=0,
            message=f"Error: {str(e)}"
        )


@router.post(
    "/produce",
    response_model=KafkaProduceResponse,
    summary="Send Test Message",
    description="Send a test message to a Kafka topic to verify producer functionality."
)
async def produce_test_message(request: KafkaProduceRequest) -> KafkaProduceResponse:
    """
    Send a test message to Kafka.

    Args:
        request: Message details including topic, content, and optional key

    Returns:
        KafkaProduceResponse: Result of the produce operation
    """
    result = {"partition": None, "offset": None, "error": None}

    def callback(err, msg):
        if err:
            result["error"] = str(err)
        else:
            result["partition"] = msg.partition()
            result["offset"] = msg.offset()

    try:
        success = kafka_client.produce_message(
            topic=request.topic,
            value=request.message,
            key=request.key,
            callback=callback
        )

        # Flush to ensure delivery
        kafka_client.flush_producer(timeout=5.0)

        if success and result["error"] is None:
            return KafkaProduceResponse(
                success=True,
                topic=request.topic,
                partition=result["partition"],
                offset=result["offset"],
                message=f"Message sent successfully to {request.topic}"
            )
        else:
            return KafkaProduceResponse(
                success=False,
                topic=request.topic,
                partition=None,
                offset=None,
                message=f"Failed to send message: {result.get('error', 'Unknown error')}"
            )

    except Exception as e:
        logger.error(f"Error producing message: {e}")
        raise HTTPException(status_code=500, detail=f"Kafka produce error: {str(e)}")


@router.post(
    "/produce/{topic}",
    response_model=KafkaProduceResponse,
    summary="Send Message to Specific Topic",
    description="Send a test message to a specific Kafka topic using path parameter."
)
async def produce_to_topic(
    topic: str,
    message: str = "Hello from ADALPHA!"
) -> KafkaProduceResponse:
    """
    Simplified endpoint to send a message to a specific topic.

    Args:
        topic: Target topic name (path parameter)
        message: Message content (query parameter, defaults to test message)

    Returns:
        KafkaProduceResponse: Result of the produce operation
    """
    request = KafkaProduceRequest(topic=topic, message=message)
    return await produce_test_message(request)


@router.get(
    "/topics",
    summary="List Topics",
    description="List all available Kafka topics in the cluster."
)
async def list_topics():
    """
    List all available Kafka topics.

    Returns:
        dict: List of topic names
    """
    try:
        metadata = kafka_client.get_cluster_metadata()
        if metadata["connected"]:
            return {
                "success": True,
                "topics": metadata.get("topics", []),
                "count": len(metadata.get("topics", []))
            }
        else:
            return {
                "success": False,
                "topics": [],
                "error": metadata.get("error", "Not connected")
            }
    except Exception as e:
        logger.error(f"Error listing topics: {e}")
        raise HTTPException(status_code=500, detail=str(e))
