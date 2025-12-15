"""
Pydantic schemas for API request/response models.
Defines data transfer objects (DTOs) for the ADALPHA backend.
"""

from datetime import datetime
from typing import Optional, List, Any
from pydantic import BaseModel, Field


# === Health Check Schemas ===

class HealthResponse(BaseModel):
    """Health check response model."""
    status: str = Field(description="Service status (healthy/unhealthy)")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    version: str = Field(default="1.0.0", description="API version")
    services: dict = Field(default_factory=dict, description="Service status details")


# === Kafka Schemas ===

class KafkaStatusResponse(BaseModel):
    """Kafka connection status response."""
    connected: bool = Field(description="Whether connected to Confluent Cloud")
    cluster_id: Optional[str] = Field(default=None, description="Kafka cluster ID")
    topics: List[str] = Field(default_factory=list, description="Available topics")
    broker_count: int = Field(default=0, description="Number of brokers")
    message: str = Field(description="Status message")


class KafkaProduceRequest(BaseModel):
    """Request model for producing a Kafka message."""
    topic: str = Field(description="Target Kafka topic")
    message: str = Field(description="Message content")
    key: Optional[str] = Field(default=None, description="Optional message key")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "topic": "market-stream",
                    "message": '{"hashtag": "#AIArt", "views": 1500000}',
                    "key": "trend-001"
                }
            ]
        }
    }


class KafkaProduceResponse(BaseModel):
    """Response model for Kafka produce operation."""
    success: bool = Field(description="Whether message was sent successfully")
    topic: str = Field(description="Topic message was sent to")
    partition: Optional[int] = Field(default=None, description="Partition number")
    offset: Optional[int] = Field(default=None, description="Message offset")
    message: str = Field(description="Result message")


# === VKS (Viral Kinetic Score) Schemas ===

class VKSDataPoint(BaseModel):
    """Single VKS data point for real-time streaming."""
    timestamp: datetime = Field(description="Data point timestamp")
    hashtag: str = Field(description="Tracked hashtag or keyword")
    vks_score: float = Field(ge=0, le=100, description="VKS score (0-100)")
    velocity: float = Field(description="Growth velocity")
    acceleration: float = Field(description="Growth acceleration")
    prediction: float = Field(description="Predicted future value")
    confidence: float = Field(ge=0, le=1, description="Confidence level (0-1)")


class TrendData(BaseModel):
    """Social media trend data from ingestion."""
    hashtag: str = Field(description="Hashtag or keyword")
    platform: str = Field(description="Source platform (tiktok, instagram, etc)")
    views: int = Field(ge=0, description="Total views")
    likes: int = Field(ge=0, description="Total likes")
    comments: int = Field(ge=0, description="Total comments")
    shares: int = Field(ge=0, description="Total shares")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    metadata: Optional[dict] = Field(default=None, description="Additional metadata")


class MarketStreamMessage(BaseModel):
    """
    Message format for market-stream Kafka topic.
    This is the raw data ingested from social media APIs.
    """
    event_id: str = Field(description="Unique event identifier")
    event_type: str = Field(default="trend_update", description="Event type")
    data: TrendData = Field(description="Trend data payload")
    source: str = Field(default="tikhub", description="Data source")
    ingested_at: datetime = Field(default_factory=datetime.utcnow)


class VKSScoreMessage(BaseModel):
    """
    Message format for vks-scores Kafka topic.
    This is the output from Flink SQL processing.
    """
    event_id: str = Field(description="Unique event identifier")
    hashtag: str = Field(description="Tracked hashtag")
    vks_score: float = Field(description="Calculated VKS score")
    components: dict = Field(description="Score components breakdown")
    window_start: datetime = Field(description="Calculation window start")
    window_end: datetime = Field(description="Calculation window end")
    calculated_at: datetime = Field(default_factory=datetime.utcnow)


# === API Response Wrappers ===

class APIResponse(BaseModel):
    """Generic API response wrapper."""
    success: bool = Field(description="Whether request succeeded")
    data: Optional[Any] = Field(default=None, description="Response data")
    message: str = Field(default="", description="Response message")
    errors: Optional[List[str]] = Field(default=None, description="Error messages if any")


class PaginatedResponse(BaseModel):
    """Paginated API response."""
    items: List[Any] = Field(description="List of items")
    total: int = Field(description="Total number of items")
    page: int = Field(ge=1, description="Current page number")
    page_size: int = Field(ge=1, le=100, description="Items per page")
    has_next: bool = Field(description="Whether there are more pages")
