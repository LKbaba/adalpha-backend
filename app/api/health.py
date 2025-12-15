"""
Health check API endpoints.
Provides service health status for monitoring and load balancing.
"""

from datetime import datetime
from fastapi import APIRouter

from app.models.schemas import HealthResponse
from app.services.kafka_client import kafka_client
from app.config import get_settings

router = APIRouter(tags=["Health"])


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Health Check",
    description="Check the health status of the backend service and its dependencies."
)
async def health_check() -> HealthResponse:
    """
    Comprehensive health check endpoint.

    Checks:
    - API service status
    - Kafka connectivity
    - Configuration validity

    Returns:
        HealthResponse: Service health status with dependency details
    """
    settings = get_settings()
    services = {}

    # Check Kafka connectivity
    try:
        kafka_metadata = kafka_client.get_cluster_metadata(timeout=5.0)
        services["kafka"] = {
            "status": "healthy" if kafka_metadata["connected"] else "unhealthy",
            "cluster_id": kafka_metadata.get("cluster_id"),
            "topics_count": len(kafka_metadata.get("topics", []))
        }
    except Exception as e:
        services["kafka"] = {
            "status": "unhealthy",
            "error": str(e)
        }

    # Check configuration
    services["config"] = {
        "status": "healthy" if settings.KAFKA_BOOTSTRAP_SERVERS else "unhealthy",
        "kafka_configured": bool(settings.KAFKA_BOOTSTRAP_SERVERS),
        "tikhub_configured": bool(settings.TIKHUB_API_KEY),
        "gemini_configured": bool(settings.GEMINI_API_KEY)
    }

    # Determine overall status
    all_healthy = all(
        svc.get("status") == "healthy"
        for svc in services.values()
    )

    return HealthResponse(
        status="healthy" if all_healthy else "degraded",
        timestamp=datetime.utcnow(),
        version="1.0.0",
        services=services
    )


@router.get(
    "/",
    summary="Root",
    description="API root endpoint with basic service info."
)
async def root():
    """Root endpoint returning basic API information."""
    settings = get_settings()
    return {
        "service": settings.APP_NAME,
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs",
        "health": "/health"
    }
