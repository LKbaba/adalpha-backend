"""
ADALPHA Backend - FastAPI Application Entry Point

This is the main entry point for the ADALPHA real-time social intelligence backend.
It provides APIs for:
- Kafka connectivity testing
- VKS (Viral Kinetic Score) streaming
- Social media data ingestion
- AI-powered trend analysis
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.api import health, kafka_test, stream, logs, history, crawl, config
from app.services.kafka_client import kafka_client
from app.services.stream_manager import stream_manager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager.
    Handles startup and shutdown events.
    """
    # Startup
    settings = get_settings()
    logger.info(f"Starting {settings.APP_NAME}...")
    logger.info(f"Debug mode: {settings.DEBUG}")

    # Verify Kafka connection on startup
    if settings.KAFKA_BOOTSTRAP_SERVERS:
        try:
            import asyncio
            loop = asyncio.get_event_loop()
            # 在线程池中执行阻塞的 Kafka 操作，避免卡死事件循环
            metadata = await loop.run_in_executor(
                None,
                lambda: kafka_client.get_cluster_metadata(timeout=10.0)
            )
            if metadata["connected"]:
                logger.info(f"Connected to Kafka cluster: {metadata.get('cluster_id')}")
                logger.info(f"Available topics: {metadata.get('topics')}")
                
                # 自动启动 Stream Manager (Kafka consumer)
                await stream_manager.start()
                logger.info("Stream manager started automatically")
            else:
                logger.warning(f"Kafka connection failed: {metadata.get('error')}")
        except Exception as e:
            logger.warning(f"Could not verify Kafka connection: {e}")
    else:
        logger.warning("Kafka not configured - KAFKA_BOOTSTRAP_SERVERS is empty")

    yield

    # Shutdown
    logger.info("Shutting down...")
    await stream_manager.stop()
    kafka_client.close()
    logger.info("Shutdown complete")


# Create FastAPI application
settings = get_settings()

app = FastAPI(
    title=settings.APP_NAME,
    description="""
## ADALPHA Backend API

Real-time social intelligence backend for the ADALPHA Growth Agent.

### Features
- **Kafka Integration**: Connect to Confluent Cloud for real-time data streaming
- **VKS Calculation**: Viral Kinetic Score calculation via Flink SQL
- **Trend Analysis**: AI-powered trend detection and analysis
- **SSE Streaming**: Server-Sent Events for real-time frontend updates

### Architecture
```
TikHub API → Kafka (market-stream) → Flink SQL → Kafka (vks-scores) → SSE → Frontend
```
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",  # Vite dev server
        "http://localhost:3000",  # Alternative dev server
        "https://trendpulse-ai-real-time-social-intelligence-88968107416.us-west1.run.app",  # Production frontend
        "*"  # Allow all for development (restrict in production)
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health.router)
app.include_router(kafka_test.router)
app.include_router(stream.router)
app.include_router(logs.router)
app.include_router(history.router)
app.include_router(crawl.router)
app.include_router(config.router)


# Error handlers
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler for unhandled errors."""
    logger.error(f"Unhandled error: {exc}", exc_info=True)
    return {
        "success": False,
        "error": "Internal server error",
        "detail": str(exc) if settings.DEBUG else "An unexpected error occurred"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG
    )
