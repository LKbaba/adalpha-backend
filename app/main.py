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
    处理应用启动和关闭事件。

    注意：为了兼容 Cloud Run 部署，Kafka 连接失败不会阻止服务启动。
    """
    # 启动
    settings = get_settings()
    logger.info(f"正在启动 {settings.APP_NAME}...")
    logger.info(f"调试模式: {settings.DEBUG}")

    # 延迟验证 Kafka 连接（非阻塞启动）
    # Cloud Run 要求容器在指定时间内开始监听端口
    if settings.KAFKA_BOOTSTRAP_SERVERS:
        try:
            import asyncio
            loop = asyncio.get_event_loop()

            # 使用较短的超时时间，避免阻塞启动
            # 在后台线程执行 Kafka 连接，设置 5 秒超时
            metadata = await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: kafka_client.get_cluster_metadata(timeout=5.0)
                ),
                timeout=8.0  # 总超时 8 秒，留出足够时间让服务启动
            )

            if metadata["connected"]:
                logger.info(f"已连接到 Kafka 集群: {metadata.get('cluster_id')}")
                logger.info(f"可用 Topics: {metadata.get('topics')}")

                # 在后台启动 Stream Manager，不阻塞主线程
                asyncio.create_task(_start_stream_manager_background())
            else:
                logger.warning(f"Kafka 连接失败: {metadata.get('error')}")
                logger.warning("服务将在没有 Kafka 的情况下运行")
        except asyncio.TimeoutError:
            logger.warning("Kafka 连接超时，服务将继续启动")
            logger.warning("稍后可以通过 /api/health 端点检查 Kafka 状态")
        except Exception as e:
            logger.warning(f"无法验证 Kafka 连接: {e}")
            logger.warning("服务将在没有 Kafka 的情况下运行")
    else:
        logger.warning("Kafka 未配置 - KAFKA_BOOTSTRAP_SERVERS 为空")

    yield

    # 关闭
    logger.info("正在关闭服务...")
    await stream_manager.stop()
    kafka_client.close()
    logger.info("服务已关闭")


async def _start_stream_manager_background():
    """
    在后台启动 Stream Manager。
    这样不会阻塞主应用启动。
    """
    try:
        await stream_manager.start()
        logger.info("Stream Manager 已在后台启动")
    except Exception as e:
        logger.error(f"Stream Manager 启动失败: {e}")


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
