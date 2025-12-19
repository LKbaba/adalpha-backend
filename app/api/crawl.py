"""
Crawl API Endpoints - 爬虫触发接口

提供手动触发爬虫的 API，支持前端在加载页面时触发爬取流程。
通过调用 spider6p 服务器 (http://localhost:8001) 来执行爬取。
"""

import logging
import asyncio
import aiohttp
from typing import Optional, List
from datetime import datetime
from fastapi import APIRouter, Query, BackgroundTasks
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/crawl", tags=["Crawler Control"])

# Spider6P 服务器地址
SPIDER_SERVER_URL = "http://localhost:8001"


# === State ===
class CrawlState:
    """爬虫状态管理"""

    def __init__(self):
        self.is_running = False
        self.last_run: Optional[datetime] = None
        self.last_result: Optional[dict] = None
        self.progress: int = 0
        self.current_platform: str = ""
        self.error: Optional[str] = None
        self.trigger_count: int = 0  # 触发计数器


crawl_state = CrawlState()


# === Response Models ===


class CrawlStatusResponse(BaseModel):
    """爬虫状态响应"""

    is_running: bool
    progress: int = Field(ge=0, le=100, description="进度百分比")
    current_platform: str = ""
    last_run: Optional[str] = None
    error: Optional[str] = None


class CrawlTriggerResponse(BaseModel):
    """触发爬虫响应"""

    success: bool
    message: str
    status: CrawlStatusResponse


# === Background Task ===


async def run_crawler_task(tags: List[str], mock: bool = True):
    """
    后台运行爬虫任务

    通过调用 spider6p 服务器的 API 来执行爬取
    
    Args:
        tags: 标签列表
        mock: 是否使用 Mock 模式（使用已有数据，不消耗 API）
    """
    global crawl_state

    try:
        crawl_state.is_running = True
        crawl_state.progress = 10
        crawl_state.error = None
        crawl_state.current_platform = "CONNECTING"

        mode_text = "Mock 模式" if mock else "真实爬取"
        logger.info(f"Triggering spider6p ({mode_text}) with tags: {tags}")

        # 调用 spider6p 服务器
        async with aiohttp.ClientSession() as session:
            crawl_state.current_platform = "CRAWLING"
            crawl_state.progress = 20

            try:
                # 统一调用 /run 端点，由爬虫端的 config.useMock 决定模式
                url = f"{SPIDER_SERVER_URL}/run"
                payload = {}
                logger.info(f"Calling spider6p: POST {url}")

                async with session.post(
                    url, json=payload, timeout=aiohttp.ClientTimeout(total=180)
                ) as response:
                    result = await response.json()

                    if response.status == 200 and result.get("success"):
                        crawl_state.progress = 100
                        crawl_state.current_platform = "COMPLETE"
                        crawl_state.last_result = result
                        logger.info(f"Spider6p completed successfully ({mode_text}): {result}")
                    elif response.status == 409:
                        # 爬虫正在运行
                        crawl_state.current_platform = "ALREADY_RUNNING"
                        crawl_state.error = result.get("message", "Crawler already running")
                        logger.warning(f"Spider6p already running: {result}")
                    else:
                        crawl_state.error = result.get("message", "Unknown error")
                        crawl_state.current_platform = "ERROR"
                        logger.error(f"Spider6p failed: {result}")

            except aiohttp.ClientConnectorError:
                # 爬虫服务器未启动
                crawl_state.error = "Spider6p server not running (localhost:8001)"
                crawl_state.current_platform = "SERVER_OFFLINE"
                logger.error(
                    "Spider6p server not running. Start it with: cd spider6p && npm run server"
                )

            except asyncio.TimeoutError:
                crawl_state.error = "Crawler timeout (180s)"
                crawl_state.current_platform = "TIMEOUT"
                logger.error("Spider6p request timeout")

        crawl_state.last_run = datetime.utcnow()

    except Exception as e:
        logger.error(f"Crawl task failed: {e}", exc_info=True)
        crawl_state.error = str(e)
        crawl_state.current_platform = "ERROR"

    finally:
        crawl_state.is_running = False


# === Endpoints ===


@router.get("/status", response_model=CrawlStatusResponse)
async def get_crawl_status():
    """
    获取爬虫状态

    返回当前爬虫的运行状态、进度和最后运行时间。
    同时会尝试从 spider6p 服务器获取实时状态。
    """
    # 尝试从 spider6p 获取实时状态
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{SPIDER_SERVER_URL}/status",
                timeout=aiohttp.ClientTimeout(total=5),
            ) as response:
                if response.status == 200:
                    spider_status = await response.json()
                    return CrawlStatusResponse(
                        is_running=spider_status.get("running", False),
                        progress=100 if spider_status.get("lastResult") else 0,
                        current_platform=spider_status.get("config", {}).get(
                            "platforms", [""]
                        )[0]
                        if spider_status.get("running")
                        else "IDLE",
                        last_run=spider_status.get("lastRun"),
                        error=None,
                    )
    except Exception:
        pass  # 如果无法连接，使用本地状态

    return CrawlStatusResponse(
        is_running=crawl_state.is_running,
        progress=crawl_state.progress,
        current_platform=crawl_state.current_platform,
        last_run=crawl_state.last_run.isoformat() if crawl_state.last_run else None,
        error=crawl_state.error,
    )


@router.post("/trigger", response_model=CrawlTriggerResponse)
async def trigger_crawl(
    background_tasks: BackgroundTasks,
    tags: Optional[str] = Query(
        default="AI,trending", description="逗号分隔的标签列表"
    ),
    mock: bool = Query(
        default=True, description="是否使用 Mock 模式（使用已有数据，不消耗 API 费用）"
    ),
):
    """
    触发爬虫

    启动后台爬虫任务，返回立即响应。
    使用 /status 端点轮询进度。

    参数:
    - tags: 逗号分隔的标签列表
    - mock: 是否使用 Mock 模式（默认 True，使用已有数据不消耗 API）

    注意：需要先启动 spider6p 服务器：
    ```
    cd spider6p && npm run server
    ```

    示例:
    - POST /api/crawl/trigger?tags=AI,NFT&mock=true  (Mock 模式)
    - POST /api/crawl/trigger?tags=AI,NFT&mock=false (真实爬取)
    """
    if crawl_state.is_running:
        return CrawlTriggerResponse(
            success=False,
            message="Crawler is already running",
            status=CrawlStatusResponse(
                is_running=True,
                progress=crawl_state.progress,
                current_platform=crawl_state.current_platform,
                last_run=crawl_state.last_run.isoformat()
                if crawl_state.last_run
                else None,
                error=crawl_state.error,
            ),
        )

    tag_list = [t.strip() for t in tags.split(",") if t.strip()]

    # 增加触发计数
    crawl_state.trigger_count += 1
    logger.info(f"🔢 Crawl trigger count: {crawl_state.trigger_count}, mock={mock}")

    # 启动后台任务
    background_tasks.add_task(run_crawler_task, tag_list, mock)

    mode_text = "Mock 模式" if mock else "真实爬取"
    return CrawlTriggerResponse(
        success=True,
        message=f"Crawler started ({mode_text}) for tags: {tag_list}",
        status=CrawlStatusResponse(
            is_running=True,
            progress=0,
            current_platform="INITIALIZING",
            last_run=crawl_state.last_run.isoformat() if crawl_state.last_run else None,
            error=None,
        ),
    )


@router.post("/stop")
async def stop_crawl():
    """
    停止爬虫

    注意：spider6p 目前不支持中途停止，此接口仅重置本地状态。
    """
    crawl_state.is_running = False
    crawl_state.current_platform = "STOPPED"

    return {"success": True, "message": "Local state reset. Note: spider6p cannot be stopped mid-crawl."}


@router.get("/trigger-count")
async def get_trigger_count():
    """
    获取爬虫触发次数（用于调试）
    """
    return {
        "trigger_count": crawl_state.trigger_count,
        "is_running": crawl_state.is_running,
        "last_run": crawl_state.last_run.isoformat() if crawl_state.last_run else None,
    }


@router.post("/reset-count")
async def reset_trigger_count():
    """
    重置触发计数器（用于调试）
    """
    crawl_state.trigger_count = 0
    return {"success": True, "message": "Trigger count reset to 0"}


@router.get("/health")
async def check_spider_health():
    """
    检查 spider6p 服务器健康状态
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{SPIDER_SERVER_URL}/health",
                timeout=aiohttp.ClientTimeout(total=5),
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        "spider_server": "online",
                        "url": SPIDER_SERVER_URL,
                        "timestamp": data.get("timestamp"),
                    }
    except aiohttp.ClientConnectorError:
        return {
            "spider_server": "offline",
            "url": SPIDER_SERVER_URL,
            "error": "Cannot connect to spider6p server",
            "hint": "Start it with: cd spider6p && npm run server",
        }
    except Exception as e:
        return {
            "spider_server": "error",
            "url": SPIDER_SERVER_URL,
            "error": str(e),
        }
