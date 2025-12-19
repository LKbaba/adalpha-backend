"""
History API Endpoints - 历史数据查询接口

提供历史得分数据的查询和排名功能。
"""

import logging
from typing import Optional, List
from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from app.services.history_store import history_store

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/history", tags=["History Data"])


# === Response Models ===

class ScoreRecordResponse(BaseModel):
    """得分记录响应"""
    id: str
    timestamp: str
    platform: str
    hashtag: str
    trend_score: float
    dimensions: dict
    author: str = ""
    description: str = ""
    post_id: str = ""
    lifecycle: str = "unknown"
    priority: str = "P3"
    rank: Optional[int] = None


class PlatformRankingResponse(BaseModel):
    """平台排名响应"""
    platform: str
    records: List[ScoreRecordResponse]
    total: int


class HistoryStatsResponse(BaseModel):
    """历史统计响应"""
    total_records: int
    retention_hours: int
    platforms: dict
    average_scores: dict
    oldest_record: Optional[str]
    newest_record: Optional[str]


class TimeSeriesPoint(BaseModel):
    """时间序列数据点"""
    time: str
    avg_score: float
    max_score: float
    min_score: float
    count: int


# === Endpoints ===

@router.get("/all", response_model=List[ScoreRecordResponse])
async def get_all_history(
    limit: int = Query(default=100, ge=1, le=500, description="返回数量限制"),
    offset: int = Query(default=0, ge=0, description="偏移量"),
    sort_by_score: bool = Query(default=True, description="是否按分数排序")
):
    """
    获取所有历史记录
    
    返回过去2小时内的所有得分记录，支持分页和排序。
    """
    records = history_store.get_all(limit=limit, offset=offset, sort_by_score=sort_by_score)
    return records


@router.get("/platform/{platform}", response_model=List[ScoreRecordResponse])
async def get_platform_history(
    platform: str,
    limit: int = Query(default=50, ge=1, le=200, description="返回数量限制"),
    sort_by_score: bool = Query(default=True, description="是否按分数排序")
):
    """
    获取指定平台的历史记录
    
    返回指定平台过去2小时内的得分记录，带排名。
    
    支持的平台: TIKTOK, INSTAGRAM, TWITTER, YOUTUBE, REDDIT, LINKEDIN, FACEBOOK
    """
    records = history_store.get_by_platform(
        platform=platform,
        limit=limit,
        sort_by_score=sort_by_score
    )
    return records


@router.get("/rankings")
async def get_all_rankings(
    top_n: int = Query(default=20, ge=1, le=100, description="每个平台返回的数量")
):
    """
    获取各平台排名
    
    返回所有平台的 Top N 排名数据，按 trend_score 降序排列。
    """
    rankings = history_store.get_rankings(top_n=top_n)
    
    result = {}
    for platform, records in rankings.items():
        result[platform] = {
            "platform": platform,
            "records": records,
            "total": len(records)
        }
    
    return result


@router.get("/stats", response_model=HistoryStatsResponse)
async def get_history_stats():
    """
    获取历史数据统计
    
    返回存储的统计信息，包括各平台记录数和平均分。
    """
    stats = history_store.get_stats()
    return HistoryStatsResponse(**stats)


@router.get("/crawl-summary")
async def get_crawl_summary():
    """
    获取爬取摘要
    
    返回爬取次数、数据量等统计信息。
    """
    stats = history_store.get_stats()
    
    # 从 crawl API 获取爬取次数
    from app.api.crawl import crawl_state
    
    return {
        "total_records": stats["total_records"],
        "platforms": stats["platforms"],
        "average_scores": stats["average_scores"],
        "retention_hours": stats["retention_hours"],
        "crawl_info": {
            "last_run": crawl_state.last_run.isoformat() if crawl_state.last_run else None,
            "is_running": crawl_state.is_running,
            "last_result": crawl_state.last_result,
        },
        "time_range": {
            "oldest": stats["oldest_record"],
            "newest": stats["newest_record"],
        }
    }


@router.get("/timeseries", response_model=List[TimeSeriesPoint])
async def get_time_series(
    platform: Optional[str] = Query(default=None, description="平台过滤（可选）"),
    minutes: int = Query(default=30, ge=5, le=120, description="时间范围（分钟）"),
    interval: int = Query(default=60, ge=10, le=300, description="聚合间隔（秒）")
):
    """
    获取时间序列数据
    
    返回聚合后的时间序列数据，用于图表展示。
    """
    data = history_store.get_time_series(
        platform=platform,
        minutes=minutes,
        interval_seconds=interval
    )
    return data
