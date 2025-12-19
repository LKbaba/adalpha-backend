"""
History API Endpoints - 历史数据查询接口 (Smart History 版本)

基于 SmartHistoryStore 提供历史得分数据的查询和排名功能。
"""

import logging
from typing import Optional, List
from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.services.smart_history_store import smart_history_store

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/history", tags=["History Data"])


# === Response Models ===

class ScoreRecordResponse(BaseModel):
    """得分记录响应 (兼容旧格式)"""
    id: str
    timestamp: str
    platform: str
    hashtag: str
    trend_score: float
    dimensions: dict
    author: str = ""
    title: str = ""
    description: str = ""
    post_id: str = ""
    content_url: str = ""
    cover_url: str = ""
    lifecycle: str = "unknown"
    priority: str = "P3"
    rank: Optional[int] = None
    # 新增字段
    post_count: int = 0
    activity: Optional[dict] = None


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
    oldest_record: Optional[str] = None
    newest_record: Optional[str] = None


# === Helper Functions ===

def _normalize_platform(platform: str) -> str:
    """统一平台名称"""
    p = platform.upper()
    # Twitter -> X
    if p == "TWITTER":
        return "X"
    return p


def _post_to_record(post_data: dict, rank: int) -> dict:
    """将 post 数据转换为前端兼容格式"""
    return {
        "id": post_data["id"],
        "timestamp": post_data.get("last_updated_at", ""),
        "platform": _normalize_platform(post_data["platform"]),
        "hashtag": f"#{post_data['tag']}",
        "trend_score": post_data.get("trend_score", 0),
        "dimensions": post_data.get("dimensions", {}),
        "author": post_data.get("author", ""),
        "title": post_data.get("title") or post_data.get("description", "")[:100] or f"#{post_data['tag']}",
        "description": post_data.get("description", ""),
        "post_id": post_data["post_id"],
        "content_url": post_data.get("content_url", ""),
        "cover_url": post_data.get("cover_url", ""),
        "lifecycle": post_data.get("lifecycle", "unknown"),
        "priority": post_data.get("priority", "P3"),
        "rank": rank,
        "stats": post_data.get("stats", {}),
    }


# === Main Endpoints (使用 Smart History) ===

@router.get("/rankings")
async def get_all_rankings(
    top_n: int = Query(default=100, ge=1, le=500, description="每个平台返回的数量")
):
    """
    获取各平台排名
    
    返回所有平台的 Top N 排名数据，按 trend_score 降序排列。
    """
    # 获取所有帖子（按分数排序）
    all_posts = smart_history_store.get_posts_ranking(limit=2000)
    
    # 按平台分组
    rankings = {}
    for post in all_posts:
        platform = _normalize_platform(post["platform"])
        if platform not in rankings:
            rankings[platform] = {
                "platform": platform,
                "records": [],
                "total": 0
            }
        
        if len(rankings[platform]["records"]) < top_n:
            rank = len(rankings[platform]["records"]) + 1
            record = _post_to_record(post, rank)
            rankings[platform]["records"].append(record)
            rankings[platform]["total"] = len(rankings[platform]["records"])
    
    return rankings


@router.get("/platform/{platform}")
async def get_platform_history(
    platform: str,
    limit: int = Query(default=50, ge=1, le=200, description="返回数量限制")
):
    """
    获取指定平台的历史记录
    """
    posts = smart_history_store.get_posts_ranking(
        platform=platform.lower(),
        limit=limit
    )
    
    records = []
    for rank, post in enumerate(posts, 1):
        records.append(_post_to_record(post, rank))
    
    return records


@router.get("/stats")
async def get_history_stats():
    """
    获取历史数据统计
    """
    stats = smart_history_store.get_stats()
    
    # 获取平台分布（从帖子表）
    all_posts = smart_history_store.get_posts_ranking(limit=1000)
    platforms = {}
    avg_scores = {}
    
    for post in all_posts:
        platform = post["platform"].upper()
        if platform not in platforms:
            platforms[platform] = 0
            avg_scores[platform] = []
        platforms[platform] += 1
        avg_scores[platform].append(post.get("trend_score", 0))
    
    # 计算平均分
    for platform in avg_scores:
        scores_list = avg_scores[platform]
        avg_scores[platform] = round(sum(scores_list) / len(scores_list), 2) if scores_list else 0
    
    return {
        "total_records": stats["total_posts"],
        "retention_hours": 2,
        "platforms": platforms,
        "average_scores": avg_scores,
        "oldest_record": None,
        "newest_record": None,
    }


@router.get("/crawl-summary")
async def get_crawl_summary():
    """
    获取爬取摘要
    """
    stats = smart_history_store.get_stats()
    
    from app.api.crawl import crawl_state
    
    return {
        "total_records": stats["total_tags"],
        "total_posts": stats["total_posts"],
        "platforms_count": stats["platforms"],
        "avg_score": stats["avg_score"],
        "crawl_info": {
            "last_run": crawl_state.last_run.isoformat() if crawl_state.last_run else None,
            "is_running": crawl_state.is_running,
            "last_result": crawl_state.last_result,
        },
    }


# === Tag Detail Endpoints ===

@router.get("/tags/{platform}/{tag}/posts")
async def get_tag_posts(
    platform: str,
    tag: str,
    limit: int = Query(default=30, ge=1, le=100, description="返回数量")
):
    """
    获取某个 Tag 下的帖子列表
    """
    posts = smart_history_store.get_posts_by_tag(
        platform=platform,
        tag=tag,
        limit=limit
    )
    return posts


# === Management Endpoints ===

@router.post("/cleanup")
async def cleanup_history():
    """
    手动触发清理过期数据
    """
    smart_history_store.cleanup_expired()
    stats = smart_history_store.get_stats()
    return {
        "message": "Cleanup completed",
        "stats": stats
    }


@router.delete("/clear")
async def clear_all_history():
    """
    清空所有历史数据（谨慎使用）
    """
    smart_history_store.clear_all()
    return {
        "message": "All history data cleared",
        "stats": smart_history_store.get_stats()
    }
