"""
自适应 Trend Score 计算引擎

根据各平台返回的不同数据特征，自动调整算法权重和计算方式。

支持平台:
- TikTok: views, likes, comments, shares, saves
- Instagram: likes, comments, views, shares (部分字段可能为空)
- Reddit: upvotes, downvotes, score, comments
- YouTube: views, likes, comments (未来支持)
- X/Twitter: likes, retweets, replies, views (未来支持)
- 小红书: likes, comments, collects, shares (未来支持)

公式: trend_score = 0.20*H + 0.30*V + 0.15*D + 0.15*F + 0.20*M - 0.25*R
结果范围: 0-100
"""

import math
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class Platform(Enum):
    """支持的平台枚举"""
    TIKTOK = "tiktok"
    INSTAGRAM = "instagram"
    REDDIT = "reddit"
    YOUTUBE = "youtube"
    TWITTER = "twitter"
    LINKEDIN = "linkedin"  # LinkedIn 数据结构不同，仅基础支持
    UNKNOWN = "unknown"


@dataclass
class PlatformMetricMapping:
    """平台指标映射配置"""
    # 主要指标映射 (平台字段 -> 标准字段)
    views_field: Optional[str] = None
    likes_field: Optional[str] = None
    comments_field: Optional[str] = None
    shares_field: Optional[str] = None
    saves_field: Optional[str] = None
    posts_field: Optional[str] = None
    
    # 特殊指标 (Reddit等)
    upvotes_field: Optional[str] = None
    downvotes_field: Optional[str] = None
    score_field: Optional[str] = None
    
    # 权重调整因子 (用于补偿缺失字段)
    views_weight: float = 1.0
    likes_weight: float = 1.0
    comments_weight: float = 1.0
    shares_weight: float = 1.0
    saves_weight: float = 1.0
    
    # 热度计算权重
    hotness_views_weight: float = 0.50
    hotness_engagement_weight: float = 0.30
    hotness_posts_weight: float = 0.20


# 平台配置
# 基于实际爬虫数据结构配置
# TikTok stats: views, likes, comments, shares, saves
# Instagram stats: views, likes, comments, shares (数据可能为0)
# Twitter stats: views, likes, comments, retweets, quotes, bookmarks
# YouTube stats: views, likes, comments (likes/comments 可能为0)
# LinkedIn stats: services (人员资料，不适合趋势评分)

PLATFORM_CONFIGS: Dict[Platform, PlatformMetricMapping] = {
    Platform.TIKTOK: PlatformMetricMapping(
        views_field="views",
        likes_field="likes",
        comments_field="comments",
        shares_field="shares",
        saves_field="saves",
        # TikTok 数据最完整，使用标准权重
        views_weight=1.0,
        likes_weight=1.0,
        comments_weight=1.0,
        shares_weight=1.0,
        saves_weight=1.0,
        hotness_views_weight=0.50,
        hotness_engagement_weight=0.30,
        hotness_posts_weight=0.20,
    ),
    Platform.INSTAGRAM: PlatformMetricMapping(
        views_field="views",
        likes_field="likes",
        comments_field="comments",
        shares_field="shares",
        saves_field=None,  # Instagram 爬虫数据没有 saves
        # Instagram 数据可能不完整（很多为0），调整权重
        views_weight=1.0,
        likes_weight=1.2,  # 提高 likes 权重补偿
        comments_weight=1.3,  # 提高 comments 权重补偿
        shares_weight=1.1,
        saves_weight=0.0,
        # Instagram 互动数据可能不完整，调整热度权重
        hotness_views_weight=0.35,
        hotness_engagement_weight=0.45,  # 更依赖互动
        hotness_posts_weight=0.20,
    ),
    Platform.YOUTUBE: PlatformMetricMapping(
        views_field="views",
        likes_field="likes",
        comments_field="comments",
        shares_field=None,  # YouTube 爬虫数据没有 shares
        saves_field=None,   # YouTube 爬虫数据没有 saves
        # YouTube likes/comments 可能为0，主要看 views
        views_weight=1.3,   # YouTube 以播放量为核心
        likes_weight=1.0,
        comments_weight=1.5,  # YouTube 评论更有价值
        shares_weight=0.0,
        saves_weight=0.0,
        hotness_views_weight=0.65,  # 播放量占主导
        hotness_engagement_weight=0.20,
        hotness_posts_weight=0.15,
    ),
    Platform.TWITTER: PlatformMetricMapping(
        views_field="views",
        likes_field="likes",
        comments_field="comments",  # 爬虫已映射 replies -> comments
        shares_field="retweets",
        saves_field="bookmarks",
        # Twitter 数据较完整
        views_weight=0.8,   # Twitter views 可能包含无效曝光
        likes_weight=1.0,
        comments_weight=1.2,
        shares_weight=1.5,  # 转发在 Twitter 非常重要
        saves_weight=0.8,
        hotness_views_weight=0.40,
        hotness_engagement_weight=0.40,
        hotness_posts_weight=0.20,
    ),
    Platform.REDDIT: PlatformMetricMapping(
        # Reddit stats: upvotes, downvotes, score, comments
        views_field=None,  # Reddit 没有 views
        likes_field=None,  # Reddit 用 upvotes/score
        comments_field="comments",
        shares_field=None,  # Reddit 没有 shares
        saves_field=None,   # Reddit 没有 saves
        upvotes_field="upvotes",
        downvotes_field="downvotes",
        score_field="score",
        # Reddit 主要依赖 score 和 comments
        views_weight=0.0,
        likes_weight=0.0,
        comments_weight=2.0,  # 大幅提高 comments 权重
        shares_weight=0.0,
        saves_weight=0.0,
        # Reddit 热度主要看 score 和 comments
        hotness_views_weight=0.0,
        hotness_engagement_weight=0.70,
        hotness_posts_weight=0.30,
    ),
    Platform.LINKEDIN: PlatformMetricMapping(
        # LinkedIn 返回的是人员资料，不是帖子
        # stats 只有 services 字段，无法计算趋势分数
        views_field=None,
        likes_field=None,
        comments_field=None,
        shares_field=None,
        saves_field=None,
        views_weight=0.0,
        likes_weight=0.0,
        comments_weight=0.0,
        shares_weight=0.0,
        saves_weight=0.0,
        # LinkedIn 数据不适合趋势评分，给予最低权重
        hotness_views_weight=0.0,
        hotness_engagement_weight=0.0,
        hotness_posts_weight=1.0,  # 只能看帖子数量
    ),
}


# 权重配置
WEIGHTS = {
    "H": 0.20,  # 热度
    "V": 0.30,  # 增速 (最高权重)
    "M": 0.20,  # 商业化
    "D": 0.15,  # 密度
    "F": 0.15,  # 可行性
    "R": 0.25,  # 风险 (惩罚项)
}

# 增长率权重 (标准)
VELOCITY_WEIGHTS = {
    "views": 0.45,
    "likes": 0.25,
    "comments": 0.15,
    "shares": 0.10,
    "saves": 0.05,
}

# 默认关键词配置
DEFAULT_KEYWORD_PROFILES = {
    "ai headshot": {"ai_feasibility": 5, "monetization": 0.9, "ip_risk": 0.1, "category": "portrait"},
    "ai manga filter": {"ai_feasibility": 4, "monetization": 0.8, "ip_risk": 0.3, "category": "filter"},
    "background remover": {"ai_feasibility": 5, "monetization": 0.85, "ip_risk": 0.1, "category": "tool"},
    "image upscaler": {"ai_feasibility": 5, "monetization": 0.8, "ip_risk": 0.1, "category": "tool"},
    "anime filter": {"ai_feasibility": 4, "monetization": 0.7, "ip_risk": 0.2, "category": "filter"},
    "ghibli filter": {"ai_feasibility": 4, "monetization": 0.7, "ip_risk": 0.5, "category": "filter"},
    "arcane filter": {"ai_feasibility": 4, "monetization": 0.7, "ip_risk": 0.6, "category": "filter"},
    "music": {"ai_feasibility": 3, "monetization": 0.5, "ip_risk": 0.3, "category": "general"},
    "dance": {"ai_feasibility": 3, "monetization": 0.5, "ip_risk": 0.2, "category": "general"},
    "fashion": {"ai_feasibility": 3, "monetization": 0.6, "ip_risk": 0.2, "category": "general"},
}


def clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    """将值限制在指定范围内"""
    return max(lo, min(hi, x))


def safe_growth(curr: float, prev: float) -> float:
    """安全计算增长率，避免除零"""
    if prev is None or prev <= 0:
        return 0.0
    return (curr - prev) / prev


def log_normalize(value: float, max_value: float = 100000000) -> float:
    """对数归一化，用于处理大数值"""
    if value <= 0:
        return 0.0
    return math.log1p(value) / math.log1p(max_value)


class AdaptiveTrendScorer:
    """
    自适应 Trend Score 计算器
    
    根据平台自动调整计算方式和权重
    """
    
    def __init__(self, keyword_profiles: Optional[Dict] = None):
        self.keyword_profiles = keyword_profiles or DEFAULT_KEYWORD_PROFILES
        self._history: Dict[str, Dict] = {}
    
    def detect_platform(self, platform_str: str) -> Platform:
        """检测平台类型"""
        platform_lower = platform_str.lower().strip()
        
        platform_map = {
            "tiktok": Platform.TIKTOK,
            "instagram": Platform.INSTAGRAM,
            "reddit": Platform.REDDIT,
            "youtube": Platform.YOUTUBE,
            "twitter": Platform.TWITTER,
            "x": Platform.TWITTER,
            "linkedin": Platform.LINKEDIN,
        }
        
        return platform_map.get(platform_lower, Platform.UNKNOWN)
    
    def get_platform_config(self, platform: Platform) -> PlatformMetricMapping:
        """获取平台配置"""
        return PLATFORM_CONFIGS.get(platform, PlatformMetricMapping())
    
    def extract_metrics(
        self, 
        raw_stats: Dict[str, Any], 
        platform: Platform
    ) -> Dict[str, float]:
        """
        从原始数据中提取标准化指标
        
        根据平台配置映射字段，并处理缺失值
        """
        config = self.get_platform_config(platform)
        metrics = {}
        
        # 提取 views
        if config.views_field and config.views_field in raw_stats:
            metrics["views"] = float(raw_stats.get(config.views_field, 0) or 0)
        elif platform == Platform.REDDIT and config.score_field:
            # Reddit 用 score * 估算系数 作为 views 代理
            score = float(raw_stats.get(config.score_field, 0) or 0)
            metrics["views"] = score * 10  # 估算系数
        else:
            metrics["views"] = 0.0
        
        # 提取 likes
        if config.likes_field and config.likes_field in raw_stats:
            metrics["likes"] = float(raw_stats.get(config.likes_field, 0) or 0)
        elif config.upvotes_field and config.upvotes_field in raw_stats:
            # Reddit 用 upvotes 作为 likes
            metrics["likes"] = float(raw_stats.get(config.upvotes_field, 0) or 0)
        elif config.score_field and config.score_field in raw_stats:
            # 用 score 作为 likes 代理
            metrics["likes"] = float(raw_stats.get(config.score_field, 0) or 0)
        else:
            metrics["likes"] = 0.0
        
        # 提取 comments
        if config.comments_field and config.comments_field in raw_stats:
            metrics["comments"] = float(raw_stats.get(config.comments_field, 0) or 0)
        else:
            metrics["comments"] = 0.0
        
        # 提取 shares
        if config.shares_field and config.shares_field in raw_stats:
            metrics["shares"] = float(raw_stats.get(config.shares_field, 0) or 0)
        else:
            metrics["shares"] = 0.0
        
        # 提取 saves
        if config.saves_field and config.saves_field in raw_stats:
            metrics["saves"] = float(raw_stats.get(config.saves_field, 0) or 0)
        else:
            metrics["saves"] = 0.0
        
        # 存储权重配置
        metrics["_weights"] = {
            "views": config.views_weight,
            "likes": config.likes_weight,
            "comments": config.comments_weight,
            "shares": config.shares_weight,
            "saves": config.saves_weight,
        }
        
        metrics["_hotness_weights"] = {
            "views": config.hotness_views_weight,
            "engagement": config.hotness_engagement_weight,
            "posts": config.hotness_posts_weight,
        }
        
        return metrics
    
    def get_keyword_profile(self, keyword: str) -> Dict[str, Any]:
        """获取关键词配置"""
        keyword_lower = keyword.lower().strip().lstrip("#")
        
        if keyword_lower in self.keyword_profiles:
            return self.keyword_profiles[keyword_lower]
        
        for k, v in self.keyword_profiles.items():
            if k in keyword_lower or keyword_lower in k:
                return v
        
        return {
            "ai_feasibility": 3,
            "monetization": 0.5,
            "ip_risk": 0.2,
            "category": "general"
        }
    
    def compute_hotness(
        self, 
        metrics: Dict[str, Any], 
        platform: Platform
    ) -> float:
        """
        计算热度 (H) - 自适应版本
        
        根据平台可用数据自动调整计算方式
        """
        views = metrics.get("views", 0)
        likes = metrics.get("likes", 0)
        comments = metrics.get("comments", 0)
        shares = metrics.get("shares", 0)
        posts = metrics.get("posts", 0)
        
        hotness_weights = metrics.get("_hotness_weights", {
            "views": 0.50,
            "engagement": 0.30,
            "posts": 0.20,
        })
        
        # LinkedIn 特殊处理：只有帖子数量
        if platform == Platform.LINKEDIN:
            posts_norm = log_normalize(posts, max_value=1000) if posts > 0 else 0.0
            return clamp(posts_norm * 0.3)  # LinkedIn 热度上限较低
        
        # 播放量归一化
        if views > 0:
            views_norm = log_normalize(views, max_value=100000000)
        else:
            views_norm = 0.0
        
        # 互动强度计算
        engagement = 0.0
        if platform == Platform.REDDIT:
            # Reddit: 主要看 score 和 comments
            score = likes  # 已经映射为 score
            if score > 0:
                engagement = log_normalize(score, max_value=100000)
                # comments 加成
                if comments > 0:
                    engagement = 0.6 * engagement + 0.4 * log_normalize(comments, max_value=10000)
        elif views > 0:
            # 标准互动率计算
            engagement_rate = (likes + comments * 2 + shares * 3) / views
            engagement_rate = min(engagement_rate, 0.5)
            engagement = engagement_rate / 0.5
        elif likes > 0:
            # 没有 views，用 likes 作为基准
            engagement = log_normalize(likes, max_value=1000000)
        
        # 帖子数量归一化
        posts_norm = log_normalize(posts, max_value=10000) if posts > 0 else 0.0
        
        # 加权组合
        H = (
            hotness_weights.get("views", 0.5) * views_norm +
            hotness_weights.get("engagement", 0.3) * engagement +
            hotness_weights.get("posts", 0.2) * posts_norm
        )
        
        return clamp(H)
    
    def compute_velocity(
        self, 
        metrics: Dict[str, Any], 
        prev_metrics: Optional[Dict[str, Any]],
        platform: Platform
    ) -> float:
        """
        计算增速 (V) - 自适应版本
        
        根据平台可用数据自动调整权重
        """
        if prev_metrics is None:
            return 0.5  # 无历史数据时返回中性值
        
        weights = metrics.get("_weights", {
            "views": 1.0,
            "likes": 1.0,
            "comments": 1.0,
            "shares": 1.0,
            "saves": 1.0,
        })
        
        # 计算有效权重总和
        total_weight = 0.0
        velocity_raw = 0.0
        has_valid_growth = False
        
        for metric, base_weight in VELOCITY_WEIGHTS.items():
            curr = metrics.get(metric, 0)
            prev = prev_metrics.get(metric, 0)
            platform_weight = weights.get(metric, 1.0)
            
            # 跳过无效指标
            if platform_weight == 0:
                continue
            
            # 如果有历史数据，计算增长率
            if prev > 0:
                growth = safe_growth(curr, prev)
                growth_clamped = clamp(growth, -1.0, 3.0)
                has_valid_growth = True
            elif curr > 0:
                # 从0增长到有值，视为高增长
                growth_clamped = 2.0
                has_valid_growth = True
            else:
                # 都是0，跳过
                continue
            
            effective_weight = base_weight * platform_weight
            velocity_raw += effective_weight * growth_clamped
            total_weight += effective_weight
        
        # 如果没有有效的增长数据，返回中性值
        if not has_valid_growth or total_weight == 0:
            return 0.5
        
        # 归一化
        velocity_raw = velocity_raw / total_weight
        
        # 映射到 [0, 1]: -1 → 0, 0 → 0.25, 1 → 0.5, 3 → 1
        V = (velocity_raw + 1) / 4
        return clamp(V)
    
    def compute_density(self, metrics: Dict[str, Any]) -> float:
        """
        计算密度 (D) - 包含新鲜度因子
        
        D 维度现在由三部分组成：
        1. 帖子数量 (posts_norm) - 30%
        2. 创作者多样性 (creators_norm) - 20%
        3. 新鲜度 (freshness_rate) - 50% (最重要！)
        
        新鲜度逻辑：
        - 每次爬取 tag 最新 20 条内容
        - 如果大部分是新内容 → tag 很活跃 → D 高
        - 如果大部分是重复内容 → tag 不活跃 → D 低
        """
        posts = float(metrics.get("posts", 0) or 0)
        unique_creators = float(metrics.get("unique_creators", 0) or 0)
        freshness_rate = float(metrics.get("freshness_rate", 0.5))  # 默认 0.5
        
        # 帖子数量归一化
        posts_norm = log_normalize(posts, max_value=10000)
        
        # 创作者多样性归一化
        if unique_creators > 0:
            creators_norm = log_normalize(unique_creators, max_value=1000)
        else:
            creators_norm = posts_norm * 0.5  # 没有创作者数据时，用帖子数估算
        
        # 新鲜度直接使用 (已经是 0-1 范围)
        # 但需要调整：首次爬取 freshness=1.0 不应该给满分
        # 因为首次爬取没有历史对比，应该给中性值
        if metrics.get("is_first_crawl", False):
            freshness_norm = 0.5  # 首次爬取给中性值
        else:
            freshness_norm = freshness_rate
        
        # 加权组合：新鲜度占 50%，帖子数占 30%，创作者占 20%
        D = 0.50 * freshness_norm + 0.30 * posts_norm + 0.20 * creators_norm
        
        return clamp(D)
    
    def compute_feasibility(self, keyword: str) -> float:
        """计算可行性 (F)"""
        profile = self.get_keyword_profile(keyword)
        ai_feasibility = profile.get("ai_feasibility", 3)
        F = (ai_feasibility - 1) / 4.0
        return clamp(F)
    
    def compute_monetization(
        self, 
        keyword: str, 
        metrics: Dict[str, Any],
        platform: Platform
    ) -> float:
        """
        计算商业化潜力 (M) - 自适应版本
        
        根据平台特性调整商业化评估
        """
        profile = self.get_keyword_profile(keyword)
        base_monetization = profile.get("monetization", 0.5)
        
        views = metrics.get("views", 0)
        likes = metrics.get("likes", 0)
        
        # 流量规模加成
        bonus = 0.0
        
        if platform == Platform.LINKEDIN:
            # LinkedIn 商业化潜力较低（B2B，数据不完整）
            base_monetization *= 0.5
        elif platform == Platform.REDDIT:
            # Reddit 商业化潜力较低
            base_monetization *= 0.7
            if likes >= 10000:  # score >= 10000
                bonus = 0.1
            elif likes >= 5000:
                bonus = 0.05
        elif platform == Platform.INSTAGRAM:
            # Instagram 商业化潜力较高
            base_monetization *= 1.1
            if views >= 10000000:
                bonus = 0.1
            elif views >= 1000000:
                bonus = 0.05
        elif platform == Platform.TWITTER:
            # Twitter 商业化潜力中等
            base_monetization *= 0.9
            if views >= 10000000:
                bonus = 0.1
            elif views >= 1000000:
                bonus = 0.05
        else:
            # TikTok, YouTube 标准计算
            if views >= 50000000:
                bonus = 0.1
            elif views >= 10000000:
                bonus = 0.05
        
        M = base_monetization + bonus
        return clamp(M)
    
    def compute_risk(
        self, 
        keyword: str, 
        metrics: Dict[str, Any],
        platform: Platform
    ) -> float:
        """计算风险 (R)"""
        profile = self.get_keyword_profile(keyword)
        ip_risk = profile.get("ip_risk", 0.2)
        
        posts = metrics.get("posts", 0)
        views = metrics.get("views", 0)
        likes = metrics.get("likes", 0)
        
        # 竞争风险
        competition_risk = 0.0
        
        if platform == Platform.LINKEDIN:
            # LinkedIn 竞争风险较低（数据不完整）
            competition_risk = 0.1
        elif platform == Platform.REDDIT:
            # Reddit 用 score 评估竞争
            if likes > 10000:
                competition_risk = 0.6
            elif likes > 5000:
                competition_risk = 0.4
            elif likes > 1000:
                competition_risk = 0.2
        elif posts > 5000 and views > 50000000:
            competition_risk = 0.8
        elif posts > 1000 and views > 10000000:
            competition_risk = 0.5
        elif posts > 100:
            competition_risk = 0.3
        
        R = 0.6 * ip_risk + 0.4 * competition_risk
        return clamp(R)
    
    def determine_lifecycle(self, V: float, H: float, D: float) -> str:
        """判断生命周期"""
        if V >= 0.8:
            return "rising"
        elif V <= 0.2:
            return "declining"
        elif H >= 0.7 and D >= 0.6:
            return "evergreen"
        elif V >= 0.6 and H >= 0.6:
            return "sustained"
        elif V >= 0.7:
            return "flash"
        else:
            return "sustained"
    
    def determine_priority(
        self, 
        trend_score: float, 
        M: float, 
        F: float
    ) -> str:
        """判断优先级"""
        if trend_score >= 85 and M >= 0.85 and F >= 0.8:
            return "P0"
        elif trend_score >= 75 and M >= 0.70 and F >= 0.6:
            return "P1"
        elif trend_score >= 60 and M >= 0.50 and F >= 0.5:
            return "P2"
        else:
            return "P3"
    
    def compute_trend_score(
        self,
        keyword: str,
        platform_str: str,
        raw_stats: Dict[str, Any],
        prev_raw_stats: Optional[Dict[str, Any]] = None,
        posts: int = 0,
        freshness_rate: float = 0.5,
        new_posts: int = 0,
        activity_level: str = "unknown"
    ) -> Dict[str, Any]:
        """
        计算完整的 Trend Score - 自适应版本
        
        Args:
            keyword: 关键词/标签
            platform_str: 平台名称字符串
            raw_stats: 原始统计数据 (直接从爬虫获取)
            prev_raw_stats: 上一周期原始统计数据
            posts: 帖子数量
            freshness_rate: 新鲜度 (0-1)，新内容占比
            new_posts: 新增帖子数
            activity_level: 活跃度等级 (very_active/active/moderate/stale)
        
        Returns:
            完整的评分结果
        """
        # 检测平台
        platform = self.detect_platform(platform_str)
        
        # 提取标准化指标
        metrics = self.extract_metrics(raw_stats, platform)
        metrics["posts"] = posts
        metrics["freshness_rate"] = freshness_rate
        metrics["new_posts"] = new_posts
        
        # 判断是否首次爬取（没有历史数据）
        is_first_crawl = prev_raw_stats is None or all(
            prev_raw_stats.get(k, 0) == 0 for k in ["views", "likes", "comments"]
        )
        metrics["is_first_crawl"] = is_first_crawl
        
        prev_metrics = None
        if prev_raw_stats:
            prev_metrics = self.extract_metrics(prev_raw_stats, platform)
        
        # 计算各维度
        H = self.compute_hotness(metrics, platform)
        V = self.compute_velocity(metrics, prev_metrics, platform)
        D = self.compute_density(metrics)  # 现在包含新鲜度
        F = self.compute_feasibility(keyword)
        M = self.compute_monetization(keyword, metrics, platform)
        R = self.compute_risk(keyword, metrics, platform)
        
        # 计算总分
        score_0_1 = (
            WEIGHTS["H"] * H +
            WEIGHTS["V"] * V +
            WEIGHTS["D"] * D +
            WEIGHTS["F"] * F +
            WEIGHTS["M"] * M -
            WEIGHTS["R"] * R
        )
        score_0_1 = clamp(score_0_1)
        trend_score = round(100 * score_0_1)
        
        # 判断生命周期和优先级
        lifecycle = self.determine_lifecycle(V, H, D)
        priority = self.determine_priority(trend_score, M, F)
        
        # Agent 就绪判断
        agent_ready = trend_score >= 60 and F >= 0.5
        
        # 获取类别
        profile = self.get_keyword_profile(keyword)
        category = profile.get("category", "general")
        
        return {
            "keyword": keyword,
            "platform": platform_str,
            "platform_type": platform.value,
            "trend_score": trend_score,
            "H": round(H, 3),
            "V": round(V, 3),
            "D": round(D, 3),
            "F": round(F, 3),
            "M": round(M, 3),
            "R": round(R, 3),
            "lifecycle": lifecycle,
            "priority": priority,
            "agent_ready": agent_ready,
            "category": category,
            "raw_metrics": {
                "views": metrics.get("views", 0),
                "likes": metrics.get("likes", 0),
                "comments": metrics.get("comments", 0),
                "shares": metrics.get("shares", 0),
                "saves": metrics.get("saves", 0),
                "posts": posts,
            },
            # 新增：活跃度指标
            "activity": {
                "freshness_rate": freshness_rate,
                "new_posts": new_posts,
                "activity_level": activity_level,
                "is_first_crawl": is_first_crawl,
            },
            "computed_at": datetime.utcnow().isoformat()
        }

    def compute_from_crawl_item(
        self,
        item: Dict[str, Any],
        keyword: str,
        prev_item: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        从爬虫数据项直接计算 Trend Score
        
        Args:
            item: 爬虫返回的单条数据，格式如:
                {
                    "platform": "tiktok",
                    "id": "xxx",
                    "stats": {"views": 1000, "likes": 100, ...},
                    ...
                }
            keyword: 关键词
            prev_item: 上一周期的数据项
        
        Returns:
            评分结果
        """
        platform_str = item.get("platform", "unknown")
        raw_stats = item.get("stats", {})
        
        prev_raw_stats = None
        if prev_item:
            prev_raw_stats = prev_item.get("stats", {})
        
        return self.compute_trend_score(
            keyword=keyword,
            platform_str=platform_str,
            raw_stats=raw_stats,
            prev_raw_stats=prev_raw_stats,
            posts=1  # 单条数据
        )
    
    def compute_aggregated_score(
        self,
        items: List[Dict[str, Any]],
        keyword: str,
        platform_str: str,
        prev_items: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        聚合多条数据计算 Trend Score
        
        Args:
            items: 同一关键词、同一平台的多条数据
            keyword: 关键词
            platform_str: 平台名称
            prev_items: 上一周期的数据列表
        
        Returns:
            聚合后的评分结果
        """
        if not items:
            return {
                "keyword": keyword,
                "platform": platform_str,
                "trend_score": 0,
                "error": "No items to aggregate"
            }
        
        platform = self.detect_platform(platform_str)
        
        # 聚合统计数据
        aggregated_stats = {
            "views": 0,
            "likes": 0,
            "comments": 0,
            "shares": 0,
            "saves": 0,
        }
        
        for item in items:
            stats = item.get("stats", {})
            metrics = self.extract_metrics(stats, platform)
            
            aggregated_stats["views"] += metrics.get("views", 0)
            aggregated_stats["likes"] += metrics.get("likes", 0)
            aggregated_stats["comments"] += metrics.get("comments", 0)
            aggregated_stats["shares"] += metrics.get("shares", 0)
            aggregated_stats["saves"] += metrics.get("saves", 0)
        
        # 聚合上一周期数据
        prev_aggregated_stats = None
        if prev_items:
            prev_aggregated_stats = {
                "views": 0,
                "likes": 0,
                "comments": 0,
                "shares": 0,
                "saves": 0,
            }
            for item in prev_items:
                stats = item.get("stats", {})
                metrics = self.extract_metrics(stats, platform)
                
                prev_aggregated_stats["views"] += metrics.get("views", 0)
                prev_aggregated_stats["likes"] += metrics.get("likes", 0)
                prev_aggregated_stats["comments"] += metrics.get("comments", 0)
                prev_aggregated_stats["shares"] += metrics.get("shares", 0)
                prev_aggregated_stats["saves"] += metrics.get("saves", 0)
        
        return self.compute_trend_score(
            keyword=keyword,
            platform_str=platform_str,
            raw_stats=aggregated_stats,
            prev_raw_stats=prev_aggregated_stats,
            posts=len(items)
        )
    
    def process_crawl_data(
        self,
        crawl_data: Dict[str, Any],
        prev_crawl_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        处理完整的爬虫数据文件
        
        Args:
            crawl_data: 爬虫输出的完整 JSON 数据，格式如:
                {
                    "timestamp": "...",
                    "tags": ["music", "dance"],
                    "platforms": {
                        "tiktok": {
                            "platform": "TikTok",
                            "success": true,
                            "data": {
                                "music": [...],
                                "dance": [...]
                            }
                        },
                        ...
                    }
                }
            prev_crawl_data: 上一周期的爬虫数据
        
        Returns:
            按平台和关键词组织的评分结果:
            {
                "tiktok": [
                    {"keyword": "music", "trend_score": 75, ...},
                    {"keyword": "dance", "trend_score": 68, ...}
                ],
                "instagram": [...],
                ...
            }
        """
        results = {}
        
        platforms_data = crawl_data.get("platforms", {})
        prev_platforms_data = prev_crawl_data.get("platforms", {}) if prev_crawl_data else {}
        
        for platform_key, platform_info in platforms_data.items():
            if not platform_info.get("success", False):
                continue
            
            platform_results = []
            platform_data = platform_info.get("data", {})
            prev_platform_data = prev_platforms_data.get(platform_key, {}).get("data", {})
            
            # 遍历每个关键词
            for keyword, items in platform_data.items():
                if not isinstance(items, list):
                    continue
                
                prev_items = prev_platform_data.get(keyword, []) if prev_platform_data else None
                
                # 计算聚合分数
                score_result = self.compute_aggregated_score(
                    items=items,
                    keyword=keyword,
                    platform_str=platform_key,
                    prev_items=prev_items
                )
                
                platform_results.append(score_result)
            
            # 按分数排序
            platform_results.sort(key=lambda x: x.get("trend_score", 0), reverse=True)
            results[platform_key] = platform_results
        
        return results
    
    def get_top_trends(
        self,
        crawl_data: Dict[str, Any],
        top_n: int = 10,
        min_score: int = 50
    ) -> List[Dict[str, Any]]:
        """
        获取所有平台的 Top N 趋势
        
        Args:
            crawl_data: 爬虫数据
            top_n: 返回数量
            min_score: 最低分数阈值
        
        Returns:
            跨平台的 Top N 趋势列表
        """
        all_results = self.process_crawl_data(crawl_data)
        
        # 合并所有平台结果
        all_trends = []
        for platform, results in all_results.items():
            all_trends.extend(results)
        
        # 过滤和排序
        filtered = [t for t in all_trends if t.get("trend_score", 0) >= min_score]
        filtered.sort(key=lambda x: x.get("trend_score", 0), reverse=True)
        
        return filtered[:top_n]


# 全局单例
adaptive_trend_scorer = AdaptiveTrendScorer()


# 便捷函数
def compute_adaptive_trend_score(
    keyword: str,
    platform_str: str,
    raw_stats: Dict[str, Any],
    prev_raw_stats: Optional[Dict[str, Any]] = None,
    posts: int = 0,
    freshness_rate: float = 0.5,
    new_posts: int = 0,
    activity_level: str = "unknown"
) -> Dict[str, Any]:
    """便捷函数，使用全局单例计算"""
    return adaptive_trend_scorer.compute_trend_score(
        keyword=keyword,
        platform_str=platform_str,
        raw_stats=raw_stats,
        prev_raw_stats=prev_raw_stats,
        posts=posts,
        freshness_rate=freshness_rate,
        new_posts=new_posts,
        activity_level=activity_level
    )


def process_crawl_file(
    crawl_data: Dict[str, Any],
    prev_crawl_data: Optional[Dict[str, Any]] = None
) -> Dict[str, List[Dict[str, Any]]]:
    """便捷函数，处理爬虫数据文件"""
    return adaptive_trend_scorer.process_crawl_data(crawl_data, prev_crawl_data)
