"""
Trend Score 计算引擎

基于 6 维度评分算法计算趋势分数:
- H (Hotness): 热度 - 基于播放量和互动
- V (Velocity): 增速 - 基于增长率
- D (Density): 密度 - 基于帖子数量
- F (Feasibility): 可行性 - AI 技术可行性
- M (Monetization): 商业化 - 商业潜力
- R (Risk): 风险 - IP 和竞争风险

公式: trend_score = 0.20*H + 0.30*V + 0.15*D + 0.15*F + 0.20*M - 0.25*R
结果范围: 0-100
"""

import math
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)

# 权重配置
WEIGHTS = {
    "H": 0.20,  # 热度
    "V": 0.30,  # 增速 (最高权重)
    "M": 0.20,  # 商业化
    "D": 0.15,  # 密度
    "F": 0.15,  # 可行性
    "R": 0.25,  # 风险 (惩罚项)
}

# 增长率权重
VELOCITY_WEIGHTS = {
    "views": 0.45,
    "likes": 0.25,
    "comments": 0.15,
    "shares": 0.10,
    "saves": 0.05,
}

# 默认关键词配置 (可从数据库或配置文件加载)
DEFAULT_KEYWORD_PROFILES = {
    # AI 工具类 - 高可行性，高商业潜力
    "ai headshot": {"ai_feasibility": 5, "monetization": 0.9, "ip_risk": 0.1, "category": "portrait"},
    "ai manga filter": {"ai_feasibility": 4, "monetization": 0.8, "ip_risk": 0.3, "category": "filter"},
    "background remover": {"ai_feasibility": 5, "monetization": 0.85, "ip_risk": 0.1, "category": "tool"},
    "image upscaler": {"ai_feasibility": 5, "monetization": 0.8, "ip_risk": 0.1, "category": "tool"},
    
    # 滤镜类 - 中等可行性，有 IP 风险
    "anime filter": {"ai_feasibility": 4, "monetization": 0.7, "ip_risk": 0.2, "category": "filter"},
    "ghibli filter": {"ai_feasibility": 4, "monetization": 0.7, "ip_risk": 0.5, "category": "filter"},
    "arcane filter": {"ai_feasibility": 4, "monetization": 0.7, "ip_risk": 0.6, "category": "filter"},
    
    # 通用热门 - 默认配置
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


class TrendScorer:
    """
    Trend Score 计算器
    
    使用方法:
    ```python
    scorer = TrendScorer()
    result = scorer.compute_trend_score(
        keyword="music",
        platform="tiktok",
        metrics={
            "views": 1500000,
            "likes": 85000,
            "comments": 3200,
            "shares": 1500,
            "saves": 2800,
            "posts": 142
        }
    )
    print(result["trend_score"])  # 0-100
    ```
    """
    
    def __init__(self, keyword_profiles: Optional[Dict] = None):
        """
        初始化计算器
        
        Args:
            keyword_profiles: 关键词配置字典，格式:
                {
                    "keyword": {
                        "ai_feasibility": 1-5,
                        "monetization": 0-1,
                        "ip_risk": 0-1,
                        "category": "portrait/filter/tool/..."
                    }
                }
        """
        self.keyword_profiles = keyword_profiles or DEFAULT_KEYWORD_PROFILES
        self._history: Dict[str, Dict] = {}  # 用于存储历史数据计算增长率
    
    def get_keyword_profile(self, keyword: str) -> Dict[str, Any]:
        """获取关键词配置，如果不存在则返回默认值"""
        keyword_lower = keyword.lower().strip().lstrip("#")
        
        # 精确匹配
        if keyword_lower in self.keyword_profiles:
            return self.keyword_profiles[keyword_lower]
        
        # 模糊匹配
        for k, v in self.keyword_profiles.items():
            if k in keyword_lower or keyword_lower in k:
                return v
        
        # 默认配置
        return {
            "ai_feasibility": 3,
            "monetization": 0.5,
            "ip_risk": 0.2,
            "category": "general"
        }
    
    def compute_hotness(self, metrics: Dict[str, Any]) -> float:
        """
        计算热度 (H)
        
        基于:
        - 播放量 (50%)
        - 互动强度 (30%)
        - 帖子数量 (20%)
        """
        views = float(metrics.get("views", 0) or 0)
        likes = float(metrics.get("likes", 0) or 0)
        comments = float(metrics.get("comments", 0) or 0)
        shares = float(metrics.get("shares", 0) or 0)
        posts = float(metrics.get("posts", 0) or 0)
        
        # 播放量归一化 (对数)
        views_norm = log_normalize(views, max_value=100000000)  # 1亿
        
        # 互动强度 = (likes + comments + shares) / views
        engagement = 0.0
        if views > 0:
            engagement = (likes + comments * 2 + shares * 3) / views
            engagement = min(engagement, 0.5)  # 上限 50%
            engagement = engagement / 0.5  # 归一化到 0-1
        
        # 帖子数量归一化
        posts_norm = log_normalize(posts, max_value=10000)
        
        # 加权组合
        H = 0.50 * views_norm + 0.30 * engagement + 0.20 * posts_norm
        return clamp(H)
    
    def compute_velocity(
        self, 
        metrics: Dict[str, Any], 
        prev_metrics: Optional[Dict[str, Any]] = None
    ) -> float:
        """
        计算增速 (V)
        
        基于各指标的增长率加权组合
        """
        if prev_metrics is None:
            # 没有历史数据，使用默认值
            return 0.5
        
        velocity_raw = 0.0
        
        for metric, weight in VELOCITY_WEIGHTS.items():
            curr = float(metrics.get(metric, 0) or 0)
            prev = float(prev_metrics.get(metric, 0) or 0)
            
            growth = safe_growth(curr, prev)
            # 截断到 [-1, 3] (最多 -100% 到 +300%)
            growth_clamped = clamp(growth, -1.0, 3.0)
            
            velocity_raw += weight * growth_clamped
        
        # 将 velocity_raw 从 [-1, 3] 映射到 [0, 1]
        # -1 → 0, 0 → 0.25, 1 → 0.5, 3 → 1
        V = (velocity_raw + 1) / 4
        return clamp(V)
    
    def compute_density(self, metrics: Dict[str, Any]) -> float:
        """
        计算密度 (D)
        
        基于:
        - 帖子数量
        - 创作者多样性 (如果有)
        """
        posts = float(metrics.get("posts", 0) or 0)
        unique_creators = float(metrics.get("unique_creators", 0) or 0)
        
        # 帖子数量归一化
        posts_norm = log_normalize(posts, max_value=10000)
        
        # 创作者多样性归一化
        if unique_creators > 0:
            creators_norm = log_normalize(unique_creators, max_value=1000)
            D = 0.6 * posts_norm + 0.4 * creators_norm
        else:
            D = posts_norm
        
        return clamp(D)
    
    def compute_feasibility(self, keyword: str) -> float:
        """
        计算可行性 (F)
        
        基于关键词配置的 ai_feasibility (1-5)
        """
        profile = self.get_keyword_profile(keyword)
        ai_feasibility = profile.get("ai_feasibility", 3)
        
        # 1-5 映射到 0-1
        F = (ai_feasibility - 1) / 4.0
        return clamp(F)
    
    def compute_monetization(self, keyword: str, metrics: Dict[str, Any]) -> float:
        """
        计算商业化潜力 (M)
        
        基于:
        - 关键词配置的 monetization
        - 流量规模加成
        """
        profile = self.get_keyword_profile(keyword)
        base_monetization = profile.get("monetization", 0.5)
        
        # 流量规模加成
        views = float(metrics.get("views", 0) or 0)
        bonus = 0.0
        if views >= 50000000:  # 5000万+
            bonus = 0.1
        elif views >= 10000000:  # 1000万+
            bonus = 0.05
        
        M = base_monetization + bonus
        return clamp(M)
    
    def compute_risk(self, keyword: str, metrics: Dict[str, Any]) -> float:
        """
        计算风险 (R)
        
        基于:
        - IP 风险 (60%)
        - 竞争风险 (40%)
        """
        profile = self.get_keyword_profile(keyword)
        ip_risk = profile.get("ip_risk", 0.2)
        
        # 竞争风险 - 基于热度和创作者数量
        posts = float(metrics.get("posts", 0) or 0)
        views = float(metrics.get("views", 0) or 0)
        
        competition_risk = 0.0
        if posts > 5000 and views > 50000000:
            competition_risk = 0.8  # 高竞争
        elif posts > 1000 and views > 10000000:
            competition_risk = 0.5  # 中等竞争
        elif posts > 100:
            competition_risk = 0.3  # 低竞争
        
        R = 0.6 * ip_risk + 0.4 * competition_risk
        return clamp(R)
    
    def determine_lifecycle(self, V: float, H: float, D: float) -> str:
        """
        判断生命周期
        
        - flash: 快闪型 (突然爆发)
        - rising: 高速增长
        - sustained: 持续热度
        - evergreen: 长青型
        - declining: 衰退中
        """
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
        """
        判断优先级
        
        - P0: 极高优先，立即启动
        - P1: 高优先，2周内
        - P2: 中优先，排期
        """
        if trend_score >= 85 and M >= 0.85 and F >= 0.8:
            return "P0"
        elif trend_score >= 75 and M >= 0.70 and F >= 0.6:
            return "P1"
        elif trend_score >= 60 and M >= 0.50 and F >= 0.5:
            return "P2"
        else:
            return "P3"  # 不推荐
    
    def compute_trend_score(
        self,
        keyword: str,
        platform: str,
        metrics: Dict[str, Any],
        prev_metrics: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        计算完整的 Trend Score
        
        Args:
            keyword: 关键词/标签
            platform: 平台名称
            metrics: 当前指标
            prev_metrics: 上一周期指标 (用于计算增长率)
        
        Returns:
            {
                "keyword": str,
                "platform": str,
                "trend_score": int (0-100),
                "H": float (0-1),
                "V": float (0-1),
                "D": float (0-1),
                "F": float (0-1),
                "M": float (0-1),
                "R": float (0-1),
                "lifecycle": str,
                "priority": str,
                "agent_ready": bool,
                "category": str,
                "computed_at": str
            }
        """
        # 计算各维度
        H = self.compute_hotness(metrics)
        V = self.compute_velocity(metrics, prev_metrics)
        D = self.compute_density(metrics)
        F = self.compute_feasibility(keyword)
        M = self.compute_monetization(keyword, metrics)
        R = self.compute_risk(keyword, metrics)
        
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
            "platform": platform,
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
            "computed_at": datetime.utcnow().isoformat()
        }
    
    def compute_batch(
        self,
        items: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        批量计算 Trend Score
        
        Args:
            items: 列表，每项包含:
                {
                    "keyword": str,
                    "platform": str,
                    "metrics": {...},
                    "prev_metrics": {...} (可选)
                }
        
        Returns:
            计算结果列表
        """
        results = []
        for item in items:
            try:
                result = self.compute_trend_score(
                    keyword=item.get("keyword", ""),
                    platform=item.get("platform", ""),
                    metrics=item.get("metrics", {}),
                    prev_metrics=item.get("prev_metrics")
                )
                results.append(result)
            except Exception as e:
                logger.error(f"计算 Trend Score 失败: {item.get('keyword')}, 错误: {e}")
                continue
        
        return results


# 全局单例
trend_scorer = TrendScorer()


# 便捷函数
def compute_trend_score(
    keyword: str,
    platform: str,
    metrics: Dict[str, Any],
    prev_metrics: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """便捷函数，使用全局单例计算"""
    return trend_scorer.compute_trend_score(keyword, platform, metrics, prev_metrics)
