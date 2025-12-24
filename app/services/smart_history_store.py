"""
Smart History Store - 智能历史数据存储服务

优化特性：
1. 帖子级别去重 - 同一个 post_id 不重复入库，而是更新
2. 历史数据对比 - 自动计算增长率 (V 维度)
3. Tag 级别聚合 - 同一 platform + tag 的数据聚合计算
4. 热度衰减 - 长时间未更新的数据降低权重

并发优化 (2025-12-24):
- 使用 WAL 模式提升并发性能
- 使用连接池避免频繁创建/销毁连接
- 读操作无锁，写操作使用细粒度锁
- 支持批量写入减少事务次数
"""

import logging
import sqlite3
import json
import os
import threading
import queue
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# 数据保留时间（小时）
RETENTION_HOURS = 2

# 数据库文件路径
DB_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
DB_PATH = os.path.join(DB_DIR, "smart_history.db")

# 连接池大小
POOL_SIZE = 3


class ConnectionPool:
    """
    简单的 SQLite 连接池

    SQLite 在 WAL 模式下支持多个读连接和一个写连接并发
    """

    def __init__(self, db_path: str, pool_size: int = POOL_SIZE):
        self._db_path = db_path
        self._pool = queue.Queue(maxsize=pool_size)
        self._pool_size = pool_size
        self._lock = threading.Lock()
        self._initialized = False

    def _create_connection(self) -> sqlite3.Connection:
        """创建一个新的数据库连接"""
        conn = sqlite3.connect(
            self._db_path,
            timeout=30.0,
            check_same_thread=False,  # 允许跨线程使用
            isolation_level=None  # 自动提交模式，手动控制事务
        )
        conn.row_factory = sqlite3.Row
        # 启用 WAL 模式 - 大幅提升并发性能
        conn.execute("PRAGMA journal_mode=WAL")
        # 设置同步模式为 NORMAL（平衡性能和安全）
        conn.execute("PRAGMA synchronous=NORMAL")
        # 增加缓存大小（默认 2000 页，约 8MB）
        conn.execute("PRAGMA cache_size=4000")
        # 启用外键约束
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def initialize(self):
        """初始化连接池"""
        if self._initialized:
            return
        with self._lock:
            if self._initialized:
                return
            for _ in range(self._pool_size):
                conn = self._create_connection()
                self._pool.put(conn)
            self._initialized = True
            logger.info(f"连接池已初始化: {self._pool_size} 个连接, WAL 模式已启用")

    @contextmanager
    def get_connection(self):
        """获取一个连接（上下文管理器）"""
        if not self._initialized:
            self.initialize()

        conn = None
        try:
            # 尝试从池中获取连接，最多等待 10 秒
            conn = self._pool.get(timeout=10.0)
            yield conn
        except queue.Empty:
            # 池中没有可用连接，创建临时连接
            logger.warning("连接池已满，创建临时连接")
            conn = self._create_connection()
            yield conn
            conn.close()  # 临时连接用完就关闭
            conn = None
        finally:
            if conn is not None:
                try:
                    self._pool.put_nowait(conn)
                except queue.Full:
                    conn.close()

    def close_all(self):
        """关闭所有连接"""
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except queue.Empty:
                break
        self._initialized = False
        logger.info("连接池已关闭")


class SmartHistoryStore:
    """
    智能历史数据存储管理器

    数据模型：
    1. posts 表 - 存储每个帖子的最新数据和历史快照
    2. tag_scores 表 - 存储每个 platform+tag 的聚合分数

    并发优化：
    - 使用连接池管理数据库连接
    - WAL 模式支持读写并发
    - 写操作使用锁，读操作无锁
    """

    def __init__(self, db_path: str = DB_PATH, retention_hours: int = RETENTION_HOURS):
        self._db_path = db_path
        self._retention_hours = retention_hours
        self._write_lock = threading.Lock()  # 只用于写操作
        self._last_vacuum_time = datetime.utcnow()

        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        # 使用连接池
        self._pool = ConnectionPool(db_path)
        self._pool.initialize()

        self._init_db()
        logger.info(f"SmartHistoryStore 已初始化: {db_path} (WAL 模式, 连接池)")

    @contextmanager
    def _get_connection(self):
        """获取数据库连接（从连接池）"""
        with self._pool.get_connection() as conn:
            yield conn
    
    def _init_db(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # 帖子表 - 存储每个帖子的数据，用于去重和计算增长率
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS posts (
                    id TEXT PRIMARY KEY,
                    platform TEXT NOT NULL,
                    tag TEXT NOT NULL,
                    post_id TEXT NOT NULL,
                    author TEXT DEFAULT '',
                    title TEXT DEFAULT '',
                    description TEXT DEFAULT '',
                    content_url TEXT DEFAULT '',
                    cover_url TEXT DEFAULT '',
                    
                    -- 当前指标
                    views INTEGER DEFAULT 0,
                    likes INTEGER DEFAULT 0,
                    comments INTEGER DEFAULT 0,
                    shares INTEGER DEFAULT 0,
                    saves INTEGER DEFAULT 0,
                    
                    -- 上次指标 (用于计算增长率)
                    prev_views INTEGER DEFAULT 0,
                    prev_likes INTEGER DEFAULT 0,
                    prev_comments INTEGER DEFAULT 0,
                    prev_shares INTEGER DEFAULT 0,
                    prev_saves INTEGER DEFAULT 0,
                    
                    -- 趋势分数
                    trend_score REAL DEFAULT 0,
                    dimensions TEXT DEFAULT '{}',
                    lifecycle TEXT DEFAULT 'unknown',
                    priority TEXT DEFAULT 'P3',
                    
                    -- 时间戳
                    first_seen_at TEXT NOT NULL,
                    last_updated_at TEXT NOT NULL,
                    post_created_at TEXT DEFAULT '',
                    
                    -- 更新次数 (用于判断热度)
                    update_count INTEGER DEFAULT 1,
                    
                    UNIQUE(platform, post_id)
                )
            """)
            
            # Tag 聚合分数表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tag_scores (
                    id TEXT PRIMARY KEY,
                    platform TEXT NOT NULL,
                    tag TEXT NOT NULL,
                    
                    -- 聚合指标
                    total_views INTEGER DEFAULT 0,
                    total_likes INTEGER DEFAULT 0,
                    total_comments INTEGER DEFAULT 0,
                    total_shares INTEGER DEFAULT 0,
                    total_saves INTEGER DEFAULT 0,
                    post_count INTEGER DEFAULT 0,
                    
                    -- 上次聚合指标
                    prev_total_views INTEGER DEFAULT 0,
                    prev_total_likes INTEGER DEFAULT 0,
                    prev_total_comments INTEGER DEFAULT 0,
                    prev_total_shares INTEGER DEFAULT 0,
                    prev_total_saves INTEGER DEFAULT 0,
                    
                    -- 计算的分数
                    trend_score REAL DEFAULT 0,
                    dimensions TEXT DEFAULT '{}',
                    lifecycle TEXT DEFAULT 'unknown',
                    priority TEXT DEFAULT 'P3',
                    
                    -- 活跃度指标 (新增)
                    freshness_rate REAL DEFAULT 0.5,
                    activity_level TEXT DEFAULT 'unknown',
                    new_posts_count INTEGER DEFAULT 0,
                    
                    -- 时间戳
                    first_seen_at TEXT NOT NULL,
                    last_updated_at TEXT NOT NULL,
                    
                    UNIQUE(platform, tag)
                )
            """)
            
            # 索引
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_posts_platform_tag ON posts(platform, tag)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_posts_last_updated ON posts(last_updated_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_tag_scores_platform ON tag_scores(platform)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_tag_scores_score ON tag_scores(trend_score DESC)")
            
            conn.commit()
            logger.info("Smart database initialized")

    def upsert_post(
        self,
        platform: str,
        tag: str,
        post_id: str,
        stats: Dict[str, int],
        author: str = "",
        title: str = "",
        description: str = "",
        content_url: str = "",
        cover_url: str = "",
        post_created_at: str = "",
        trend_score: float = 0,
        dimensions: Optional[Dict[str, float]] = None,
        lifecycle: str = "unknown",
        priority: str = "P3"
    ) -> Tuple[bool, Optional[Dict[str, int]]]:
        """
        插入或更新帖子数据
        
        Returns:
            (is_new, prev_stats): 是否新帖子，以及上次的统计数据（用于计算增长率）
        """
        now = datetime.utcnow().isoformat()
        unique_id = f"{platform}:{post_id}"
        
        views = stats.get("views", 0) or 0
        likes = stats.get("likes", 0) or 0
        comments = stats.get("comments", 0) or 0
        shares = stats.get("shares", 0) or 0
        saves = stats.get("saves", 0) or 0
        
        with self._write_lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                # 开始事务（WAL 模式下更高效）
                cursor.execute("BEGIN IMMEDIATE")

                try:
                    # 检查是否已存在
                    cursor.execute(
                        "SELECT views, likes, comments, shares, saves, update_count FROM posts WHERE id = ?",
                        (unique_id,)
                    )
                    existing = cursor.fetchone()

                    if existing:
                        # 更新现有记录，保存当前值为 prev_*
                        prev_stats = {
                            "views": existing["views"],
                            "likes": existing["likes"],
                            "comments": existing["comments"],
                            "shares": existing["shares"],
                            "saves": existing["saves"]
                        }

                        cursor.execute("""
                            UPDATE posts SET
                                views = ?, likes = ?, comments = ?, shares = ?, saves = ?,
                                prev_views = ?, prev_likes = ?, prev_comments = ?, prev_shares = ?, prev_saves = ?,
                                author = COALESCE(NULLIF(?, ''), author),
                                title = COALESCE(NULLIF(?, ''), title),
                                description = COALESCE(NULLIF(?, ''), description),
                                content_url = COALESCE(NULLIF(?, ''), content_url),
                                cover_url = COALESCE(NULLIF(?, ''), cover_url),
                                trend_score = ?,
                                dimensions = ?,
                                lifecycle = ?,
                                priority = ?,
                                last_updated_at = ?,
                                update_count = update_count + 1
                            WHERE id = ?
                        """, (
                            views, likes, comments, shares, saves,
                            prev_stats["views"], prev_stats["likes"], prev_stats["comments"],
                            prev_stats["shares"], prev_stats["saves"],
                            author, title, description, content_url, cover_url,
                            trend_score, json.dumps(dimensions or {}), lifecycle, priority,
                            now, unique_id
                        ))
                        cursor.execute("COMMIT")

                        logger.debug(f"Updated post: {unique_id}, update_count={existing['update_count']+1}")
                        return False, prev_stats
                    else:
                        # 插入新记录
                        cursor.execute("""
                            INSERT INTO posts
                            (id, platform, tag, post_id, author, title, description, content_url, cover_url,
                             views, likes, comments, shares, saves,
                             prev_views, prev_likes, prev_comments, prev_shares, prev_saves,
                             trend_score, dimensions, lifecycle, priority,
                             first_seen_at, last_updated_at, post_created_at, update_count)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 0, ?, ?, ?, ?, ?, ?, ?, 1)
                        """, (
                            unique_id, platform.lower(), tag.lower().lstrip("#"), post_id,
                            author, title[:200] if title else "", description[:500] if description else "",
                            content_url, cover_url,
                            views, likes, comments, shares, saves,
                            trend_score, json.dumps(dimensions or {}), lifecycle, priority,
                            now, now, post_created_at
                        ))
                        cursor.execute("COMMIT")

                        logger.debug(f"Inserted new post: {unique_id}")
                        return True, None
                except Exception as e:
                    cursor.execute("ROLLBACK")
                    logger.error(f"upsert_post 失败: {e}")
                    raise
    
    def get_tag_aggregated_stats(
        self,
        platform: str,
        tag: str,
        current_batch_size: int = 20
    ) -> Dict[str, Any]:
        """
        获取某个 platform+tag 的聚合统计数据
        
        Args:
            platform: 平台名称
            tag: 标签名称
            current_batch_size: 本次爬取的帖子数量（用于计算新鲜度）
        
        Returns:
            {
                "current": {"views": ..., "likes": ..., ...},
                "previous": {"views": ..., "likes": ..., ...},
                "post_count": int,
                "new_posts": int,
                "freshness_rate": float,  # 新内容比例 (0-1)
                "activity_level": str,    # 活跃度等级
            }
        """
        tag_clean = tag.lower().lstrip("#")
        platform_clean = platform.lower()
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # 聚合当前数据
            cursor.execute("""
                SELECT 
                    SUM(views) as total_views,
                    SUM(likes) as total_likes,
                    SUM(comments) as total_comments,
                    SUM(shares) as total_shares,
                    SUM(saves) as total_saves,
                    SUM(prev_views) as prev_total_views,
                    SUM(prev_likes) as prev_total_likes,
                    SUM(prev_comments) as prev_total_comments,
                    SUM(prev_shares) as prev_total_shares,
                    SUM(prev_saves) as prev_total_saves,
                    COUNT(*) as post_count,
                    SUM(CASE WHEN update_count = 1 THEN 1 ELSE 0 END) as new_posts,
                    AVG(update_count) as avg_update_count
                FROM posts
                WHERE platform = ? AND tag = ?
            """, (platform_clean, tag_clean))
            
            row = cursor.fetchone()
            
            if not row or row["post_count"] == 0:
                return {
                    "current": {"views": 0, "likes": 0, "comments": 0, "shares": 0, "saves": 0},
                    "previous": {"views": 0, "likes": 0, "comments": 0, "shares": 0, "saves": 0},
                    "post_count": 0,
                    "new_posts": 0,
                    "freshness_rate": 1.0,  # 首次爬取，100% 新内容
                    "activity_level": "new"
                }
            
            new_posts = row["new_posts"] or 0
            post_count = row["post_count"]
            avg_update = row["avg_update_count"] or 1
            
            # 计算新鲜度 (Freshness Rate)
            # 新鲜度 = 本次新增帖子数 / 本次爬取总数
            # 如果 current_batch_size 未知，用 new_posts / post_count 估算
            if current_batch_size > 0:
                freshness_rate = min(new_posts / current_batch_size, 1.0)
            else:
                freshness_rate = new_posts / post_count if post_count > 0 else 0.0
            
            # 判断活跃度等级
            # - very_active: 新鲜度 >= 70%，说明大部分是新内容
            # - active: 新鲜度 40-70%，有一定新内容
            # - moderate: 新鲜度 20-40%，新内容较少
            # - stale: 新鲜度 < 20%，几乎没有新内容
            if freshness_rate >= 0.7:
                activity_level = "very_active"
            elif freshness_rate >= 0.4:
                activity_level = "active"
            elif freshness_rate >= 0.2:
                activity_level = "moderate"
            else:
                activity_level = "stale"
            
            return {
                "current": {
                    "views": row["total_views"] or 0,
                    "likes": row["total_likes"] or 0,
                    "comments": row["total_comments"] or 0,
                    "shares": row["total_shares"] or 0,
                    "saves": row["total_saves"] or 0
                },
                "previous": {
                    "views": row["prev_total_views"] or 0,
                    "likes": row["prev_total_likes"] or 0,
                    "comments": row["prev_total_comments"] or 0,
                    "shares": row["prev_total_shares"] or 0,
                    "saves": row["prev_total_saves"] or 0
                },
                "post_count": post_count,
                "new_posts": new_posts,
                "freshness_rate": round(freshness_rate, 3),
                "activity_level": activity_level,
                "avg_update_count": round(avg_update, 2)
            }
    
    def save_tag_score(
        self,
        platform: str,
        tag: str,
        aggregated_stats: Dict[str, Any],
        trend_score: float,
        dimensions: Dict[str, float],
        lifecycle: str = "unknown",
        priority: str = "P3"
    ):
        """保存 tag 的聚合分数（包含活跃度信息）"""
        now = datetime.utcnow().isoformat()
        unique_id = f"{platform.lower()}:{tag.lower().lstrip('#')}"
        
        current = aggregated_stats.get("current", {})
        previous = aggregated_stats.get("previous", {})
        freshness_rate = aggregated_stats.get("freshness_rate", 0.5)
        activity_level = aggregated_stats.get("activity_level", "unknown")
        new_posts = aggregated_stats.get("new_posts", 0)
        
        with self._write_lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute("""
                    INSERT INTO tag_scores
                    (id, platform, tag,
                     total_views, total_likes, total_comments, total_shares, total_saves, post_count,
                     prev_total_views, prev_total_likes, prev_total_comments, prev_total_shares, prev_total_saves,
                     trend_score, dimensions, lifecycle, priority,
                     freshness_rate, activity_level, new_posts_count,
                     first_seen_at, last_updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(platform, tag) DO UPDATE SET
                        total_views = excluded.total_views,
                        total_likes = excluded.total_likes,
                        total_comments = excluded.total_comments,
                        total_shares = excluded.total_shares,
                        total_saves = excluded.total_saves,
                        post_count = excluded.post_count,
                        prev_total_views = excluded.prev_total_views,
                        prev_total_likes = excluded.prev_total_likes,
                        prev_total_comments = excluded.prev_total_comments,
                        prev_total_shares = excluded.prev_total_shares,
                        prev_total_saves = excluded.prev_total_saves,
                        trend_score = excluded.trend_score,
                        dimensions = excluded.dimensions,
                        lifecycle = excluded.lifecycle,
                        priority = excluded.priority,
                        freshness_rate = excluded.freshness_rate,
                        activity_level = excluded.activity_level,
                        new_posts_count = excluded.new_posts_count,
                        last_updated_at = excluded.last_updated_at
                """, (
                    unique_id, platform.lower(), tag.lower().lstrip("#"),
                    current.get("views", 0), current.get("likes", 0), current.get("comments", 0),
                    current.get("shares", 0), current.get("saves", 0), aggregated_stats.get("post_count", 0),
                    previous.get("views", 0), previous.get("likes", 0), previous.get("comments", 0),
                    previous.get("shares", 0), previous.get("saves", 0),
                    trend_score, json.dumps(dimensions), lifecycle, priority,
                    freshness_rate, activity_level, new_posts,
                    now, now
                ))
                conn.commit()

    def get_tag_scores(
        self,
        platform: Optional[str] = None,
        limit: int = 50,
        min_score: float = 0,
        activity_filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        获取 tag 分数排名
        
        Args:
            platform: 平台过滤
            limit: 返回数量
            min_score: 最低分数
            activity_filter: 活跃度过滤 (very_active/active/moderate/stale)
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # 构建查询
            conditions = ["trend_score >= ?"]
            params = [min_score]
            
            if platform:
                conditions.append("platform = ?")
                params.append(platform.lower())
            
            if activity_filter:
                conditions.append("activity_level = ?")
                params.append(activity_filter)
            
            where_clause = " AND ".join(conditions)
            params.append(limit)
            
            cursor.execute(f"""
                SELECT * FROM tag_scores 
                WHERE {where_clause}
                ORDER BY trend_score DESC
                LIMIT ?
            """, params)
            
            results = []
            for row in cursor.fetchall():
                # 安全获取可能不存在的列
                freshness = row["freshness_rate"] if "freshness_rate" in row.keys() else 0.5
                activity = row["activity_level"] if "activity_level" in row.keys() else "unknown"
                new_posts = row["new_posts_count"] if "new_posts_count" in row.keys() else 0
                
                results.append({
                    "platform": row["platform"],
                    "tag": row["tag"],
                    "trend_score": row["trend_score"],
                    "dimensions": json.loads(row["dimensions"]) if row["dimensions"] else {},
                    "lifecycle": row["lifecycle"],
                    "priority": row["priority"],
                    "post_count": row["post_count"],
                    "stats": {
                        "views": row["total_views"],
                        "likes": row["total_likes"],
                        "comments": row["total_comments"],
                        "shares": row["total_shares"],
                        "saves": row["total_saves"]
                    },
                    # 新增：活跃度信息
                    "activity": {
                        "freshness_rate": freshness,
                        "activity_level": activity,
                        "new_posts": new_posts,
                    },
                    "last_updated_at": row["last_updated_at"]
                })
            
            return results
    
    def get_posts_by_tag(
        self,
        platform: str,
        tag: str,
        limit: int = 30
    ) -> List[Dict[str, Any]]:
        """获取某个 tag 下的帖子列表"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT * FROM posts
                WHERE platform = ? AND tag = ?
                ORDER BY views DESC
                LIMIT ?
            """, (platform.lower(), tag.lower().lstrip("#"), limit))
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    "post_id": row["post_id"],
                    "platform": row["platform"],
                    "tag": row["tag"],
                    "author": row["author"],
                    "title": row["title"] if "title" in row.keys() else "",
                    "description": row["description"],
                    "content_url": row["content_url"],
                    "cover_url": row["cover_url"],
                    "stats": {
                        "views": row["views"],
                        "likes": row["likes"],
                        "comments": row["comments"],
                        "shares": row["shares"],
                        "saves": row["saves"]
                    },
                    "prev_stats": {
                        "views": row["prev_views"],
                        "likes": row["prev_likes"],
                        "comments": row["prev_comments"],
                        "shares": row["prev_shares"],
                        "saves": row["prev_saves"]
                    },
                    "update_count": row["update_count"],
                    "first_seen_at": row["first_seen_at"],
                    "last_updated_at": row["last_updated_at"]
                })
            
            return results
    
    def get_posts_ranking(
        self,
        platform: Optional[str] = None,
        limit: int = 50,
        min_score: float = 0
    ) -> List[Dict[str, Any]]:
        """
        获取帖子排名（按 trend_score 降序）
        
        Args:
            platform: 平台过滤（可选）
            limit: 返回数量
            min_score: 最低分数
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            if platform:
                cursor.execute("""
                    SELECT * FROM posts 
                    WHERE platform = ? AND trend_score >= ?
                    ORDER BY trend_score DESC
                    LIMIT ?
                """, (platform.lower(), min_score, limit))
            else:
                cursor.execute("""
                    SELECT * FROM posts 
                    WHERE trend_score >= ?
                    ORDER BY trend_score DESC
                    LIMIT ?
                """, (min_score, limit))
            
            results = []
            for row in cursor.fetchall():
                # 安全获取可能不存在的列
                dims = {}
                try:
                    dims = json.loads(row["dimensions"]) if row["dimensions"] else {}
                except:
                    pass
                
                results.append({
                    "id": row["id"],
                    "platform": row["platform"],
                    "tag": row["tag"],
                    "post_id": row["post_id"],
                    "author": row["author"],
                    "title": row["title"] if "title" in row.keys() else "",
                    "description": row["description"],
                    "content_url": row["content_url"],
                    "cover_url": row["cover_url"],
                    "trend_score": row["trend_score"] if "trend_score" in row.keys() else 0,
                    "dimensions": dims,
                    "lifecycle": row["lifecycle"] if "lifecycle" in row.keys() else "unknown",
                    "priority": row["priority"] if "priority" in row.keys() else "P3",
                    "stats": {
                        "views": row["views"],
                        "likes": row["likes"],
                        "comments": row["comments"],
                        "shares": row["shares"],
                        "saves": row["saves"]
                    },
                    "update_count": row["update_count"],
                    "last_updated_at": row["last_updated_at"]
                })
            
            return results

    def get_top_post_for_tag(
        self,
        platform: str,
        tag: str
    ) -> Optional[Dict[str, Any]]:
        """获取某个 tag 下最热门的帖子（用于展示代表信息）"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT author, title, description, content_url, cover_url, views
                FROM posts
                WHERE platform = ? AND tag = ?
                ORDER BY views DESC
                LIMIT 1
            """, (platform.lower(), tag.lower().lstrip("#")))
            
            row = cursor.fetchone()
            if row:
                return {
                    "author": row["author"],
                    "title": row["title"] if "title" in row.keys() else "",
                    "description": row["description"],
                    "content_url": row["content_url"],
                    "cover_url": row["cover_url"],
                    "views": row["views"]
                }
            return None
    
    def cleanup_expired(self):
        """清理过期数据"""
        cutoff = datetime.utcnow() - timedelta(hours=self._retention_hours)
        cutoff_str = cutoff.isoformat()
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # 清理过期帖子
            cursor.execute("SELECT COUNT(*) FROM posts WHERE last_updated_at < ?", (cutoff_str,))
            posts_count = cursor.fetchone()[0]
            
            if posts_count > 0:
                cursor.execute("DELETE FROM posts WHERE last_updated_at < ?", (cutoff_str,))
                logger.info(f"Cleaned {posts_count} expired posts")
            
            # 清理过期 tag 分数
            cursor.execute("SELECT COUNT(*) FROM tag_scores WHERE last_updated_at < ?", (cutoff_str,))
            tags_count = cursor.fetchone()[0]
            
            if tags_count > 0:
                cursor.execute("DELETE FROM tag_scores WHERE last_updated_at < ?", (cutoff_str,))
                logger.info(f"Cleaned {tags_count} expired tag scores")
            
            conn.commit()
            
            # VACUUM
            if posts_count + tags_count >= 100:
                conn.execute("VACUUM")
                logger.info("Database vacuumed")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取存储统计"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM posts")
            total_posts = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM tag_scores")
            total_tags = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT platform) FROM posts")
            platforms = cursor.fetchone()[0]
            
            cursor.execute("SELECT AVG(trend_score) FROM tag_scores")
            avg_score = cursor.fetchone()[0] or 0
            
            return {
                "total_posts": total_posts,
                "total_tags": total_tags,
                "platforms": platforms,
                "avg_score": round(avg_score, 2)
            }
    
    def clear_all(self):
        """清空所有数据（谨慎使用）"""
        with self._write_lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM posts")
                cursor.execute("DELETE FROM tag_scores")
                conn.execute("VACUUM")
                logger.warning("All smart history data cleared")

    def close(self):
        """关闭连接池（程序退出时调用）"""
        self._pool.close_all()

    def batch_upsert_posts(
        self,
        posts_data: List[Dict[str, Any]]
    ) -> Tuple[int, int]:
        """
        批量插入或更新帖子数据（高性能批量操作）

        Args:
            posts_data: 帖子数据列表，每个元素包含:
                - platform, tag, post_id, stats, author, title, description,
                - content_url, cover_url, post_created_at, trend_score,
                - dimensions, lifecycle, priority

        Returns:
            (new_count, updated_count): 新增数量和更新数量
        """
        if not posts_data:
            return 0, 0

        now = datetime.utcnow().isoformat()
        new_count = 0
        updated_count = 0

        with self._write_lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("BEGIN IMMEDIATE")

                try:
                    for data in posts_data:
                        platform = data.get("platform", "unknown")
                        post_id = data.get("post_id", "")
                        unique_id = f"{platform}:{post_id}"

                        stats = data.get("stats", {})
                        views = stats.get("views", 0) or 0
                        likes = stats.get("likes", 0) or 0
                        comments = stats.get("comments", 0) or 0
                        shares = stats.get("shares", 0) or 0
                        saves = stats.get("saves", 0) or 0

                        # 检查是否已存在
                        cursor.execute(
                            "SELECT id FROM posts WHERE id = ?",
                            (unique_id,)
                        )
                        existing = cursor.fetchone()

                        tag = data.get("tag", "").lower().lstrip("#")
                        author = data.get("author", "")
                        title = data.get("title", "")[:200] if data.get("title") else ""
                        description = data.get("description", "")[:500] if data.get("description") else ""
                        content_url = data.get("content_url", "")
                        cover_url = data.get("cover_url", "")
                        trend_score = data.get("trend_score", 0)
                        dimensions = json.dumps(data.get("dimensions", {}))
                        lifecycle = data.get("lifecycle", "unknown")
                        priority = data.get("priority", "P3")
                        post_created_at = data.get("post_created_at", "")

                        if existing:
                            # 更新
                            cursor.execute("""
                                UPDATE posts SET
                                    prev_views = views, prev_likes = likes,
                                    prev_comments = comments, prev_shares = shares, prev_saves = saves,
                                    views = ?, likes = ?, comments = ?, shares = ?, saves = ?,
                                    trend_score = ?, dimensions = ?, lifecycle = ?, priority = ?,
                                    last_updated_at = ?, update_count = update_count + 1
                                WHERE id = ?
                            """, (views, likes, comments, shares, saves,
                                  trend_score, dimensions, lifecycle, priority, now, unique_id))
                            updated_count += 1
                        else:
                            # 插入
                            cursor.execute("""
                                INSERT INTO posts
                                (id, platform, tag, post_id, author, title, description,
                                 content_url, cover_url, views, likes, comments, shares, saves,
                                 prev_views, prev_likes, prev_comments, prev_shares, prev_saves,
                                 trend_score, dimensions, lifecycle, priority,
                                 first_seen_at, last_updated_at, post_created_at, update_count)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 0, ?, ?, ?, ?, ?, ?, ?, 1)
                            """, (unique_id, platform.lower(), tag, post_id, author, title, description,
                                  content_url, cover_url, views, likes, comments, shares, saves,
                                  trend_score, dimensions, lifecycle, priority, now, now, post_created_at))
                            new_count += 1

                    cursor.execute("COMMIT")
                    logger.info(f"批量写入完成: {new_count} 新增, {updated_count} 更新")

                except Exception as e:
                    cursor.execute("ROLLBACK")
                    logger.error(f"批量写入失败: {e}")
                    raise

        return new_count, updated_count


# 全局单例
smart_history_store = SmartHistoryStore()
