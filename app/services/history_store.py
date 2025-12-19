"""
History Store Service - 历史数据存储服务 (SQLite 版本)

使用 SQLite 持久化存储过去2小时内的得分数据和原始数据，支持：
- 按平台查询
- 按时间范围查询
- 按分数排名
- 自动清理过期数据
"""

import logging
import sqlite3
import json
import os
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# 数据保留时间（小时）
RETENTION_HOURS = 2

# 数据库文件路径
DB_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data")
DB_PATH = os.path.join(DB_DIR, "history.db")


class HistoryStore:
    """
    历史数据存储管理器 (SQLite 版本)
    
    Features:
    - SQLite 持久化存储
    - 自动清理超过2小时的数据
    - 按平台分组查询
    - 支持排名查询
    - 线程安全
    """
    
    def __init__(self, db_path: str = DB_PATH, retention_hours: int = RETENTION_HOURS):
        self._db_path = db_path
        self._retention_hours = retention_hours
        self._lock = threading.Lock()
        self._record_count = 0
        self._last_vacuum_time = datetime.utcnow()
        self._vacuum_interval_hours = 24  # 每24小时自动 VACUUM 一次
        
        # 确保数据目录存在
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # 初始化数据库
        self._init_db()
        
        logger.info(f"HistoryStore initialized with SQLite: {db_path}")
    
    @contextmanager
    def _get_connection(self):
        """获取数据库连接的上下文管理器"""
        conn = sqlite3.connect(self._db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def _init_db(self):
        """初始化数据库表"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # 创建主表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS score_records (
                    id TEXT PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    hashtag TEXT NOT NULL,
                    trend_score REAL NOT NULL,
                    dimensions TEXT NOT NULL,
                    raw_data TEXT NOT NULL,
                    author TEXT DEFAULT '',
                    description TEXT DEFAULT '',
                    post_id TEXT DEFAULT '',
                    content_url TEXT DEFAULT '',
                    cover_url TEXT DEFAULT '',
                    lifecycle TEXT DEFAULT 'unknown',
                    priority TEXT DEFAULT 'P3',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 创建索引
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_platform 
                ON score_records(platform)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_timestamp 
                ON score_records(timestamp)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_trend_score 
                ON score_records(trend_score DESC)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_hashtag 
                ON score_records(hashtag)
            """)
            
            conn.commit()
            
            # 获取当前记录数
            cursor.execute("SELECT COUNT(*) FROM score_records")
            self._record_count = cursor.fetchone()[0]
            
            logger.info(f"Database initialized, existing records: {self._record_count}")
    
    @property
    def total_records(self) -> int:
        """总记录数"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM score_records")
            return cursor.fetchone()[0]
    
    def add_record(
        self,
        platform: str,
        hashtag: str,
        trend_score: float,
        dimensions: Dict[str, float],
        raw_data: Dict[str, Any],
        author: str = "",
        description: str = "",
        post_id: str = "",
        content_url: str = "",
        cover_url: str = "",
        lifecycle: str = "unknown",
        priority: str = "P3"
    ) -> dict:
        """
        添加一条得分记录
        
        Args:
            platform: 平台名称
            hashtag: 话题标签
            trend_score: 综合得分
            dimensions: 6维度分数 {H, V, D, F, M, R}
            raw_data: 原始爬虫数据
            author: 作者
            description: 内容描述
            post_id: 帖子ID
            content_url: 内容链接
            cover_url: 封面图链接
            lifecycle: 生命周期阶段
            priority: 优先级
            
        Returns:
            dict: 创建的记录
        """
        with self._lock:
            self._record_count += 1
            now = datetime.utcnow()
            record_id = f"{platform}-{self._record_count}-{now.strftime('%H%M%S%f')}"
            
            record = {
                "id": record_id,
                "timestamp": now.isoformat(),
                "platform": platform.upper(),
                "hashtag": hashtag,
                "trend_score": trend_score,
                "dimensions": dimensions,
                "raw_data": raw_data,
                "author": author,
                "description": description[:500] if description else "",
                "post_id": post_id,
                "content_url": content_url,
                "cover_url": cover_url,
                "lifecycle": lifecycle,
                "priority": priority
            }
            
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO score_records 
                    (id, timestamp, platform, hashtag, trend_score, dimensions, 
                     raw_data, author, description, post_id, content_url, cover_url, lifecycle, priority)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    record_id,
                    now.isoformat(),
                    platform.upper(),
                    hashtag,
                    trend_score,
                    json.dumps(dimensions),
                    json.dumps(raw_data),
                    author,
                    description[:500] if description else "",
                    post_id,
                    content_url,
                    cover_url,
                    lifecycle,
                    priority
                ))
                conn.commit()
            
            logger.debug(f"Added record: {record_id}, platform={platform}, score={trend_score}")
            
            # 定期清理（每100条记录清理一次）
            if self._record_count % 100 == 0:
                self._cleanup_expired()
                # 检查是否需要定期 VACUUM
                self._check_periodic_vacuum()
                
            return record
    
    def _cleanup_expired(self):
        """清理过期数据"""
        cutoff = datetime.utcnow() - timedelta(hours=self._retention_hours)
        cutoff_str = cutoff.isoformat()
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM score_records WHERE timestamp < ?", (cutoff_str,))
            count_before = cursor.fetchone()[0]
            
            if count_before > 0:
                cursor.execute("DELETE FROM score_records WHERE timestamp < ?", (cutoff_str,))
                conn.commit()
                logger.info(f"Cleaned {count_before} expired records")
                
                # 删除超过1000条记录时，执行 VACUUM 释放磁盘空间
                if count_before >= 1000:
                    self._vacuum_db()
    
    def _vacuum_db(self):
        """执行 VACUUM 释放磁盘空间"""
        try:
            with self._get_connection() as conn:
                conn.execute("VACUUM")
                self._last_vacuum_time = datetime.utcnow()
                logger.info("Database vacuumed, disk space reclaimed")
        except Exception as e:
            logger.warning(f"Failed to vacuum database: {e}")
    
    def _check_periodic_vacuum(self):
        """检查是否需要定期 VACUUM"""
        hours_since_vacuum = (datetime.utcnow() - self._last_vacuum_time).total_seconds() / 3600
        if hours_since_vacuum >= self._vacuum_interval_hours:
            self._vacuum_db()
    
    def _row_to_dict(self, row: sqlite3.Row) -> dict:
        """将数据库行转换为字典"""
        result = {
            "id": row["id"],
            "timestamp": row["timestamp"],
            "platform": row["platform"],
            "hashtag": row["hashtag"],
            "trend_score": row["trend_score"],
            "dimensions": json.loads(row["dimensions"]),
            "raw_data": json.loads(row["raw_data"]),
            "author": row["author"],
            "description": row["description"],
            "post_id": row["post_id"],
            "lifecycle": row["lifecycle"],
            "priority": row["priority"]
        }
        # 兼容旧数据（没有 content_url 和 cover_url 字段）
        try:
            result["content_url"] = row["content_url"] or ""
            result["cover_url"] = row["cover_url"] or ""
        except (IndexError, KeyError):
            result["content_url"] = ""
            result["cover_url"] = ""
        return result
    
    def get_all(
        self,
        limit: int = 100,
        offset: int = 0,
        sort_by_score: bool = True
    ) -> List[dict]:
        """
        获取所有记录
        
        Args:
            limit: 返回数量限制
            offset: 偏移量
            sort_by_score: 是否按分数排序
            
        Returns:
            List[dict]: 记录列表
        """
        self._cleanup_expired()
        
        order_by = "trend_score DESC" if sort_by_score else "timestamp DESC"
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT * FROM score_records 
                ORDER BY {order_by}
                LIMIT ? OFFSET ?
            """, (limit, offset))
            
            return [self._row_to_dict(row) for row in cursor.fetchall()]
    
    def get_by_platform(
        self,
        platform: str,
        limit: int = 50,
        sort_by_score: bool = True
    ) -> List[dict]:
        """
        按平台获取记录
        
        Args:
            platform: 平台名称
            limit: 返回数量限制
            sort_by_score: 是否按分数排序
            
        Returns:
            List[dict]: 记录列表（带排名）
        """
        self._cleanup_expired()
        
        order_by = "trend_score DESC" if sort_by_score else "timestamp DESC"
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT * FROM score_records 
                WHERE platform = ?
                ORDER BY {order_by}
                LIMIT ?
            """, (platform.upper(), limit))
            
            result = []
            for rank, row in enumerate(cursor.fetchall(), 1):
                data = self._row_to_dict(row)
                data["rank"] = rank
                result.append(data)
            
            return result
    
    def get_rankings(self, top_n: int = 20) -> Dict[str, dict]:
        """
        获取各平台排名
        
        Args:
            top_n: 每个平台返回的数量
            
        Returns:
            Dict[str, dict]: 按平台分组的排名数据
        """
        self._cleanup_expired()
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # 获取所有平台
            cursor.execute("SELECT DISTINCT platform FROM score_records")
            platforms = [row[0] for row in cursor.fetchall()]
            
            rankings = {}
            
            for platform in platforms:
                cursor.execute("""
                    SELECT * FROM score_records 
                    WHERE platform = ?
                    ORDER BY trend_score DESC
                    LIMIT ?
                """, (platform, top_n))
                
                records = []
                for rank, row in enumerate(cursor.fetchall(), 1):
                    data = self._row_to_dict(row)
                    data["rank"] = rank
                    records.append(data)
                
                rankings[platform] = {
                    "platform": platform,
                    "records": records,
                    "total": len(records)
                }
            
            return rankings
    
    def get_stats(self) -> dict:
        """获取存储统计信息"""
        self._cleanup_expired()
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # 总记录数
            cursor.execute("SELECT COUNT(*) FROM score_records")
            total = cursor.fetchone()[0]
            
            # 按平台统计
            cursor.execute("""
                SELECT platform, COUNT(*) as count, AVG(trend_score) as avg_score
                FROM score_records
                GROUP BY platform
            """)
            
            platform_counts = {}
            avg_scores = {}
            for row in cursor.fetchall():
                platform_counts[row["platform"]] = row["count"]
                avg_scores[row["platform"]] = round(row["avg_score"], 2) if row["avg_score"] else 0
            
            # 最早和最新记录
            cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM score_records")
            time_range = cursor.fetchone()
            
            # 按小时统计（估算爬取批次）
            cursor.execute("""
                SELECT 
                    strftime('%Y-%m-%d %H:00', timestamp) as hour,
                    COUNT(*) as count
                FROM score_records
                GROUP BY hour
                ORDER BY hour DESC
                LIMIT 10
            """)
            hourly_stats = [{"hour": row[0], "count": row[1]} for row in cursor.fetchall()]
            
            # 唯一 hashtag 数量
            cursor.execute("SELECT COUNT(DISTINCT hashtag) FROM score_records")
            unique_hashtags = cursor.fetchone()[0]
            
            return {
                "total_records": total,
                "retention_hours": self._retention_hours,
                "platforms": platform_counts,
                "average_scores": avg_scores,
                "oldest_record": time_range[0],
                "newest_record": time_range[1],
                "db_path": self._db_path,
                "unique_hashtags": unique_hashtags,
                "hourly_stats": hourly_stats,
            }
    
    def get_time_series(
        self,
        platform: Optional[str] = None,
        minutes: int = 30,
        interval_seconds: int = 60
    ) -> List[dict]:
        """
        获取时间序列数据（用于图表）
        
        Args:
            platform: 平台过滤（可选）
            minutes: 时间范围（分钟）
            interval_seconds: 聚合间隔（秒）
            
        Returns:
            List[dict]: 时间序列数据点
        """
        cutoff = datetime.utcnow() - timedelta(minutes=minutes)
        cutoff_str = cutoff.isoformat()
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            if platform:
                cursor.execute("""
                    SELECT timestamp, trend_score FROM score_records
                    WHERE platform = ? AND timestamp > ?
                    ORDER BY timestamp
                """, (platform.upper(), cutoff_str))
            else:
                cursor.execute("""
                    SELECT timestamp, trend_score FROM score_records
                    WHERE timestamp > ?
                    ORDER BY timestamp
                """, (cutoff_str,))
            
            rows = cursor.fetchall()
            
            if not rows:
                return []
            
            # 按时间间隔聚合
            buckets: Dict[str, List[float]] = {}
            
            for row in rows:
                try:
                    ts = datetime.fromisoformat(row["timestamp"])
                    # 向下取整到最近的间隔
                    bucket_time = ts.replace(
                        second=(ts.second // interval_seconds) * interval_seconds,
                        microsecond=0
                    )
                    bucket_key = bucket_time.strftime("%H:%M:%S")
                    
                    if bucket_key not in buckets:
                        buckets[bucket_key] = []
                    buckets[bucket_key].append(row["trend_score"])
                except Exception:
                    continue
            
            # 计算每个桶的统计值
            result = []
            for time_key in sorted(buckets.keys()):
                scores = buckets[time_key]
                result.append({
                    "time": time_key,
                    "avg_score": round(sum(scores) / len(scores), 2),
                    "max_score": round(max(scores), 2),
                    "min_score": round(min(scores), 2),
                    "count": len(scores)
                })
            
            return result
    
    def clear_all(self):
        """清空所有数据（谨慎使用）"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM score_records")
            conn.commit()
            self._record_count = 0
            logger.warning("All records cleared from database")
        # 清空后立即释放空间
        self._vacuum_db()
    
    def vacuum(self):
        """手动触发 VACUUM 释放磁盘空间"""
        self._vacuum_db()
        return self.get_db_size()
    
    def get_db_size(self) -> dict:
        """获取数据库文件大小信息"""
        try:
            file_size = os.path.getsize(self._db_path)
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("PRAGMA page_count")
                page_count = cursor.fetchone()[0]
                cursor.execute("PRAGMA freelist_count")
                free_pages = cursor.fetchone()[0]
                cursor.execute("PRAGMA page_size")
                page_size = cursor.fetchone()[0]
            
            return {
                "file_size_bytes": file_size,
                "file_size_mb": round(file_size / 1024 / 1024, 2),
                "page_count": page_count,
                "free_pages": free_pages,
                "used_pages": page_count - free_pages,
                "page_size": page_size,
                "fragmentation_percent": round(free_pages / max(page_count, 1) * 100, 2)
            }
        except Exception as e:
            logger.error(f"Failed to get db size: {e}")
            return {"error": str(e)}
    
    def maintenance(self):
        """执行数据库维护：清理过期数据 + VACUUM"""
        self._cleanup_expired()
        self._vacuum_db()
        return self.get_db_size()


# 全局单例
history_store = HistoryStore()
