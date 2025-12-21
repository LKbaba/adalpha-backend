"""
配置存储服务 - 使用 SQLite 存储系统配置
支持爬虫配置、平台开关等动态配置项
"""

import json
import sqlite3
import logging
from pathlib import Path
from typing import Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# 数据库路径
DB_PATH = Path(__file__).parent.parent.parent / "data" / "config.db"


def get_connection():
    """获取数据库连接"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """初始化数据库表"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS system_config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            description TEXT,
            updated_at TEXT NOT NULL
        )
    """)
    
    conn.commit()
    conn.close()
    logger.info("Config database initialized")


# 默认配置
DEFAULT_CONFIG = {
    "spider.tags": {
        "value": ["music", "dance", "ai art", "trending"],
        "description": "爬虫抓取的标签列表"
    },
    "spider.use_mock": {
        "value": True,
        "description": "是否使用 Mock 模式（不消耗 API）"
    },
    "spider.limit": {
        "value": 20,
        "description": "每个标签抓取的帖子数量"
    },
    "spider.request_delay": {
        "value": 500,
        "description": "请求间隔（毫秒）"
    },
    "platforms.tiktok": {
        "value": True,
        "description": "启用 TikTok 平台"
    },
    "platforms.instagram": {
        "value": True,
        "description": "启用 Instagram 平台"
    },
    "platforms.twitter": {
        "value": True,
        "description": "启用 Twitter/X 平台"
    },
    "platforms.youtube": {
        "value": True,
        "description": "启用 YouTube 平台"
    },
    "platforms.reddit": {
        "value": True,
        "description": "启用 Reddit 平台"
    },
    "platforms.linkedin": {
        "value": False,
        "description": "启用 LinkedIn 平台"
    }
}


def ensure_defaults():
    """确保默认配置存在"""
    conn = get_connection()
    cursor = conn.cursor()
    
    for key, config in DEFAULT_CONFIG.items():
        cursor.execute("SELECT key FROM system_config WHERE key = ?", (key,))
        if not cursor.fetchone():
            cursor.execute(
                "INSERT INTO system_config (key, value, description, updated_at) VALUES (?, ?, ?, ?)",
                (key, json.dumps(config["value"]), config["description"], datetime.utcnow().isoformat())
            )
    
    conn.commit()
    conn.close()


def get_config(key: str) -> Optional[Any]:
    """获取单个配置项"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM system_config WHERE key = ?", (key,))
    row = cursor.fetchone()
    conn.close()
    
    if row:
        return json.loads(row["value"])
    return None


def set_config(key: str, value: Any, description: str = None) -> bool:
    """设置配置项"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT key FROM system_config WHERE key = ?", (key,))
    exists = cursor.fetchone()
    
    if exists:
        if description:
            cursor.execute(
                "UPDATE system_config SET value = ?, description = ?, updated_at = ? WHERE key = ?",
                (json.dumps(value), description, datetime.utcnow().isoformat(), key)
            )
        else:
            cursor.execute(
                "UPDATE system_config SET value = ?, updated_at = ? WHERE key = ?",
                (json.dumps(value), datetime.utcnow().isoformat(), key)
            )
    else:
        cursor.execute(
            "INSERT INTO system_config (key, value, description, updated_at) VALUES (?, ?, ?, ?)",
            (key, json.dumps(value), description or "", datetime.utcnow().isoformat())
        )
    
    conn.commit()
    conn.close()
    logger.info(f"Config updated: {key} = {value}")
    return True


def get_all_configs() -> dict:
    """获取所有配置"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT key, value, description, updated_at FROM system_config")
    rows = cursor.fetchall()
    conn.close()
    
    return {
        row["key"]: {
            "value": json.loads(row["value"]),
            "description": row["description"],
            "updated_at": row["updated_at"]
        }
        for row in rows
    }


def get_spider_config() -> dict:
    """获取爬虫配置（供爬虫服务调用）"""
    configs = get_all_configs()
    
    return {
        "useMock": configs.get("spider.use_mock", {}).get("value", True),
        "spider": {
            "tags": configs.get("spider.tags", {}).get("value", ["music", "dance"]),
            "limit": configs.get("spider.limit", {}).get("value", 20),
            "requestDelay": configs.get("spider.request_delay", {}).get("value", 500)
        },
        "platforms": {
            "tiktok": {"enabled": configs.get("platforms.tiktok", {}).get("value", True), "name": "TikTok"},
            "instagram": {"enabled": configs.get("platforms.instagram", {}).get("value", True), "name": "Instagram"},
            "twitter": {"enabled": configs.get("platforms.twitter", {}).get("value", True), "name": "Twitter/X"},
            "youtube": {"enabled": configs.get("platforms.youtube", {}).get("value", True), "name": "YouTube"},
            "reddit": {"enabled": configs.get("platforms.reddit", {}).get("value", True), "name": "Reddit"},
            "linkedin": {"enabled": configs.get("platforms.linkedin", {}).get("value", False), "name": "LinkedIn"}
        }
    }


# 初始化
init_db()
ensure_defaults()
