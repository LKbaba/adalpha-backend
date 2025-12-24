"""
Stream Manager Service for SSE (Server-Sent Events).

Manages real-time data streaming from Kafka to frontend clients
using Server-Sent Events (SSE).

Architecture:
- Kafka Consumer reads from vks-scores and market-stream topics
- StreamManager broadcasts messages to all connected SSE clients
- Heartbeat keeps connections alive
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Set, Optional, AsyncGenerator
from dataclasses import dataclass, field
import threading
from queue import Queue, Empty

from confluent_kafka import Consumer, KafkaError

from app.config import get_settings
from app.services.kafka_client import kafka_client
from app.services.adaptive_trend_scorer import adaptive_trend_scorer
from app.services.smart_history_store import smart_history_store

logger = logging.getLogger(__name__)


@dataclass
class SSEClient:
    """Represents a connected SSE client."""
    client_id: str
    connected_at: datetime = field(default_factory=datetime.utcnow)
    last_event_id: Optional[str] = None
    topics: Set[str] = field(default_factory=set)


class StreamManager:
    """
    Manages SSE connections and Kafka message broadcasting.

    Features:
    - Multiple client support
    - Automatic heartbeat (every 15 seconds)
    - Kafka consumer for real-time VKS updates
    - Message queue for each client
    """

    def __init__(self):
        self._settings = get_settings()
        self._clients: Dict[str, Queue] = {}
        self._client_info: Dict[str, SSEClient] = {}
        self._running = False
        self._consumer_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._kafka_consumer: Optional[Consumer] = None
        self._lock = threading.Lock()

    @property
    def client_count(self) -> int:
        """Number of connected clients."""
        return len(self._clients)

    @property
    def is_running(self) -> bool:
        """Check if stream manager is running."""
        return self._running

    def get_stats(self) -> dict:
        """Get stream manager statistics."""
        return {
            "running": self._running,
            "client_count": len(self._clients),
            "clients": [
                {
                    "id": info.client_id,
                    "connected_at": info.connected_at.isoformat(),
                    "topics": list(info.topics)
                }
                for info in self._client_info.values()
            ]
        }

    async def start(self):
        """Start the stream manager and background tasks."""
        if self._running:
            logger.warning("Stream manager already running")
            return

        self._running = True

        # Start background tasks
        self._consumer_task = asyncio.create_task(self._kafka_consumer_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        logger.info("Stream manager started")

    async def stop(self):
        """Stop the stream manager and cleanup."""
        self._running = False

        # Cancel tasks
        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        # Close Kafka consumer
        if self._kafka_consumer:
            self._kafka_consumer.close()
            self._kafka_consumer = None

        # Clear clients
        self._clients.clear()
        self._client_info.clear()

        logger.info("Stream manager stopped")

    def register_client(self, client_id: str, topics: Optional[Set[str]] = None) -> Queue:
        """
        Register a new SSE client.

        Args:
            client_id: Unique client identifier
            topics: Set of topics to subscribe to

        Returns:
            Queue: Message queue for this client
        """
        with self._lock:
            if client_id in self._clients:
                logger.warning(f"Client {client_id} already registered, replacing")

            # 增加队列容量，避免数据量大时消息丢失
            queue = Queue(maxsize=500)  # Buffer up to 500 messages (原来是100)
            self._clients[client_id] = queue
            self._client_info[client_id] = SSEClient(
                client_id=client_id,
                topics=topics or {"vks-scores", "market-stream"}
            )

            logger.info(f"Client {client_id} registered. Total clients: {len(self._clients)}")
            return queue

    def unregister_client(self, client_id: str):
        """Remove a client from the stream manager."""
        with self._lock:
            if client_id in self._clients:
                del self._clients[client_id]
                del self._client_info[client_id]
                logger.info(f"Client {client_id} unregistered. Total clients: {len(self._clients)}")

    def broadcast(self, event_type: str, data: dict, topic: Optional[str] = None):
        """
        Broadcast a message to all connected clients.

        Args:
            event_type: SSE event type
            data: Message data
            topic: Optional topic filter (only send to clients subscribed to this topic)
        """
        message = {
            "event": event_type,
            "data": data,
            "timestamp": datetime.utcnow().isoformat()
        }

        with self._lock:
            for client_id, queue in self._clients.items():
                # Check topic filter
                if topic:
                    client_info = self._client_info.get(client_id)
                    if client_info and topic not in client_info.topics:
                        continue

                try:
                    queue.put_nowait(message)
                except Exception:
                    # Queue full, skip this message for this client
                    logger.warning(f"Queue full for client {client_id}, dropping message")

    def _parse_vks_scores_message(self, msg) -> dict:
        """
        Parse a message from vks-scores topic.

        Flink SQL outputs raw bytes in this format:
        - key: hashtag as UTF-8 bytes (e.g., b"#NFT")
        - val: VKS score as UTF-8 bytes (e.g., b"58.55")

        Returns:
            dict: Structured VKS data with hashtag and score
        """
        try:
            # Decode key (hashtag)
            key_bytes = msg.key()
            hashtag = key_bytes.decode("utf-8") if key_bytes else "unknown"

            # Decode value (VKS score)
            val_bytes = msg.value()
            vks_score_str = val_bytes.decode("utf-8") if val_bytes else "0"

            # Try to parse as float
            try:
                vks_score = float(vks_score_str)
            except ValueError:
                vks_score = 0.0
                logger.warning(f"Could not parse VKS score: {vks_score_str}")

            return {
                "hashtag": hashtag,
                "vks_score": vks_score,
                "timestamp": datetime.utcnow().isoformat(),
                "source": "flink_sql"
            }
        except Exception as e:
            logger.error(f"Error parsing vks-scores message: {e}")
            return {
                "hashtag": "error",
                "vks_score": 0.0,
                "error": str(e)
            }

    def _calculate_vks_from_market_data(self, data: dict) -> dict:
        """
        Calculate Trend Score from market-stream raw data using adaptive_trend_scorer.
        
        Uses the full 6-dimension scoring system:
        - H (Hotness): 热度
        - V (Velocity): 增速
        - D (Density): 密度
        - F (Feasibility): 可行性
        - M (Monetization): 商业化
        - R (Risk): 风险
        
        Formula: trend_score = 0.20*H + 0.30*V + 0.15*D + 0.15*F + 0.20*M - 0.25*R
        """
        try:
            # 提取平台信息
            platform = data.get("platform", "unknown")
            
            # 提取 hashtag
            hashtag = data.get("hashtag", data.get("tag", "unknown"))
            if hashtag and not hashtag.startswith("#"):
                hashtag = f"#{hashtag}"
            
            # 提取作者信息
            author = data.get("author", {})
            if isinstance(author, dict):
                author_name = author.get("username") or author.get("nickname") or "unknown"
            else:
                author_name = str(author) if author else "unknown"
            
            # 提取内容对象
            content = data.get("content", {})
            
            # 提取标题 (title) - 各平台字段名不同
            # TikTok: content.title
            # YouTube: content.title 或 rawData.title
            # Instagram: content.title 或 caption
            # Twitter: text 或 content.text
            # Reddit: title
            title = ""
            if isinstance(content, dict):
                title = content.get("title", "")
            if not title:
                title = (
                    data.get("title", "") or  # 通用 (Kafka 标准格式)
                    data.get("text", "") or   # Twitter
                    data.get("caption", "") or  # Instagram
                    data.get("name", "")  # Reddit
                )
            # 从 rawData 中尝试获取
            raw_data_inner = data.get("rawData", {}) or data.get("raw", {})
            if not title and isinstance(raw_data_inner, dict):
                title = (
                    raw_data_inner.get("title", "") or 
                    raw_data_inner.get("desc", "") or
                    raw_data_inner.get("text", "") or  # Twitter raw
                    raw_data_inner.get("full_text", "")  # Twitter full_text
                )
            
            # 调试日志
            if platform.lower() == "twitter" and not title:
                logger.warning(f"[DEBUG] Twitter post missing title. Keys: {list(data.keys())}, raw keys: {list(raw_data_inner.keys()) if isinstance(raw_data_inner, dict) else 'N/A'}")
            
            # 提取描述 (description) - 通常是更长的文本
            description = data.get("description", "")
            if not description and isinstance(content, dict):
                description = content.get("description", "") or content.get("text", "")
            if not description and isinstance(raw_data_inner, dict):
                description = raw_data_inner.get("description", "") or raw_data_inner.get("desc", "")
            # 如果没有 description，使用 title 作为 fallback
            if not description:
                description = title
            
            # 提取 URL
            content_url = ""
            cover_url = ""
            
            # 优先从顶层字段获取 (Kafka 发送的标准格式)
            content_url = data.get("content_url", "")
            cover_url = data.get("cover_url", "")
            
            # 如果顶层没有，从 content 对象获取
            if not content_url and isinstance(content, dict):
                content_url = content.get("url", "")
            if not cover_url and isinstance(content, dict):
                cover_url = content.get("coverUrl", "") or content.get("thumbnailUrl", "") or content.get("mediaUrl", "")
            
            # 最后尝试其他字段
            if not content_url:
                content_url = data.get("url", "") or data.get("link", "") or data.get("share_url", "")
            if not cover_url:
                cover_url = data.get("coverUrl", "") or data.get("thumbnail", "") or data.get("image", "") or data.get("cover", "")
            
            # 提取帖子 ID
            post_id = data.get("post_id") or data.get("id") or "unknown"
            
            # 构建爬虫数据格式供 adaptive_trend_scorer 使用
            crawl_item = {
                "platform": platform,
                "id": post_id,
                "stats": data.get("stats", {
                    "views": data.get("views", 0) or 0,
                    "likes": data.get("likes", 0) or 0,
                    "comments": data.get("comments", 0) or 0,
                    "shares": data.get("shares", 0) or 0,
                    "saves": data.get("saves", 0) or 0,
                    # Reddit 特殊字段
                    "upvotes": data.get("upvotes", 0) or 0,
                    "downvotes": data.get("downvotes", 0) or 0,
                    "score": data.get("score", 0) or 0,
                })
            }
            
            # 如果 stats 字段不存在，从顶层提取
            if not data.get("stats"):
                crawl_item["stats"] = {
                    "views": data.get("views", 0) or 0,
                    "likes": data.get("likes", 0) or 0,
                    "comments": data.get("comments", 0) or 0,
                    "shares": data.get("shares", 0) or 0,
                    "saves": data.get("saves", 0) or 0,
                    "upvotes": data.get("upvotes", 0) or 0,
                    "downvotes": data.get("downvotes", 0) or 0,
                    "score": data.get("score", 0) or 0,
                }
            
            # 使用 adaptive_trend_scorer 计算完整的 Trend Score
            score_result = adaptive_trend_scorer.compute_from_crawl_item(
                item=crawl_item,
                keyword=hashtag.lstrip("#")
            )
            
            logger.info(f"📊 Adaptive Trend Score - platform: {platform}, keyword: {hashtag}, "
                        f"trend_score: {score_result.get('trend_score')}, "
                        f"H={score_result.get('H')}, V={score_result.get('V')}, "
                        f"D={score_result.get('D')}, F={score_result.get('F')}, "
                        f"M={score_result.get('M')}, R={score_result.get('R')}")
            
            # 构建结果
            result = {
                "hashtag": hashtag,
                "vks_score": score_result.get("trend_score", 0),  # 兼容旧字段名
                "trend_score": score_result.get("trend_score", 0),
                "platform": platform,
                "post_id": post_id,
                "author": author_name,
                "title": title[:200] if title else "",  # 新增：帖子标题
                "description": description[:500] if description else "",
                "content_url": content_url,
                "cover_url": cover_url,
                "timestamp": datetime.utcnow().isoformat(),
                "source": "adaptive_trend_scorer",
                # 6 维度分数
                "dimensions": {
                    "H": score_result.get("H", 0),  # 热度
                    "V": score_result.get("V", 0),  # 增速
                    "D": score_result.get("D", 0),  # 密度
                    "F": score_result.get("F", 0),  # 可行性
                    "M": score_result.get("M", 0),  # 商业化
                    "R": score_result.get("R", 0),  # 风险
                },
                # 元数据
                "lifecycle": score_result.get("lifecycle", "unknown"),
                "priority": score_result.get("priority", "P3"),
                "agent_ready": score_result.get("agent_ready", False),
                "category": score_result.get("category", "general"),
                # 原始指标
                "metrics": score_result.get("raw_metrics", {
                    "views": crawl_item["stats"].get("views", 0),
                    "likes": crawl_item["stats"].get("likes", 0),
                    "comments": crawl_item["stats"].get("comments", 0),
                    "shares": crawl_item["stats"].get("shares", 0),
                    "saves": crawl_item["stats"].get("saves", 0),
                })
            }
            
            # 智能存储：去重 + 更新 + 聚合计算
            try:
                # 1. 存储/更新帖子数据（自动去重），包含单帖分数
                is_new, prev_stats = smart_history_store.upsert_post(
                    platform=platform,
                    tag=hashtag.lstrip("#"),
                    post_id=post_id,
                    stats=crawl_item["stats"],
                    author=author_name,
                    title=title[:200] if title else "",
                    description=description[:200] if description else "",
                    content_url=content_url,
                    cover_url=cover_url,
                    post_created_at=data.get("created_at", ""),
                    # 单帖分数
                    trend_score=score_result.get("trend_score", 0),
                    dimensions=result["dimensions"],
                    lifecycle=score_result.get("lifecycle", "unknown"),
                    priority=score_result.get("priority", "P3")
                )
                
                # 2. 获取该 tag 的聚合数据（包含新鲜度）
                aggregated = smart_history_store.get_tag_aggregated_stats(
                    platform, hashtag, 
                    current_batch_size=20  # 每次爬取约 20 条
                )
                
                # 3. 使用聚合数据重新计算 Trend Score（带增长率 + 新鲜度）
                from app.services.adaptive_trend_scorer import compute_adaptive_trend_score
                aggregated_score = compute_adaptive_trend_score(
                    keyword=hashtag.lstrip("#"),
                    platform_str=platform,
                    raw_stats=aggregated["current"],
                    prev_raw_stats=aggregated["previous"] if aggregated["previous"]["views"] > 0 else None,
                    posts=aggregated["post_count"],
                    freshness_rate=aggregated.get("freshness_rate", 0.5),
                    new_posts=aggregated.get("new_posts", 0),
                    activity_level=aggregated.get("activity_level", "unknown")
                )
                
                # 4. 保存 tag 聚合分数
                smart_history_store.save_tag_score(
                    platform=platform,
                    tag=hashtag,
                    aggregated_stats=aggregated,
                    trend_score=aggregated_score["trend_score"],
                    dimensions={
                        "H": aggregated_score["H"],
                        "V": aggregated_score["V"],
                        "D": aggregated_score["D"],
                        "F": aggregated_score["F"],
                        "M": aggregated_score["M"],
                        "R": aggregated_score["R"]
                    },
                    lifecycle=aggregated_score["lifecycle"],
                    priority=aggregated_score["priority"]
                )
                
                # 更新返回结果为聚合分数
                result["trend_score"] = aggregated_score["trend_score"]
                result["vks_score"] = aggregated_score["trend_score"]
                result["dimensions"] = {
                    "H": aggregated_score["H"],
                    "V": aggregated_score["V"],
                    "D": aggregated_score["D"],
                    "F": aggregated_score["F"],
                    "M": aggregated_score["M"],
                    "R": aggregated_score["R"]
                }
                result["lifecycle"] = aggregated_score["lifecycle"]
                result["priority"] = aggregated_score["priority"]
                result["is_new_post"] = is_new
                result["post_count"] = aggregated["post_count"]
                result["new_posts_count"] = aggregated["new_posts"]
                
                # 新增：活跃度信息
                result["activity"] = {
                    "freshness_rate": aggregated.get("freshness_rate", 0),
                    "activity_level": aggregated.get("activity_level", "unknown"),
                    "new_posts": aggregated.get("new_posts", 0),
                }
                
                status = "NEW" if is_new else "UPDATED"
                freshness = aggregated.get("freshness_rate", 0)
                activity = aggregated.get("activity_level", "?")
                logger.info(f"📦 [{status}] {platform}/{hashtag}: posts={aggregated['post_count']}, "
                           f"score={aggregated_score['trend_score']}, V={aggregated_score['V']:.2f}, "
                           f"D={aggregated_score['D']:.2f}, freshness={freshness:.0%} ({activity})")
                
            except Exception as e:
                logger.warning(f"Failed to smart store: {e}", exc_info=True)
            
            return result
        except Exception as e:
            logger.error(f"Error calculating Trend Score from market data: {e}", exc_info=True)
            return {
                "hashtag": data.get("hashtag", data.get("tag", "unknown")),
                "vks_score": 0.0,
                "trend_score": 0.0,
                "platform": data.get("platform", "unknown"),
                "error": str(e),
                "source": "error_fallback"
            }

    async def _kafka_consumer_loop(self):
        """Background task to consume Kafka messages."""
        loop = asyncio.get_event_loop()
        
        try:
            # Initialize consumer with unique group id to get all messages
            import time
            unique_group_id = f"adalpha-sse-stream-{int(time.time())}"
            
            # 在线程池中初始化 consumer，避免阻塞事件循环
            self._kafka_consumer = await loop.run_in_executor(
                None, 
                lambda: kafka_client.get_consumer(group_id=unique_group_id)
            )
            await loop.run_in_executor(
                None,
                lambda: self._kafka_consumer.subscribe(["vks-scores", "market-stream"])
            )

            logger.info(f"Kafka consumer started for SSE streaming (group: {unique_group_id})")
            logger.info("Subscribed to topics: vks-scores, market-stream")

            while self._running:
                try:
                    # 在线程池中执行阻塞的 poll 操作
                    msg = await loop.run_in_executor(
                        None,
                        lambda: self._kafka_consumer.poll(timeout=1.0)
                    )

                    if msg is None:
                        # 没有消息，继续循环
                        continue

                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            logger.debug(f"Reached end of partition for {msg.topic()}")
                            continue
                        logger.error(f"Kafka error: {msg.error()}")
                        continue

                    # 收到消息，打印日志
                    logger.info(f"📨 Received message from topic: {msg.topic()}, partition: {msg.partition()}, offset: {msg.offset()}")

                    # Parse message
                    topic = msg.topic()

                    # Handle vks-scores topic specially (Flink SQL outputs raw bytes)
                    if topic == "vks-scores":
                        data = self._parse_vks_scores_message(msg)
                        event_type = "vks_update"
                        # Broadcast to clients
                        logger.info(f"📤 Broadcasting {event_type} to {self.client_count} clients: {data}")
                        self.broadcast(event_type, data, topic)
                        
                    elif topic == "market-stream":
                        # market-stream has JSON format from crawler
                        value = msg.value().decode("utf-8")
                        try:
                            raw_data = json.loads(value)
                        except json.JSONDecodeError:
                            raw_data = {"raw": value}

                        # 🔧 修复：检查数据是否被 Kafka 中间件包装
                        # 如果数据被包装，keys 会是 ['event_id', 'event_type', 'data', 'source', 'ingested_at']
                        # 真正的社交数据藏在 'data' 字段里
                        if 'data' in raw_data and isinstance(raw_data.get('data'), dict):
                            # 数据被包装了，解包获取真实数据
                            actual_data = raw_data['data']
                            logger.info(f"📦 Unwrapped packaged data: event_type={raw_data.get('event_type')}, source={raw_data.get('source')}")
                        else:
                            # 数据未被包装，直接使用
                            actual_data = raw_data

                        # 🔍 调试：打印解包后的数据结构
                        data_type = actual_data.get("type", "NO_TYPE")
                        data_keys = list(actual_data.keys())[:10]  # 前10个key
                        logger.info(f"📥 market-stream data: type={data_type}, keys={data_keys}")

                        # 1. 发送原始 trend_update 事件（使用解包后的数据）
                        logger.info(f"📤 Broadcasting trend_update to {self.client_count} clients")
                        self.broadcast("trend_update", actual_data, topic)

                        # 2. 计算 VKS 并发送 vks_update 事件
                        # 使用解包后的数据判断
                        has_social_data = (
                            actual_data.get("type") == "social_post" or
                            actual_data.get("platform") or
                            actual_data.get("hashtag") or
                            actual_data.get("tag")
                        )

                        if has_social_data:
                            vks_data = self._calculate_vks_from_market_data(actual_data)
                            logger.info(f"📤 Broadcasting vks_update (calculated) to {self.client_count} clients: hashtag={vks_data.get('hashtag')}, score={vks_data.get('trend_score')}")
                            self.broadcast("vks_update", vks_data, "vks-scores")
                        else:
                            logger.warning(f"⚠️ Skipping vks_update: no social data fields found in {data_keys}")
                    else:
                        value = msg.value().decode("utf-8")
                        try:
                            data = json.loads(value)
                        except json.JSONDecodeError:
                            data = {"raw": value}
                        event_type = "message"
                        # Broadcast to clients
                        logger.info(f"📤 Broadcasting {event_type} to {self.client_count} clients: {data}")
                        self.broadcast(event_type, data, topic)

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in Kafka consumer loop: {e}", exc_info=True)
                    await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Failed to start Kafka consumer: {e}", exc_info=True)

    async def _heartbeat_loop(self):
        """Send periodic heartbeat to keep SSE connections alive."""
        while self._running:
            try:
                await asyncio.sleep(15)  # Heartbeat every 15 seconds

                if self._clients:
                    self.broadcast("heartbeat", {
                        "type": "ping",
                        "client_count": len(self._clients),
                        "timestamp": datetime.utcnow().isoformat()
                    })
                    logger.debug(f"Sent heartbeat to {len(self._clients)} clients")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}")


# Global singleton
stream_manager = StreamManager()


async def sse_event_generator(
    client_id: str,
    topics: Optional[Set[str]] = None
) -> AsyncGenerator[str, None]:
    """
    Async generator for SSE events.

    Args:
        client_id: Unique client identifier
        topics: Optional set of topics to subscribe to

    Yields:
        SSE formatted event strings
    """
    # Ensure stream manager is running
    if not stream_manager.is_running:
        await stream_manager.start()

    # Register client and get queue
    queue = stream_manager.register_client(client_id, topics)

    # Send initial connection event
    yield format_sse_event("connected", {
        "client_id": client_id,
        "message": "Connected to ADALPHA real-time stream"
    })

    try:
        while True:
            try:
                # Non-blocking queue check
                message = queue.get_nowait()
                yield format_sse_event(message["event"], message["data"])
            except Empty:
                # No message available, yield nothing and wait
                await asyncio.sleep(0.1)

    except asyncio.CancelledError:
        pass
    except GeneratorExit:
        pass
    finally:
        stream_manager.unregister_client(client_id)


def format_sse_event(event_type: str, data: dict, event_id: Optional[str] = None) -> str:
    """
    Format data as an SSE event string.

    Args:
        event_type: Event type name
        data: Data to send
        event_id: Optional event ID for reconnection

    Returns:
        SSE formatted string
    """
    lines = []

    if event_id:
        lines.append(f"id: {event_id}")

    lines.append(f"event: {event_type}")
    lines.append(f"data: {json.dumps(data)}")
    lines.append("")  # Empty line to end event

    return "\n".join(lines) + "\n"
