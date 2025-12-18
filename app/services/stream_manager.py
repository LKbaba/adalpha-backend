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

            queue = Queue(maxsize=100)  # Buffer up to 100 messages
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
        Calculate VKS score from market-stream raw data.
        
        This is a fallback when Flink SQL is not running.
        Uses a simplified VKS formula based on engagement metrics.
        
        VKS = (views * 0.1 + likes * 2 + comments * 3 + shares * 4) / 1000
        Normalized to 0-100 range.
        """
        try:
            views = data.get("views", 0) or 0
            likes = data.get("likes", 0) or 0
            comments = data.get("comments", 0) or 0
            shares = data.get("shares", 0) or 0
            saves = data.get("saves", 0) or 0
            
            # 提取平台信息
            platform = data.get("platform", "unknown")
            
            # 提取作者信息
            author = data.get("author", {})
            if isinstance(author, dict):
                author_name = author.get("username") or author.get("nickname") or "unknown"
            else:
                author_name = str(author) if author else "unknown"
            
            # 提取内容描述
            description = data.get("description", "")
            if not description:
                content = data.get("content", {})
                if isinstance(content, dict):
                    description = content.get("title", "")
            
            # 提取帖子 ID
            post_id = data.get("post_id") or data.get("id") or "unknown"
            
            # Debug: 打印原始数据的关键字段
            logger.debug(f"📊 Raw metrics - platform: {platform}, views: {views}, likes: {likes}, comments: {comments}, shares: {shares}, saves: {saves}")
            
            # Simplified VKS calculation
            raw_score = (
                views * 0.001 +      # Views have low weight
                likes * 0.5 +        # Likes moderate weight
                comments * 1.0 +     # Comments high weight (engagement)
                shares * 2.0 +       # Shares highest weight (virality)
                saves * 0.8          # Saves moderate-high weight
            )
            
            # Normalize to 0-100 using logarithmic scale
            import math
            if raw_score > 0:
                vks_score = min(100, max(0, 10 * math.log10(raw_score + 1)))
            else:
                vks_score = 0
            
            hashtag = data.get("hashtag", data.get("tag", "unknown"))
            if hashtag and not hashtag.startswith("#"):
                hashtag = f"#{hashtag}"
            
            return {
                "hashtag": hashtag,
                "vks_score": round(vks_score, 2),
                "platform": platform,
                "post_id": post_id,
                "author": author_name,
                "description": description[:200] if description else "",  # 截断过长的描述
                "timestamp": datetime.utcnow().isoformat(),
                "source": "backend_calculated",
                "metrics": {
                    "views": views,
                    "likes": likes,
                    "comments": comments,
                    "shares": shares,
                    "saves": saves
                }
            }
        except Exception as e:
            logger.error(f"Error calculating VKS from market data: {e}")
            return {
                "hashtag": "unknown",
                "vks_score": 0.0,
                "platform": "unknown",
                "error": str(e)
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
                        
                        # 1. 发送原始 trend_update 事件
                        logger.info(f"📤 Broadcasting trend_update to {self.client_count} clients")
                        self.broadcast("trend_update", raw_data, topic)
                        
                        # 2. 同时计算 VKS 并发送 vks_update 事件 (Flink fallback)
                        if raw_data.get("type") == "social_post":
                            vks_data = self._calculate_vks_from_market_data(raw_data)
                            logger.info(f"📤 Broadcasting vks_update (calculated) to {self.client_count} clients: {vks_data}")
                            self.broadcast("vks_update", vks_data, "vks-scores")
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
