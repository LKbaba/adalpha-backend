"""
Demo Data Generator Service for ADALPHA.

Generates realistic mock social media trend data for demonstrations
when TikHub API is unavailable or rate-limited.

Supports multiple scenarios:
- viral_spike: Hashtag goes from 0 to millions in minutes
- steady_growth: Consistent 10-20% growth per interval
- declining: Peak followed by decay
- multi_trend: Multiple hashtags competing for attention
- oscillating: Wave-like pattern with peaks and troughs
"""

import asyncio
import logging
import random
import math
import uuid
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Callable
from dataclasses import dataclass, field
from enum import Enum

from app.models.schemas import TrendData, MarketStreamMessage
from app.services.kafka_client import kafka_client

logger = logging.getLogger(__name__)


class DemoScenario(str, Enum):
    """Available demo scenarios."""
    VIRAL_SPIKE = "viral_spike"
    STEADY_GROWTH = "steady_growth"
    DECLINING = "declining"
    MULTI_TREND = "multi_trend"
    OSCILLATING = "oscillating"


@dataclass
class TrendState:
    """State for a single trending hashtag."""
    hashtag: str
    platform: str = "tiktok"
    views: int = 0
    likes: int = 0
    comments: int = 0
    shares: int = 0
    velocity: float = 0.0
    created_at: datetime = field(default_factory=datetime.utcnow)
    peak_reached: bool = False


# Realistic hashtag pools for demo
DEMO_HASHTAGS = {
    "ai": ["#AIArt", "#ChatGPT", "#AIGenerated", "#MidJourney", "#StableDiffusion", "#AIVideo"],
    "crypto": ["#Bitcoin", "#Ethereum", "#CryptoNews", "#Web3", "#NFT", "#DeFi"],
    "viral": ["#Viral", "#ForYou", "#FYP", "#Trending", "#GoViral", "#TikTokFamous"],
    "gaming": ["#Gaming", "#Gamer", "#Twitch", "#Esports", "#PS5", "#Xbox"],
    "music": ["#NewMusic", "#SpotifyWrapped", "#Remix", "#MusicVideo", "#Concert", "#Festival"],
    "meme": ["#Meme", "#Funny", "#Comedy", "#Relatable", "#GenZ", "#Millennial"],
}


class DemoDataGenerator:
    """
    Generates realistic mock data for demonstration purposes.
    Publishes data to Kafka market-stream topic.
    """

    def __init__(self):
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._current_scenario: DemoScenario = DemoScenario.VIRAL_SPIKE
        self._interval: float = 3.0  # seconds between data points
        self._trends: Dict[str, TrendState] = {}
        self._iteration: int = 0
        self._callbacks: List[Callable] = []

    @property
    def is_running(self) -> bool:
        """Check if generator is currently running."""
        return self._running

    @property
    def current_scenario(self) -> str:
        """Get current scenario name."""
        return self._current_scenario.value

    @property
    def stats(self) -> dict:
        """Get current generator statistics."""
        return {
            "running": self._running,
            "scenario": self._current_scenario.value,
            "interval": self._interval,
            "iteration": self._iteration,
            "active_trends": len(self._trends),
            "trends": {
                name: {
                    "hashtag": state.hashtag,
                    "views": state.views,
                    "likes": state.likes,
                    "vks_estimate": self._estimate_vks(state)
                }
                for name, state in self._trends.items()
            }
        }

    def _estimate_vks(self, state: TrendState) -> float:
        """Estimate VKS score based on current metrics."""
        if state.views == 0:
            return 0.0

        engagement_rate = (state.likes + state.comments + state.shares) / state.views
        velocity_factor = min(state.velocity / 10000, 1.0)

        # Simplified VKS estimation
        vks = (velocity_factor * 0.4 + engagement_rate * 10 * 0.6) * 100
        return min(max(vks, 0), 100)

    async def start(
        self,
        scenario: str = "viral_spike",
        interval: float = 3.0
    ) -> bool:
        """
        Start generating demo data.

        Args:
            scenario: Demo scenario to run
            interval: Seconds between data points

        Returns:
            bool: True if started successfully
        """
        if self._running:
            logger.warning("Demo generator already running")
            return False

        try:
            self._current_scenario = DemoScenario(scenario)
        except ValueError:
            logger.error(f"Invalid scenario: {scenario}")
            return False

        self._interval = interval
        self._running = True
        self._iteration = 0
        self._initialize_scenario()

        self._task = asyncio.create_task(self._generation_loop())
        logger.info(f"Demo generator started: scenario={scenario}, interval={interval}s")
        return True

    async def stop(self) -> bool:
        """Stop the demo generator."""
        if not self._running:
            return False

        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        self._trends.clear()
        logger.info("Demo generator stopped")
        return True

    def set_scenario(self, scenario: str) -> bool:
        """
        Switch to a different scenario.

        Args:
            scenario: New scenario name

        Returns:
            bool: True if switched successfully
        """
        try:
            self._current_scenario = DemoScenario(scenario)
            self._initialize_scenario()
            self._iteration = 0
            logger.info(f"Switched to scenario: {scenario}")
            return True
        except ValueError:
            logger.error(f"Invalid scenario: {scenario}")
            return False

    def _initialize_scenario(self):
        """Initialize trends based on current scenario."""
        self._trends.clear()

        if self._current_scenario == DemoScenario.VIRAL_SPIKE:
            # Single hashtag going viral
            hashtag = random.choice(DEMO_HASHTAGS["ai"])
            self._trends["main"] = TrendState(
                hashtag=hashtag,
                views=random.randint(1000, 5000),
                likes=random.randint(100, 500),
                comments=random.randint(10, 50),
                shares=random.randint(5, 20)
            )

        elif self._current_scenario == DemoScenario.STEADY_GROWTH:
            # Multiple hashtags with steady growth
            for i, category in enumerate(["ai", "gaming", "music"]):
                hashtag = random.choice(DEMO_HASHTAGS[category])
                self._trends[f"trend_{i}"] = TrendState(
                    hashtag=hashtag,
                    views=random.randint(50000, 200000),
                    likes=random.randint(5000, 20000),
                    comments=random.randint(500, 2000),
                    shares=random.randint(200, 800)
                )

        elif self._current_scenario == DemoScenario.DECLINING:
            # Hashtag past its peak
            hashtag = random.choice(DEMO_HASHTAGS["viral"])
            self._trends["main"] = TrendState(
                hashtag=hashtag,
                views=random.randint(5000000, 10000000),
                likes=random.randint(500000, 1000000),
                comments=random.randint(50000, 100000),
                shares=random.randint(20000, 50000),
                peak_reached=True
            )

        elif self._current_scenario == DemoScenario.MULTI_TREND:
            # Multiple competing trends
            for i, category in enumerate(["ai", "crypto", "meme", "gaming"]):
                hashtag = random.choice(DEMO_HASHTAGS[category])
                self._trends[f"competitor_{i}"] = TrendState(
                    hashtag=hashtag,
                    views=random.randint(100000, 500000),
                    likes=random.randint(10000, 50000),
                    comments=random.randint(1000, 5000),
                    shares=random.randint(500, 2000)
                )

        elif self._current_scenario == DemoScenario.OSCILLATING:
            # Wave pattern
            hashtag = random.choice(DEMO_HASHTAGS["music"])
            self._trends["main"] = TrendState(
                hashtag=hashtag,
                views=random.randint(500000, 1000000),
                likes=random.randint(50000, 100000),
                comments=random.randint(5000, 10000),
                shares=random.randint(2000, 5000)
            )

    async def _generation_loop(self):
        """Main generation loop."""
        while self._running:
            try:
                # Generate data for all active trends
                for name, state in self._trends.items():
                    self._update_trend(name, state)
                    await self._publish_trend(state)

                self._iteration += 1
                await asyncio.sleep(self._interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in generation loop: {e}")
                await asyncio.sleep(1)

    def _update_trend(self, name: str, state: TrendState):
        """
        Update trend metrics based on scenario.

        Args:
            name: Trend identifier
            state: Current trend state
        """
        previous_views = state.views

        # Max limits to prevent JavaScript integer overflow
        MAX_VIEWS = 500_000_000  # 500 million (realistic viral ceiling)
        MAX_LIKES = 50_000_000
        MAX_COMMENTS = 10_000_000
        MAX_SHARES = 5_000_000

        if self._current_scenario == DemoScenario.VIRAL_SPIKE:
            # Exponential growth with randomness, but capped
            if state.views < MAX_VIEWS:
                growth_rate = 1.3 + random.uniform(0.1, 0.4)  # Reduced from 1.5-2.3
                state.views = min(int(state.views * growth_rate), MAX_VIEWS)
                state.likes = min(int(state.likes * (growth_rate * 0.8)), MAX_LIKES)
                state.comments = min(int(state.comments * (growth_rate * 0.6)), MAX_COMMENTS)
                state.shares = min(int(state.shares * (growth_rate * 0.7)), MAX_SHARES)
            else:
                # At ceiling, add small fluctuations
                fluctuation = random.uniform(0.98, 1.02)
                state.views = int(state.views * fluctuation)
                state.likes = int(state.likes * fluctuation)
                state.comments = int(state.comments * fluctuation)
                state.shares = int(state.shares * fluctuation)

        elif self._current_scenario == DemoScenario.STEADY_GROWTH:
            # Linear growth with noise
            growth = 1.1 + random.uniform(0.05, 0.15)
            state.views = int(state.views * growth)
            state.likes = int(state.likes * growth)
            state.comments = int(state.comments * growth * 0.9)
            state.shares = int(state.shares * growth * 0.8)

        elif self._current_scenario == DemoScenario.DECLINING:
            # Decay with occasional small bumps
            decay = 0.85 + random.uniform(-0.05, 0.1)
            state.views = max(10000, int(state.views * decay))
            state.likes = max(1000, int(state.likes * decay))
            state.comments = max(100, int(state.comments * decay))
            state.shares = max(50, int(state.shares * decay))

        elif self._current_scenario == DemoScenario.MULTI_TREND:
            # Variable growth - some up, some down
            if random.random() > 0.3:
                growth = 1.1 + random.uniform(0.1, 0.3)
            else:
                growth = 0.9 + random.uniform(-0.1, 0.1)
            state.views = int(state.views * growth)
            state.likes = int(state.likes * growth)
            state.comments = int(state.comments * growth * 0.9)
            state.shares = int(state.shares * growth * 0.8)

        elif self._current_scenario == DemoScenario.OSCILLATING:
            # Sine wave pattern
            phase = self._iteration * 0.5
            oscillation = math.sin(phase) * 0.3 + 1.0
            state.views = int(state.views * oscillation)
            state.likes = int(state.likes * oscillation)
            state.comments = int(state.comments * oscillation * 0.9)
            state.shares = int(state.shares * oscillation * 0.8)

        # Calculate velocity
        state.velocity = state.views - previous_views

    async def _publish_trend(self, state: TrendState):
        """
        Publish trend data to Kafka.

        Args:
            state: Trend state to publish
        """
        trend_data = TrendData(
            hashtag=state.hashtag,
            platform=state.platform,
            views=state.views,
            likes=state.likes,
            comments=state.comments,
            shares=state.shares,
            timestamp=datetime.utcnow(),
            metadata={
                "demo": True,
                "scenario": self._current_scenario.value,
                "velocity": state.velocity,
                "iteration": self._iteration
            }
        )

        message = MarketStreamMessage(
            event_id=str(uuid.uuid4()),
            event_type="trend_update",
            data=trend_data,
            source="demo_generator",
            ingested_at=datetime.utcnow()
        )

        # Publish to Kafka
        success = kafka_client.produce_message(
            topic="market-stream",
            value=message.model_dump(mode="json"),
            key=state.hashtag
        )

        if success:
            kafka_client.flush_producer(timeout=1.0)
            logger.debug(f"Published demo data: {state.hashtag} views={state.views}")
        else:
            logger.warning(f"Failed to publish demo data for {state.hashtag}")

        # Call registered callbacks
        for callback in self._callbacks:
            try:
                callback(message)
            except Exception as e:
                logger.error(f"Callback error: {e}")

    def register_callback(self, callback: Callable):
        """Register a callback for new data points."""
        self._callbacks.append(callback)

    def unregister_callback(self, callback: Callable):
        """Unregister a callback."""
        if callback in self._callbacks:
            self._callbacks.remove(callback)

    @staticmethod
    def get_available_scenarios() -> List[dict]:
        """Get list of available scenarios with descriptions."""
        return [
            {
                "name": "viral_spike",
                "title": "Viral Explosion",
                "description": "Single hashtag explodes from thousands to millions of views",
                "recommended_interval": 2.0
            },
            {
                "name": "steady_growth",
                "title": "Steady Growth",
                "description": "Multiple trends with consistent 10-20% growth",
                "recommended_interval": 3.0
            },
            {
                "name": "declining",
                "title": "Declining Trend",
                "description": "Viral content past its peak, showing decay pattern",
                "recommended_interval": 3.0
            },
            {
                "name": "multi_trend",
                "title": "Competition",
                "description": "Multiple hashtags competing for attention",
                "recommended_interval": 2.5
            },
            {
                "name": "oscillating",
                "title": "Wave Pattern",
                "description": "Cyclical engagement with peaks and troughs",
                "recommended_interval": 2.0
            }
        ]


# Global singleton instance
demo_generator = DemoDataGenerator()
