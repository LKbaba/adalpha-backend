"""
Configuration management using Pydantic Settings.
Loads environment variables from .env file for secure credential management.
"""

from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.
    All sensitive credentials are stored in .env file (not committed to git).
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

    # === Service Configuration ===
    APP_NAME: str = Field(
        default="ADALPHA Backend",
        description="Application name"
    )
    DEBUG: bool = Field(
        default=False,
        description="Debug mode flag"
    )

    # === Confluent Kafka Configuration ===
    KAFKA_BOOTSTRAP_SERVERS: str = Field(
        default="",
        description="Confluent Cloud bootstrap servers"
    )
    KAFKA_API_KEY: str = Field(
        default="",
        description="Confluent Cloud API Key"
    )
    KAFKA_API_SECRET: str = Field(
        default="",
        description="Confluent Cloud API Secret"
    )

    # === Kafka Topics ===
    KAFKA_TOPIC_MARKET_STREAM: str = Field(
        default="market-stream",
        description="Topic for raw social media data"
    )
    KAFKA_TOPIC_VKS_SCORES: str = Field(
        default="vks-scores",
        description="Topic for calculated VKS scores"
    )
    KAFKA_TOPIC_PORTFOLIO: str = Field(
        default="portfolio-stream",
        description="Topic for portfolio data"
    )

    # === TikHub API Configuration ===
    TIKHUB_API_KEY: str = Field(
        default="",
        description="TikHub API Key for social media data"
    )
    TIKHUB_BASE_URL: str = Field(
        default="https://api.tikhub.io",
        description="TikHub API base URL"
    )

    # === Gemini API Configuration ===
    GEMINI_API_KEY: str = Field(
        default="",
        description="Google Gemini API Key"
    )

    # === Redis Configuration (Optional) ===
    REDIS_URL: str = Field(
        default="redis://localhost:6379",
        description="Redis connection URL"
    )

    def get_kafka_config(self) -> dict:
        """
        Get Kafka client configuration dictionary.
        Used by both Producer and Consumer.
        """
        return {
            "bootstrap.servers": self.KAFKA_BOOTSTRAP_SERVERS,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": self.KAFKA_API_KEY,
            "sasl.password": self.KAFKA_API_SECRET,
        }

    def get_producer_config(self) -> dict:
        """Get Kafka producer specific configuration."""
        config = self.get_kafka_config()
        config.update({
            "acks": "all",  # Wait for all replicas to acknowledge
            "retries": 3,
            "retry.backoff.ms": 1000,
        })
        return config

    def get_consumer_config(self, group_id: str = "adalpha-backend") -> dict:
        """Get Kafka consumer specific configuration."""
        config = self.get_kafka_config()
        config.update({
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        })
        return config


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
