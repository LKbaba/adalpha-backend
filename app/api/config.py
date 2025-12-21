"""
配置管理 API
提供系统配置的 CRUD 接口
"""

from typing import Any, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services.config_store import (
    get_config, set_config, get_all_configs, get_spider_config
)

router = APIRouter(prefix="/api/config", tags=["Configuration"])


class ConfigUpdateRequest(BaseModel):
    """配置更新请求"""
    value: Any
    description: Optional[str] = None


class ConfigResponse(BaseModel):
    """配置响应"""
    success: bool
    data: Any = None
    message: str = ""


@router.get("", response_model=ConfigResponse)
async def list_all_configs():
    """获取所有配置项"""
    configs = get_all_configs()
    return ConfigResponse(success=True, data=configs)


@router.get("/spider", response_model=ConfigResponse)
async def get_spider_config_api():
    """
    获取爬虫配置（供爬虫服务调用）
    返回格式与爬虫 config.js 兼容
    """
    config = get_spider_config()
    return ConfigResponse(success=True, data=config)


@router.get("/{key}", response_model=ConfigResponse)
async def get_config_by_key(key: str):
    """获取单个配置项"""
    value = get_config(key)
    if value is None:
        raise HTTPException(status_code=404, detail=f"Config key '{key}' not found")
    return ConfigResponse(success=True, data={"key": key, "value": value})


@router.put("/{key}", response_model=ConfigResponse)
async def update_config(key: str, request: ConfigUpdateRequest):
    """更新配置项"""
    set_config(key, request.value, request.description)
    return ConfigResponse(success=True, message=f"Config '{key}' updated")


@router.post("/batch", response_model=ConfigResponse)
async def batch_update_configs(configs: dict[str, Any]):
    """批量更新配置"""
    for key, value in configs.items():
        set_config(key, value)
    return ConfigResponse(success=True, message=f"Updated {len(configs)} configs")
