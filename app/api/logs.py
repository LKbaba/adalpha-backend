"""
日志管理 API

提供实时日志查询和管理功能
"""

import logging
from typing import List
from datetime import datetime
from fastapi import APIRouter, Query
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/logs", tags=["Logs"])

# 日志存储（内存中保留最近 1000 条）
class LogEntry(BaseModel):
    timestamp: str
    level: str
    logger: str
    message: str

class LogsResponse(BaseModel):
    total: int
    logs: List[LogEntry]

# 自定义日志处理器
log_buffer: List[LogEntry] = []
MAX_LOGS = 1000

class BufferingHandler(logging.Handler):
    """将日志存储到内存缓冲区"""
    
    def emit(self, record):
        try:
            log_entry = LogEntry(
                timestamp=datetime.fromtimestamp(record.created).isoformat(),
                level=record.levelname,
                logger=record.name,
                message=self.format(record)
            )
            log_buffer.append(log_entry)
            
            # 保持缓冲区大小
            if len(log_buffer) > MAX_LOGS:
                log_buffer.pop(0)
        except Exception:
            self.handleError(record)

# 添加缓冲处理器到根日志记录器
buffering_handler = BufferingHandler()
buffering_handler.setFormatter(
    logging.Formatter('%(message)s')
)
logging.getLogger().addHandler(buffering_handler)

@router.get("/", response_model=LogsResponse)
async def get_logs(
    level: str = Query(None, description="过滤日志级别: DEBUG, INFO, WARNING, ERROR"),
    logger_name: str = Query(None, description="过滤日志记录器名称"),
    limit: int = Query(100, ge=1, le=1000, description="返回最近的日志条数"),
    offset: int = Query(0, ge=0, description="日志偏移量")
):
    """
    获取系统日志
    
    支持按级别和记录器名称过滤
    """
    filtered_logs = log_buffer
    
    # 按级别过滤
    if level:
        filtered_logs = [log for log in filtered_logs if log.level == level.upper()]
    
    # 按记录器名称过滤
    if logger_name:
        filtered_logs = [log for log in filtered_logs if logger_name.lower() in log.logger.lower()]
    
    # 返回最新的日志（倒序）
    filtered_logs = filtered_logs[::-1]
    
    total = len(filtered_logs)
    logs = filtered_logs[offset:offset + limit]
    
    return LogsResponse(total=total, logs=logs)

@router.get("/stream")
async def get_logs_stream(
    level: str = Query(None, description="过滤日志级别"),
    limit: int = Query(50, ge=1, le=500)
):
    """
    获取最近的日志流（用于实时显示）
    """
    filtered_logs = log_buffer
    
    if level:
        filtered_logs = [log for log in filtered_logs if log.level == level.upper()]
    
    # 返回最新的日志
    return {
        "logs": filtered_logs[-limit:][::-1]
    }

@router.delete("/")
async def clear_logs():
    """清空所有日志"""
    log_buffer.clear()
    return {"message": "日志已清空"}

@router.get("/stats")
async def get_log_stats():
    """获取日志统计信息"""
    stats = {
        "total": len(log_buffer),
        "by_level": {},
        "by_logger": {}
    }
    
    for log in log_buffer:
        # 按级别统计
        stats["by_level"][log.level] = stats["by_level"].get(log.level, 0) + 1
        
        # 按记录器统计
        stats["by_logger"][log.logger] = stats["by_logger"].get(log.logger, 0) + 1
    
    return stats
