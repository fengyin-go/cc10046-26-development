"""「错误记录」命名别名：需求文案称“错误记录”，既有模块为 error_logger。"""
from __future__ import annotations

from app.services.error_logger import ErrorLoggerService
from app.services.failure_guard import failure_guard, FailureGuard

__all__ = ["ErrorRecordService", "failure_guard", "FailureGuard"]


class ErrorRecordService(ErrorLoggerService):
    """与 ErrorLoggerService 行为完全一致，仅模块/实体命名不同。"""

    MODULE = "error_record"
    ENTITY = "错误记录"
