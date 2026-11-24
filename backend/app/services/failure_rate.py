"""命名别名：failure_rate 模块名。"""
from __future__ import annotations

from app.services.failure_window import FailureWindowService
from app.services.failure_guard import failure_guard, FailureGuard


class FailureRateService(FailureWindowService):
    MODULE = "window_failure_rate"


__all__ = ["FailureRateService", "FailureWindowService", "failure_guard", "FailureGuard"]
