"""命名别名：部分调用方可能使用 failure_window / FailureWindowService。"""
from __future__ import annotations

from app.services.failure_guard import FailureGuard, failure_guard
from app.services.window_failure import WindowFailureRateService


class FailureWindowService(WindowFailureRateService):
    MODULE = "window_failure_rate"


__all__ = ["FailureWindowService", "WindowFailureRateService",
           "FailureGuard", "failure_guard"]
