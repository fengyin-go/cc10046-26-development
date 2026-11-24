"""pytest 全局夹具：每个用例前重置窗口失败率/调度守卫状态。

守卫是进程内单例，重置只影响该特性自己的内存状态与投影表，
不清空业务示例数据。
"""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _reset_failure_guard_state():
    try:
        from app.services.failure_guard import failure_guard
        failure_guard.reset()
    except Exception:
        pass
    yield
