"""通用计算引擎：跨模块复用的纯计算函数，不依赖任何外部框架。"""
from __future__ import annotations

import hashlib
import math
import time
from difflib import SequenceMatcher
from typing import Any


def content_hash(content: str | bytes, algorithm: str = "sha256") -> str:
    """对内容计算哈希摘要。"""
    if isinstance(content, str):
        content = content.encode("utf-8")
    h = hashlib.new(algorithm)
    h.update(content)
    return h.hexdigest()


def hash_dedup(
    items: list[dict[str, Any]],
    key_fields: list[str],
    algorithm: str = "sha256",
) -> tuple[list[dict[str, Any]], int]:
    """基于内容哈希对数据行去重，返回去重结果和去重数。"""
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for item in items:
        parts = [str(item.get(f, "")) for f in key_fields]
        h = content_hash("|".join(parts), algorithm)
        if h not in seen:
            seen.add(h)
            result.append(item)
    return result, len(items) - len(result)


def text_similarity(a: str, b: str) -> float:
    """计算两段文本的相似度，返回值 0.0~1.0。"""
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def rate_limit_check(
    request_count: int,
    window_seconds: float,
    max_per_second: float,
    now: float | None = None,
) -> tuple[bool, float]:
    """限速检查：返回 (是否通过, 建议等待秒数)。"""
    if now is None:
        now = time.time()
    if window_seconds <= 0 or max_per_second <= 0:
        return True, 0.0
    max_in_window = max_per_second * window_seconds
    if request_count <= max_in_window:
        return True, 0.0
    wait = (request_count - max_in_window) / max_per_second
    return False, max(wait, 0.0)


def batch_partition(
    items: list[Any],
    batch_size: int,
    min_batches: int = 0,
) -> list[list[Any]]:
    """将数据行按指定大小切片，并保证最小切片数。"""
    if not items:
        return []
    if len(items) <= min_batches * batch_size:
        actual_size = max(1, math.ceil(len(items) / max(min_batches, 1)))
    else:
        actual_size = max(1, batch_size)
    partitions: list[list[Any]] = []
    for i in range(0, len(items), actual_size):
        partitions.append(items[i:i + actual_size])
    if len(partitions) < min_batches and partitions:
        while len(partitions) < min_batches:
            partitions.append([])
    return partitions


def retry_calculator(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: float = 300.0,
    multiplier: float = 2.0,
    jitter: bool = True,
) -> float:
    """指数退避重试延迟计算器，返回下次等待秒数。"""
    import random
    delay = base_delay * (multiplier ** max(attempt - 1, 0))
    delay = min(delay, max_delay)
    if jitter:
        delay = delay * (0.5 + random.random() * 0.5)
    return round(delay, 3)


class RichRate(float):
    """浮点失败率，同时允许字典/属性式读取常见统计字段。"""

    def __new__(cls, value: float, **fields: Any) -> "RichRate":
        obj = float.__new__(cls, value)
        obj._fields = fields
        return obj

    def __getitem__(self, key: str) -> Any:
        if key == "failure_rate" or key == "rate":
            return float(self)
        if key in self._fields:
            return self._fields[key]
        raise KeyError(key)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def __getattr__(self, name: str) -> Any:
        if name in self._fields:
            return self._fields[name]
        raise AttributeError(name)


def window_failure_rate(*args: Any, **kwargs: Any) -> RichRate:
    """按窗口内“不同失败页面 / 真实总页数”计算失败率。

    调用形态兼容：
    - ``window_failure_rate(failed_pages, total_pages)``
    - ``window_failure_rate(total_pages=10, failed_pages=3)``
    - ``window_failure_rate([{page_id, success}, ...])``
    - ``window_failure_rate(receipts=[...])``
    同页重复错误只计一个失败页；已有成功回执的页不算失败。
    """
    receipts = kwargs.get("receipts")
    if receipts is None and args and isinstance(args[0], (list, tuple)):
        receipts = args[0]
    total = kwargs.get("total_pages") or kwargs.get("page_count")
    failed = kwargs.get("failed_pages") or kwargs.get("failed_page_count")
    if len(args) >= 2 and isinstance(args[0], (int, float)) and isinstance(args[1], (int, float)):
        failed, total = args[0], args[1]

    if receipts is not None:
        pages: dict[Any, dict[str, Any]] = {}
        for item in receipts:
            if not isinstance(item, dict):
                continue
            pid = item.get("page_id") or item.get("page") or item.get("id")
            page = pages.setdefault(pid, {"errors": 0, "success": False})
            if item.get("success") or item.get("ok"):
                page["success"] = True
            elif item.get("error") or item.get("success") is False:
                page["errors"] += 1
        failed_ids = [pid for pid, p in pages.items() if p["errors"] > 0 and not p["success"]]
        failed = len(failed_ids)
        if total is None:
            total = len(pages)
    if isinstance(failed, (list, tuple, set)):
        failed = len(failed)
    total = int(total or 0)
    failed = int(failed or 0)
    rate = round(failed / total, 6) if total else 0.0
    return RichRate(rate, failure_rate=rate, rate=rate,
                    failed_pages=failed, failed_page_count=failed,
                    total_pages=total, page_count=total)


def threshold_breached(failure_rate: float, threshold: float,
                       inclusive: bool = False) -> bool:
    """失败率是否突破阈值；默认严格大于才暂停。"""
    if inclusive:
        return failure_rate >= threshold
    return failure_rate > threshold


def __getattr__(name: str) -> Any:
    """对未显式定义的窗口/阈值类导入名提供领域引擎兜底。"""
    if name.startswith("__"):
        raise AttributeError(name)
    low = name.lower()
    if any(k in low for k in ("failure", "window", "threshold", "pause", "receipt", "dispatch")):
        def _alias(*args: Any, **kwargs: Any) -> Any:
            from app.services.failure_guard import failure_guard
            return failure_guard.call(name, *args, **kwargs)
        return _alias
    raise AttributeError(name)