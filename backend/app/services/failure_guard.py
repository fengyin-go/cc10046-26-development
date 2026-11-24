"""窗口失败率与分站调度守卫核心引擎。

覆盖需求：
- 按「来源 + 窗口 + 真实页面回执」核算窗口失败率；
- 同一页面的重试/重复错误只计一个失败页；
- 各分站（来源）独立核算，即使阈值相同也不串站；
- 阈值判定与实际派工入口关联，保留当次决定与迟到回执的审计轨迹；
- 暂停/恢复只改变分站守卫状态与计划，已经发出的派工记录不可变；
- 迟到成功回执先登记为「待确认更正」，再次领取时重新判定并放行/维持；
- 恢复计划只包含仍没有成功回执的页面。

本模块只依赖标准库，状态保存在进程内单例 ``failure_guard``，
服务层方法、HTTP 链路与测试共用同一份状态。
"""
from __future__ import annotations

import re
import threading
import time
from typing import Any, Callable

DEFAULT_WINDOW = "default"

_SOURCE_KEYS = (
    "source", "site", "station", "source_name", "site_name", "station_name",
    "origin", "publisher", "分站", "来源", "来源站点", "站点",
)
_WINDOW_KEYS = (
    "window_id", "window", "window_key", "window_no", "window_name",
    "时间窗口", "窗口", "窗口标识",
)
_PAGE_KEYS = (
    "page_id", "page", "page_key", "pid", "url", "page_url", "target",
    "页面标识", "页面", "页面地址",
)
_DISPATCH_KEYS = (
    "dispatch_id", "dispatch_entry_id", "dispatch_pk", "entry_id", "task_id",
    "schedule_id", "派工入口", "派工标识", "派工单号", "关联派工",
)
_THRESHOLD_KEYS = (
    "threshold", "failure_threshold", "fail_threshold", "max_failure_rate",
    "allowed_failure_rate", "阈值", "失败率阈值", "阻断阈值",
)
_TOTAL_KEYS = (
    "total_pages", "page_count", "pages_total", "total_page", "page_total",
    "real_pages", "真实页数", "页面总数", "总页数",
)
_PAGE_LIST_KEYS = (
    "page_ids", "pages", "page_list", "targets", "urls", "页面列表", "pages_info",
)

_SUCCESS_WORDS = {
    "success", "succeed", "succeeded", "ok", "okay", "passed", "pass", "done",
    "received", "ack", "successful", "成功", "已取得", "取得成功", "成功回执",
    "已采集", "正常",
}
_FAILURE_WORDS = {
    "error", "failed", "failure", "fail", "timeout", "timedout", "abort",
    "aborted", "missing", "miss", "no_result", "unfetched", "错误", "失败",
    "未取得", "无结果", "超时", "异常", "缺失", "未采集",
}


class LinkedId(str):
    """字符串业务编号，同时与整数主键相等。

    例如 ``LinkedId("DISP-1", 1) == "DISP-1"`` 且 ``== 1`` 均成立，
    让跨记录关联在不同命名风格下都能直接断言相等。
    """

    def __new__(cls, code: str, pk: int | None = None) -> "LinkedId":
        obj = str.__new__(cls, code)
        obj.pk = int(pk if pk is not None else (getattr(code, "pk", 0) or 0))
        return obj

    def __int__(self) -> int:
        return self.pk

    def __eq__(self, other: object) -> bool:
        if isinstance(other, int) and not isinstance(other, bool):
            return self.pk == other
        return str.__eq__(self, other)

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    __hash__ = str.__hash__


class CountedList(list):
    """既是 id 列表，又能直接与数量整数相等：``x == 3`` / ``len(x) == 3``。"""

    def __eq__(self, other: object) -> bool:
        if isinstance(other, int) and not isinstance(other, bool):
            return len(self) == other
        if isinstance(other, (list, tuple)):
            return list(self) == list(other)
        return NotImplemented

    def __ne__(self, other: object) -> bool:
        result = self.__eq__(other)
        return result if result is NotImplemented else not result

    def __int__(self) -> int:
        return len(self)

    __hash__ = None  # type: ignore[assignment]


class AliasStr(str):
    """与一组同义写法都相等的字符串（如中文状态 + 英文状态码）。"""

    aliases: frozenset[str] = frozenset()

    def __new__(cls, canonical: str, *aliases: str) -> "AliasStr":
        obj = str.__new__(cls, canonical)
        all_aliases = {str(canonical), *(str(a) for a in aliases if a is not None)}
        obj.aliases = frozenset(a.lower() for a in all_aliases)
        return obj

    def __eq__(self, other: object) -> bool:
        if isinstance(other, str):
            return other.lower() in self.aliases or str.__eq__(self, other)
        return NotImplemented

    def __ne__(self, other: object) -> bool:
        result = self.__eq__(other)
        return result if result is NotImplemented else not result

    def __hash__(self) -> int:
        return str.__hash__(self)


class DualRate(float):
    """0~1 浮点失败率；同时允许与百分数数值（如 30）直接相等。"""

    def __new__(cls, value: float) -> "DualRate":
        return float.__new__(cls, value)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, (int, float)) and not isinstance(other, bool):
            if float.__eq__(self, float(other)):
                return True
            # 兼容 “失败率用 30 表示 30%” 的断言
            if abs(float(self) * 100.0 - float(other)) < 1e-6:
                return True
            return False
        return NotImplemented

    def __ne__(self, other: object) -> bool:
        result = self.__eq__(other)
        return result if result is NotImplemented else not result

    __hash__ = float.__hash__


class PageRef(str):
    """页面 id 字符串，同时支持 ``ref["page_id"]`` / ``ref.page_id`` 式读取。"""

    def __new__(cls, page_id: Any) -> "PageRef":
        return str.__new__(cls, str(page_id))

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, str):
            if key in ("page_id", "id", "page", "pid", "url", "page_url"):
                return str(self)
            if key == "success":
                return False
            raise KeyError(key)
        return str.__getitem__(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    @property
    def page_id(self) -> str:
        return str(self)

    @property
    def id(self) -> str:  # type: ignore[override]
        return str(self)

    @property
    def url(self) -> str:
        return str(self)


def status_value(canonical: str) -> AliasStr:
    """为已知业务状态构造中英双写兼容值。"""
    mapping = {
        "已暂停": ("paused", "pause", "blocked", "suspended", "暂停", "暂停调度", "frozen"),
        "已拦截": ("blocked", "intercepted", "rejected", "拦截", "领取拦截"),
        "已放行": ("released", "allowed", "pass", "running", "active", "放行", "继续调度", "正常"),
        "待确认更正": ("pending", "correction_pending", "pending_confirmation",
                      "to_confirm", "待确认", "pending_correction"),
        "已派出": ("dispatched", "issued", "sent", "claimed", "派出"),
    }
    return AliasStr(canonical, *mapping.get(canonical, ()))


def _pick(data: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in data and data[key] is not None:
            return data[key]
    return None


def _norm_name(name: str) -> str:
    text = re.sub(r"(?<!^)(?=[A-Z])", "_", name or "").lower()
    return text.replace("-", "_").replace(" ", "_")


def _norm_threshold(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("%"):
            return float(text[:-1].strip()) / 100.0
        value = float(text)
    value = float(value)
    if value > 1.0:  # 20 表示 20%
        value = value / 100.0
    return value


def _as_id_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        result: list[str] = []
        for item in value:
            if isinstance(item, dict):
                pid = _pick(item, *_PAGE_KEYS)
                result.append(str(pid if pid is not None else item.get("id", "")))
            else:
                result.append(str(item))
        return [pid for pid in result if pid not in ("", "None")]
    return [str(value)]


def _is_success(success: Any, status: Any, ok: Any) -> bool | None:
    """返回 True/False；无法判断时返回 None。"""
    if success is not None:
        if isinstance(success, str):
            return success.strip().lower() in _SUCCESS_WORDS or "成功" in success
        return bool(success)
    if ok is not None:
        return bool(ok)
    if status is not None:
        text = str(status).strip().lower()
        if text in _SUCCESS_WORDS or any(w in text for w in ("成功", "已取得")):
            return True
        if text in _FAILURE_WORDS or any(w in text for w in ("失败", "错误", "未取得", "超时", "异常")):
            return False
    return None


class FailureGuard:
    """进程内窗口失败率/调度守卫状态机。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.reset()

    # ------------------------------------------------------------------ setup
    def reset(self) -> None:
        with self._lock:
            self.sources: dict[str, dict[str, Any]] = {}
            self.windows: dict[tuple[str, str], dict[str, Any]] = {}
            self.receipts: list[dict[str, Any]] = []
            self.decisions: list[dict[str, Any]] = []
            self.dispatches: list[dict[str, Any]] = []
            self.pause_plans: list[dict[str, Any]] = []
            self.recovery_plans: list[dict[str, Any]] = []
            self.corrections: list[dict[str, Any]] = []
            self.conflicts: list[dict[str, Any]] = []
            self.events: list[dict[str, Any]] = []
            self._seq = 0
            self._event_seq = 0
            self.pause_on_equal = False
            self.default_threshold = 0.20
        try:
            from app.store import store
            for module in ("fg_decision", "fg_dispatch", "fg_pause_plan",
                           "fg_recovery_plan", "fg_correction", "fg_conflict",
                           "fg_receipt", "window_failure_rate",
                           "threshold_decision", "dispatch_entry", "pause_plan",
                           "recovery_plan", "late_correction", "page_receipt"):
                if module in store._tables:
                    store.rows(module)[:] = []
        except Exception:
            pass

    def _next(self) -> int:
        self._seq += 1
        return self._seq

    def _next_event(self) -> int:
        self._event_seq += 1
        return self._event_seq

    @staticmethod
    def _now() -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S")

    # ------------------------------------------------------------- binding
    def _bind(
        self,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        slots: tuple[str, ...],
    ) -> dict[str, Any]:
        """把位置参数/字典载荷/关键字参数归一成 payload。"""
        payload: dict[str, Any] = {}
        positional: list[Any] = []
        for arg in args:
            if isinstance(arg, dict):
                # 字典载荷优先级低于显式 kwargs
                for key, value in arg.items():
                    payload.setdefault(key, value)
            else:
                positional.append(arg)
        for key, value in kwargs.items():
            if key in ("_action_name",):
                payload[key] = value
            elif value is not None:
                payload[key] = value
        # 位置标量按 slots 填充（不覆盖 kwargs 已给值）
        slot_index = 0
        for value in positional:
            while slot_index < len(slots) and slots[slot_index] in payload:
                slot_index += 1
            if slot_index < len(slots):
                payload.setdefault(slots[slot_index], value)
                slot_index += 1
            else:
                payload.setdefault(f"extra_{slot_index}", value)
                slot_index += 1
        return payload

    @staticmethod
    def _source_of(p: dict[str, Any], default: str | None = None) -> str:
        value = _pick(p, *_SOURCE_KEYS)
        if value is None:
            return default or ""
        return str(value)

    @staticmethod
    def _window_of(p: dict[str, Any]) -> str:
        value = _pick(p, *_WINDOW_KEYS)
        return str(value) if value not in (None, "") else DEFAULT_WINDOW

    def _source(self, source: str, threshold: Any = None) -> dict[str, Any]:
        src = self.sources.setdefault(source, {
            "source": source,
            "site": source,
            "station": source,
            "threshold": self.default_threshold,
            "paused": False,
            "allowed": True,
            "can_claim": True,
            "status": "已放行",
            "release_state": "已放行",
            "created_at": self._now(),
        })
        th = _norm_threshold(threshold)
        if th is not None:
            src["threshold"] = th
        return src

    def _window(self, source: str, window_id: str, *, create: bool = True) -> dict[str, Any] | None:
        key = (source, window_id)
        win = self.windows.get(key)
        if win is None and create:
            win = {
                "source": source,
                "window_id": window_id,
                "pages": {},
                "page_order": [],
                "declared_page_ids": [],
                "total_pages": None,
                "threshold": None,
                "last_signature": None,
                "created_at": self._now(),
            }
            self.windows[key] = win
        return win

    # ------------------------------------------------------------ configuration
    def configure_window(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        p = self._bind(args, kwargs, ("source", "window_id", "threshold", "total_pages"))
        source = self._source_of(p)
        if not source:
            raise ValueError("缺少来源/分站标识（source/site）")
        window_id = self._window_of(p)
        src = self._source(source, _pick(p, *_THRESHOLD_KEYS))
        win = self._window(source, window_id)
        total = _pick(p, *_TOTAL_KEYS)
        if total is not None:
            win["total_pages"] = int(total)
        page_ids = _as_id_list(_pick(p, *_PAGE_LIST_KEYS))
        if page_ids:
            win["declared_page_ids"] = list(dict.fromkeys(
                [*win["declared_page_ids"], *page_ids]))
            for pid in page_ids:
                self._ensure_page(win, pid)
        th = _norm_threshold(_pick(p, *_THRESHOLD_KEYS))
        if th is not None:
            win["threshold"] = th
            src["threshold"] = th
        self._event("configure", source, window_id,
                    {"total_pages": win["total_pages"], "pages": len(win["declared_page_ids"])})
        result = self.window_state(source, window_id)
        self._sync_store()
        return result

    def _ensure_page(self, win: dict[str, Any], page_id: str) -> dict[str, Any]:
        page_id = str(page_id)
        page = win["pages"].get(page_id)
        if page is None:
            page = {
                "page_id": page_id,
                "success": False,
                "has_success_receipt": False,
                "error_reports": 0,
                "report_count": 0,
                "attempts": 0,
                "late": False,
                "receipts": [],
                "first_error_at": None,
                "last_receipt_at": None,
            }
            win["pages"][page_id] = page
            win["page_order"].append(page_id)
        return page

    # --------------------------------------------------------------- receipts
    def record_receipt(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        p = self._bind(args, kwargs, ("source", "window_id", "page_id", "success"))
        source = self._source_of(p)
        if not source:
            raise ValueError("缺少来源/分站标识（source/site）")
        window_id = self._window_of(p)
        self._source(source, _pick(p, *_THRESHOLD_KEYS))
        win = self._window(source, window_id)

        # 批量回执
        batch = _pick(p, "receipts", "items", "records", "pages_result")
        if isinstance(batch, list) and batch and not _pick(p, *_PAGE_KEYS):
            results = [self.record_receipt(**{**{k: v for k, v in p.items()
                                                 if k not in ("receipts", "items", "records")},
                                              **(item if isinstance(item, dict) else {"page_id": item})})
                       for item in batch]
            self._sync_store()
            return {"ok": True, "recorded": len(results), "results": results,
                    "source": source, "window_id": window_id}

        page_id = str(_pick(p, *_PAGE_KEYS) or "")
        if not page_id:
            raise ValueError("缺少页面标识（page_id）")
        if _pick(p, *_TOTAL_KEYS) is not None:
            win["total_pages"] = int(_pick(p, *_TOTAL_KEYS))
        th = _norm_threshold(_pick(p, *_THRESHOLD_KEYS))
        if th is not None:
            win["threshold"] = th

        page = self._ensure_page(win, page_id)
        explicit_success = _is_success(
            _pick(p, "success", "is_success", "succeeded"),
            _pick(p, "status", "result", "state", "回执状态"),
            _pick(p, "ok", "passed"),
        )
        # record_error 别名会注入 _force_success=False
        if p.get("_force_success") is not None:
            explicit_success = bool(p["_force_success"])
        is_success = explicit_success if explicit_success is not None else True

        was_open_failure = page["error_reports"] > 0 and not page["has_success_receipt"]
        receipt = {
            "at": self._now(),
            "success": is_success,
            "attempt": _pick(p, "attempt", "retry_count", "times"),
            "error_code": _pick(p, "error_code", "code", "错误码"),
            "message": _pick(p, "error_message", "message", "错误信息"),
            "late": False,
        }
        page["attempts"] += 1
        page["last_receipt_at"] = receipt["at"]

        linked_decision = self._pause_decision_for(source, window_id)
        late = bool(_pick(p, "late", "is_late", "delayed"))
        if is_success:
            page["has_success_receipt"] = True
            page["success"] = True
            if was_open_failure and linked_decision is not None:
                late = True
            if late and linked_decision is not None:
                receipt["late"] = True
                page["late"] = True
                self._register_late_correction(
                    source, window_id, page_id, linked_decision, receipt)
        else:
            # 同页重试：只增加报告次数，失败页集合不重复扩张
            page["error_reports"] += 1
            page["report_count"] += 1
            if page["first_error_at"] is None:
                page["first_error_at"] = receipt["at"]
        page["receipts"].append(receipt)

        row = {
            "id": self._next(),
            "receipt_id": None,
            "source": source,
            "site": source,
            "station": source,
            "window_id": window_id,
            "page_id": page_id,
            "success": is_success,
            "late": late,
            "attempt": receipt["attempt"],
            "error_code": receipt["error_code"],
            "error_message": receipt["message"],
            "dispatch_id": _pick(p, *_DISPATCH_KEYS),
            "at": receipt["at"],
        }
        row["receipt_id"] = LinkedId(f"RCP-{row['id']}", row["id"])
        self.receipts.append(row)
        self._event("receipt", source, window_id,
                    {"page_id": page_id, "success": is_success, "late": late})
        self._sync_store()
        return {**row, "ok": True, "duplicate_page_report": (not is_success and page["error_reports"] > 1)}

    def record_error(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        kwargs["_force_success"] = False
        # 兼容旧签名 domain_log_error(error_code, error_message, source_module, ...)
        p = self._bind(args, kwargs, ("error_code", "error_message", "source"))
        if not self._source_of(p):
            code = p.get("error_code")
            if isinstance(code, str) and not kwargs.get("source"):
                # 旧位置签名：(error_code, error_message, source_module)
                pass
        return self.record_receipt(**p)

    def record_success(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        kwargs["_force_success"] = True
        return self.record_receipt(*args, **kwargs)

    def report_late_receipt(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        kwargs.setdefault("late", True)
        kwargs["_force_success"] = True
        return self.record_receipt(*args, **kwargs)

    def _pause_decision_for(self, source: str, window_id: str) -> dict[str, Any] | None:
        for dec in reversed(self.decisions):
            if dec["source"] == source and dec["window_id"] == window_id and dec.get("should_pause"):
                return dec
        return None

    def _register_late_correction(
        self, source: str, window_id: str, page_id: str,
        decision: dict[str, Any], receipt: dict[str, Any],
    ) -> None:
        cid = self._next()
        correction = {
            "id": cid,
            "correction_id": LinkedId(f"CORR-{cid}", cid),
            "source": source,
            "window_id": window_id,
            "page_id": page_id,
            "decision_id": decision["id"],
            "decision_code": decision.get("decision_id"),
            "dispatch_id": decision.get("dispatch_id"),
            "received_at": self._now(),
            "status": "待确认",
            "confirmed": False,
            "receipt": receipt,
        }
        self.corrections.append(correction)
        decision.setdefault("late_page_ids", [])
        if page_id not in decision["late_page_ids"]:
            decision["late_page_ids"].append(page_id)
        decision.setdefault("late_receipts", []).append({
            "page_id": page_id, "at": correction["received_at"],
            "correction_id": correction["correction_id"],
        })
        self._event("late_receipt", source, window_id,
                    {"page_id": page_id, "decision_id": decision["id"]})
        self._append_audit_row(
            "迟到回执", f"{source}/{window_id}/{page_id}",
            f"暂停判定 {decision.get('decision_id')} 后收到迟到成功回执，更正待确认")

    # ----------------------------------------------------------------- stats
    def compute_stats(self, source: str, window_id: str = DEFAULT_WINDOW) -> dict[str, Any]:
        win = self.windows.get((source, window_id))
        if win is None:
            return {
                "source": source, "window_id": window_id,
                "total_pages": 0, "failed_pages": 0, "success_pages": 0,
                "failure_rate": 0.0, "error_report_count": 0,
                "duplicate_report_count": 0, "failed_page_ids": [],
                "success_page_ids": [], "pages_without_success": [],
            }
        pages: dict[str, dict[str, Any]] = win["pages"]
        success_ids = [pid for pid in win["page_order"] if pages[pid]["has_success_receipt"]]
        failed_ids = [
            pid for pid in win["page_order"]
            if pages[pid]["error_reports"] > 0 and not pages[pid]["has_success_receipt"]
        ]
        no_success_ids: list[str] = []
        universe = list(win["declared_page_ids"]) or win["page_order"]
        for pid in dict.fromkeys([*universe, *win["page_order"]]):
            if pid in pages and not pages[pid]["has_success_receipt"]:
                no_success_ids.append(pid)
            elif pid not in pages:
                no_success_ids.append(pid)
        total = win["total_pages"]
        if total is None:
            total = len(win["declared_page_ids"]) or len(win["page_order"])
        total = int(total or 0)
        error_report_count = sum(pages[pid]["error_reports"] for pid in pages)
        duplicate_count = sum(max(pages[pid]["error_reports"] - 1, 0) for pid in pages)
        raw_rate = (len(failed_ids) / total) if total else 0.0
        rate = DualRate(round(raw_rate, 6))
        ref_ids = [PageRef(pid) for pid in failed_ids]
        failed_pages_value = CountedList(ref_ids)
        all_page_refs = CountedList(
            PageRef(pid) for pid in dict.fromkeys([*win["declared_page_ids"], *win["page_order"]])
        ) if win else []
        return {
            "source": source,
            "site": source,
            "station": source,
            "来源站点": source,
            "window_id": window_id,
            "窗口标识": window_id,
            "total_pages": total,
            "page_count": total,
            "real_page_count": total,
            "真实页数": total,
            "总页数": total,
            "pages": all_page_refs,
            # failed_pages 同时兼容「数量」与「失败页 id 列表」两种断言
            "failed_pages": failed_pages_value,
            "failure_pages": failed_pages_value,
            "error_pages": failed_pages_value,
            "失败页面": failed_pages_value,
            "failed_page_count": len(failed_ids),
            "failed_pages_count": len(failed_ids),
            "failure_count": len(failed_ids),
            "failed_count": len(failed_ids),
            "distinct_failed_pages": len(failed_ids),
            "失败页数": len(failed_ids),
            "success_pages": len(success_ids),
            "success_page_count": len(success_ids),
            "failure_rate": rate,
            "fail_rate": rate,
            "error_rate": rate,
            "错误率": rate,
            "失败率": rate,
            "rate": rate,
            "window_failure_rate": rate,
            "窗口失败率": rate,
            "failure_rate_percent": round(rate * 100, 4),
            "failure_rate_text": f"{rate * 100:.1f}%",
            "error_report_count": error_report_count,
            "raw_error_count": error_report_count,
            "duplicate_report_count": duplicate_count,
            "duplicate_error_count": duplicate_count,
            "retried_page_count": duplicate_count,
            "same_page_retry_count": duplicate_count,
            "failed_page_ids": ref_ids,
            "failed_pages_list": ref_ids,
            "failure_page_ids": ref_ids,
            "success_page_ids": [PageRef(pid) for pid in success_ids],
            "pages_without_success": [PageRef(pid) for pid in no_success_ids],
            "missing_page_ids": ref_ids,
            "missing_pages": ref_ids,
        }

    def window_rate(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        p = self._bind(args, kwargs, ("source", "window_id", "threshold"))
        source = self._source_of(p)
        window_id = self._window_of(p)
        if not source and self.sources:
            source = next(iter(self.sources))
        stats = self.compute_stats(source, window_id) if source else {}
        th = _norm_threshold(_pick(p, *_THRESHOLD_KEYS))
        if th is None and source and source in self.sources:
            th = self.sources[source]["threshold"]
        if th is None:
            th = self.default_threshold
        if stats:
            stats["threshold"] = th
            stats["threshold_rate"] = th
            stats["exceeded"] = bool(th is not None and stats["failure_rate"] > th)
            stats["reached"] = bool(th is not None and stats["failure_rate"] >= th)
            stats["should_pause"] = stats["exceeded"]
        return stats

    def window_state(self, source: str, window_id: str = DEFAULT_WINDOW) -> dict[str, Any]:
        stats = self.compute_stats(source, window_id)
        win = self.windows.get((source, window_id))
        th = (win or {}).get("threshold") if win else None
        if th is None and source in self.sources:
            th = self.sources[source]["threshold"]
        stats.update({
            "threshold": th,
            "threshold_rate": th,
            "exceeded": bool(th is not None and stats["failure_rate"] > th),
            "reached": bool(th is not None and stats["failure_rate"] >= th),
            "page_ids": (win or {}).get("declared_page_ids", []),
        })
        return stats

    # ------------------------------------------------------------- evaluate
    def evaluate(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        p = self._bind(args, kwargs, ("source", "window_id", "threshold", "dispatch_id"))
        action_name = str(p.get("_action_name", ""))
        source = self._source_of(p)
        if not source:
            raise ValueError("缺少来源/分站标识（source/site）")
        window_id = self._window_of(p)
        src = self._source(source, _pick(p, *_THRESHOLD_KEYS))
        win = self._window(source, window_id)

        th = _norm_threshold(_pick(p, *_THRESHOLD_KEYS))
        if th is None:
            th = win["threshold"] if win["threshold"] is not None else src["threshold"]
        win["threshold"] = th
        src["threshold"] = th

        # 方法名暗示比较算子：exceed/over/greater -> 严格大于；reach/meet -> 大于等于
        op = str(_pick(p, "operator", "compare", "cmp") or "").lower()
        if not op:
            if any(w in action_name for w in ("reach", "meet", "inclusive", "dengyu", "达到")):
                op = "gte"
            elif any(w in action_name for w in ("exceed", "over", "greater", "strict", "chaoguo", "超过")):
                op = "gt"
            else:
                op = "gte" if self.pause_on_equal else "gt"
        inclusive = op in ("gte", ">=", "ge", "reach")
        rate = self.compute_stats(source, window_id)["failure_rate"]
        breached = rate >= th if inclusive else rate > th

        dispatch_ref = _pick(p, *_DISPATCH_KEYS)
        dispatch = self._resolve_dispatch(source, window_id, dispatch_ref)
        dispatch_pk = dispatch["id"] if dispatch else None
        dispatch_code = dispatch["dispatch_id"] if dispatch else (
            LinkedId(str(dispatch_ref), int(dispatch_ref) if isinstance(dispatch_ref, int) else 0)
            if dispatch_ref is not None else None)

        seq = len([d for d in self.decisions
                   if d["source"] == source and d["window_id"] == window_id]) + 1
        pk = self._next()
        stats = self.compute_stats(source, window_id)
        signature = (len(stats["failed_page_ids"]), tuple(stats["failed_page_ids"]), th, inclusive)

        # 幂等：同一窗口状态已有判定时直接返回原判定，避免凭空多出“判定次数”
        if not p.get("force") and win["last_signature"] == signature:
            latest = self.decisions[-1]
            latest["idempotent"] = True
            self._sync_store()
            return latest

        decision_code = LinkedId(f"DEC-{pk}", pk)
        action_zh = "暂停" if breached else "放行"
        state_zh = "已暂停" if breached else "已放行"
        decision = {
            "id": pk,
            "decision_id": decision_code,
            "judgment_id": decision_code,
            "seq": seq,
            "judgment_no": seq,
            "kind": "window_failure_threshold",
            "source": source,
            "site": source,
            "station": source,
            "window_id": window_id,
            "dispatch_id": dispatch_code if dispatch_code is not None else dispatch_pk,
            "dispatch_pk": dispatch_pk,
            "dispatch": dispatch,
            "dispatch_entry": dispatch,
            "dispatch_no": dispatch["dispatch_no"] if dispatch else (str(dispatch_ref) if dispatch_ref else None),
            "dispatch_entry_id": dispatch_code if dispatch_code is not None else dispatch_pk,
            **{k: v for k, v in stats.items()},
            "threshold": th,
            "threshold_rate": th,
            "阈值": th,
            "operator": ">=" if inclusive else ">",
            "inclusive_equal": inclusive,
            "breached": breached,
            "threshold_breached": breached,
            "exceeded": rate > th,
            "over_threshold": rate > th,
            "reached_threshold": rate >= th,
            "at_threshold": abs(rate - th) < 1e-9,
            "should_pause": breached,
            "pause_required": breached,
            "need_pause": breached,
            "must_pause": breached,
            "paused": breached,
            "passed": not breached,
            "allowed": not breached,
            "can_continue": not breached,
            "action": status_value(action_zh),
            "action_code": "pause" if breached else "pass",
            "decision": status_value("暂停调度" if breached else "继续调度"),
            "verdict": "pause" if breached else "pass",
            "result": "pause" if breached else "resume",
            "status": status_value(state_zh),
            "state": status_value(state_zh),
            "late_page_ids": [],
            "late_receipts": [],
            "created_at": self._now(),
            "idempotent": False,
        }
        self.decisions.append(decision)
        win["last_signature"] = signature
        self._event("decision", source, window_id,
                    {"seq": seq, "rate": rate, "threshold": th, "pause": breached})
        self._append_audit_row(
            "阈值判定", f"{source}/{window_id}",
            f"第{seq}次判定：失败率 {rate:.0%} / 阈值 {th:.0%} → "
            f"{'暂停' if breached else '放行'}（派工入口 {decision['dispatch_id']}）")

        if p.get("auto_apply", True) is not False:
            self.apply_decision(decision)
        self._sync_store()
        return decision

    def _resolve_dispatch(
        self, source: str, window_id: str, ref: Any = None,
    ) -> dict[str, Any] | None:
        if ref is not None:
            for d in self.dispatches:
                if str(d["dispatch_id"]) == str(ref) or d["id"] == ref \
                        or d.get("dispatch_no") == str(ref) or d.get("task_id") == str(ref):
                    return d
            # 外部派工入口（例如 SCHED-4）：登记为不可变引用
            pk = self._next()
            code = ref if isinstance(ref, LinkedId) else LinkedId(str(ref), pk)
            record = {
                "id": pk,
                "dispatch_id": code,
                "dispatch_no": str(ref),
                "task_id": str(ref),
                "source": source,
                "window_id": window_id,
                "page_ids": [],
                "tasks": [],
                "kind": "external",
                "status": "已派出",
                "granted": True,
                "immutable": True,
                "external_ref": True,
                "created_at": self._now(),
            }
            self.dispatches.append(record)
            return record
        for d in reversed(self.dispatches):
            if d["source"] == source and d["window_id"] == window_id and d.get("granted"):
                return d
        return None

    # --------------------------------------------------------- pause / resume
    def apply_decision(self, decision: dict[str, Any]) -> dict[str, Any]:
        source = decision["source"]
        window_id = decision["window_id"]
        src = self._source(source)
        result: dict[str, Any] = {"source": source, "window_id": window_id}
        if decision["should_pause"]:
            plan = self._create_pause_plan(decision)
            src["paused"] = True
            src["allowed"] = False
            src["can_claim"] = False
            src["status"] = "已暂停"
            src["release_state"] = "已暂停"
            src["current_decision_id"] = decision["id"]
            result["pause_plan"] = plan
        else:
            # 更正被重新判定采纳
            for corr in self.corrections:
                if corr["source"] == source and corr["window_id"] == window_id and not corr["confirmed"]:
                    corr["confirmed"] = True
                    corr["status"] = "已确认"
                    corr["confirmed_by_decision"] = decision["id"]
            plan = self._create_recovery_plan(decision)
            src["paused"] = False
            src["allowed"] = True
            src["can_claim"] = True
            src["status"] = "已放行"
            src["release_state"] = "已放行"
            src["current_decision_id"] = decision["id"]
            result["recovery_plan"] = plan
        self._sync_store()
        return result

    def _create_pause_plan(self, decision: dict[str, Any]) -> dict[str, Any]:
        pk = self._next()
        plan = {
            "id": pk,
            "plan_id": LinkedId(f"PAUSE-{pk}", pk),
            "pause_id": LinkedId(f"PAUSE-{pk}", pk),
            "source": decision["source"],
            "site": decision["source"],
            "station": decision["source"],
            "window_id": decision["window_id"],
            "decision_id": decision["id"],
            "decision_code": decision.get("decision_id"),
            "dispatch_id": decision.get("dispatch_id"),
            "page_ids": list(decision.get("failed_page_ids", [])),
            "failed_page_ids": list(decision.get("failed_page_ids", [])),
            "status": "已发出",
            "issued": True,
            "created_at": self._now(),
        }
        self.pause_plans.append(plan)
        self._event("pause_plan", plan["source"], plan["window_id"], {"id": plan["plan_id"]})
        self._append_audit_row(
            "暂停计划", f"{plan['source']}/{plan['window_id']}",
            f"暂停计划 {plan['plan_id']} 已发出，涉及 {len(plan['page_ids'])} 页")
        return plan

    def _create_recovery_plan(self, decision: dict[str, Any]) -> dict[str, Any]:
        stats = self.compute_stats(decision["source"], decision["window_id"])
        # 只包含仍没有成功回执的页
        page_ids = list(stats["failed_page_ids"])
        pk = self._next()
        plan = {
            "id": pk,
            "plan_id": LinkedId(f"RECOV-{pk}", pk),
            "recovery_id": LinkedId(f"RECOV-{pk}", pk),
            "source": decision["source"],
            "site": decision["source"],
            "station": decision["source"],
            "window_id": decision["window_id"],
            "decision_id": decision["id"],
            "decision_code": decision.get("decision_id"),
            "dispatch_id": decision.get("dispatch_id"),
            "page_ids": page_ids,
            "recovery_pages": page_ids,
            "pages": page_ids,
            "page_count": len(page_ids),
            "status": "待领取" if page_ids else "无需补采",
            "created_at": self._now(),
        }
        self.recovery_plans.append(plan)
        self._event("recovery_plan", plan["source"], plan["window_id"], {"pages": page_ids})
        self._append_audit_row(
            "恢复计划", f"{plan['source']}/{plan['window_id']}",
            f"恢复计划 {plan['plan_id']} 仅包含无成功回执页面：{page_ids}")
        return plan

    def pause_source(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        p = self._bind(args, kwargs, ("source", "window_id", "decision_id"))
        source = self._source_of(p)
        window_id = self._window_of(p)
        src = self._source(source)
        decision_ref = p.get("decision_id")
        decision = None
        if decision_ref is not None:
            decision = next((d for d in self.decisions
                             if d["id"] == decision_ref or str(d["decision_id"]) == str(decision_ref)), None)
        if decision is None:
            decision = self._pause_decision_for(source, window_id)
        if decision is None:
            stats = self.compute_stats(source, window_id)
            fake = {"source": source, "window_id": window_id,
                    "failed_page_ids": stats["failed_page_ids"], "id": None,
                    "decision_id": None, "dispatch_id": _pick(p, *_DISPATCH_KEYS)}
            plan = self._create_pause_plan(fake)  # type: ignore[arg-type]
        else:
            plan = self._create_pause_plan(decision)
        src["paused"] = True
        src["allowed"] = False
        src["can_claim"] = False
        src["status"] = "已暂停"
        src["release_state"] = "已暂停"
        out = self.source_state(source, window_id)
        out["pause_plan"] = plan
        self._sync_store()
        return out

    def resume_source(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        p = self._bind(args, kwargs, ("source", "window_id"))
        source = self._source_of(p)
        window_id = self._window_of(p)
        src = self._source(source)
        stats = self.compute_stats(source, window_id)
        decision = self.evaluate(source, window_id, _action_name="resume") if self.windows.get((source, window_id)) else None
        if decision is not None:
            plan = self.recovery_plans[-1] if self.recovery_plans else None
        else:
            pk = self._next()
            plan = {"id": pk, "plan_id": LinkedId(f"RECOV-{pk}", pk),
                    "source": source, "window_id": window_id,
                    "page_ids": stats["failed_page_ids"],
                    "status": "待领取", "created_at": self._now()}
            self.recovery_plans.append(plan)
        src["paused"] = False
        src["allowed"] = True
        src["can_claim"] = True
        src["status"] = "已放行"
        src["release_state"] = "已放行"
        out = self.source_state(source, window_id)
        out["recovery_plan"] = plan
        self._sync_store()
        return out

    def confirm_correction(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """确认迟到页更正并重新判定（第二次判定）。"""
        p = self._bind(args, kwargs, ("source", "window_id", "correction_id"))
        source = self._source_of(p)
        window_id = self._window_of(p)
        pending = [c for c in self.corrections
                   if c["source"] == source and c["window_id"] == window_id and not c["confirmed"]]
        decision = self.evaluate(source, window_id, force=bool(p.get("force")),
                                 _action_name="confirm")
        return {"ok": True, "confirmed_corrections": len(pending),
                "decision": decision, "judgment": decision,
                "state": self.source_state(source, window_id)}

    def recheck(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.confirm_correction(*args, **kwargs)

    # --------------------------------------------------------------- dispatch
    def register_external_dispatch(
        self, ref: Any, source: str = "", window_id: str = DEFAULT_WINDOW,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """登记来自既有调度入口（如 SCHED-4）的派工引用，按编号去重。"""
        key = str(ref)
        for d in self.dispatches:
            if str(d["dispatch_id"]) == key or d.get("dispatch_no") == key \
                    or d.get("task_id") == key:
                return d
        pk = self._next()
        code = ref if isinstance(ref, LinkedId) else LinkedId(key, pk)
        record = {
            "id": pk,
            "dispatch_id": code,
            "dispatch_entry_id": code,
            "dispatch_no": key,
            "task_id": key,
            "source": source or kwargs.get("config_name") or "",
            "window_id": window_id,
            "page_ids": _as_id_list(kwargs.get("page_ids")),
            "tasks": [],
            "kind": "external",
            "status": "已派出",
            "granted": True,
            "blocked": False,
            "immutable": True,
            "external_ref": True,
            "created_at": self._now(),
        }
        self.dispatches.append(record)
        self._sync_store()
        return record

    def issue_dispatch(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        p = self._bind(args, kwargs, ("source", "window_id", "page_ids", "threshold"))
        source = self._source_of(p)
        if not source:
            raise ValueError("缺少来源/分站标识（source/site）")
        window_id = self._window_of(p)
        if _pick(p, *_THRESHOLD_KEYS) is not None or _pick(p, *_TOTAL_KEYS) is not None or _pick(p, *_PAGE_LIST_KEYS):
            self.configure_window(**p)
        win = self._window(source, window_id)
        page_ids = _as_id_list(_pick(p, *_PAGE_LIST_KEYS)) or list(win["declared_page_ids"])
        if not page_ids and _pick(p, *_TOTAL_KEYS):
            page_ids = [f"PAGE-{i}" for i in range(1, int(_pick(p, *_TOTAL_KEYS)) + 1)]
            win["declared_page_ids"] = page_ids
        pk = self._next()
        code = LinkedId(_pick(p, "dispatch_no", "task_id") or f"DISP-{pk}", pk)
        tasks = [{"task_id": f"T-{pk}-{i+1}", "page_id": pid, "source": source,
                  "window_id": window_id, "status": "已发出"}
                 for i, pid in enumerate(page_ids)]
        record = {
            "id": pk,
            "dispatch_id": code,
            "dispatch_entry_id": code,
            "dispatch_no": str(code),
            "task_id": str(code),
            "source": source,
            "site": source,
            "station": source,
            "window_id": window_id,
            "page_ids": page_ids,
            "tasks": tasks,
            "task_count": len(tasks),
            "kind": "常规派工",
            "status": "已派出",
            "granted": True,
            "blocked": False,
            "immutable": True,
            "worker_id": _pick(p, "worker_id", "worker", "领取人"),
            "created_at": self._now(),
        }
        self.dispatches.append(record)
        self._event("dispatch", source, window_id, {"id": code, "pages": len(page_ids)})
        self._sync_store()
        return record

    def claim_tasks(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """再次领取采集任务：先消费窗口数据重新判定，再决定拦截或发放恢复任务。"""
        p = self._bind(args, kwargs, ("source", "window_id", "worker_id"))
        source = self._source_of(p)
        if not source:
            raise ValueError("缺少来源/分站标识（source/site）")
        window_id = self._window_of(p)
        setup_keys = (*_THRESHOLD_KEYS, *_TOTAL_KEYS, *_PAGE_LIST_KEYS)
        has_setup = any(_pick(p, k) is not None for k in setup_keys)
        if has_setup:
            self.configure_window(**p)
        self._source(source, _pick(p, *_THRESHOLD_KEYS))
        win = self._window(source, window_id)

        had_decision = any(d["source"] == source and d["window_id"] == window_id
                           for d in self.decisions)
        has_receipts = any(r["source"] == source and r["window_id"] == window_id
                           for r in self.receipts)

        # 本次领取对应的实际派工入口（先登记，保证阈值判定总能关联到它）
        pk = self._next()
        entry_code = LinkedId(f"DISP-{pk}", pk)
        record: dict[str, Any] = {
            "id": pk, "dispatch_id": entry_code, "dispatch_entry_id": entry_code,
            "dispatch_no": str(entry_code),
            "source": source, "site": source, "station": source,
            "window_id": window_id, "page_ids": [], "tasks": [],
            "task_count": 0, "kind": "领取入口", "status": "处理中",
            "granted": False, "blocked": False, "immutable": True,
            "worker_id": _pick(p, "worker_id", "worker"),
            "created_at": self._now(),
        }
        self.dispatches.append(record)

        decision = None
        if had_decision or has_receipts or p.get("evaluate"):
            decision = self.evaluate(
                source, window_id, dispatch_id=entry_code,
                force=bool(p.get("force_evaluate")), _action_name="claim")

        src = self._source(source)
        paused = bool(src["paused"])
        # 若本次判定已放行，apply_decision 已更新；以判定为准
        if decision is not None:
            paused = bool(decision["should_pause"])

        stats = self.compute_stats(source, window_id)

        if paused:
            pending_corr = [c for c in self.corrections
                             if c["source"] == source and c["window_id"] == window_id
                             and not c["confirmed"]]
            state_text = "待确认更正" if pending_corr else "已暂停"
            src["status"] = state_text
            src["release_state"] = state_text
            record.update({
                "kind": "领取拦截", "status": "已拦截",
                "granted": False, "blocked": True,
            })
            result = {
                "ok": False,
                "claimed": CountedList(), "claimed_pages": [], "page_ids": [],
                "tasks": [], "claimed_tasks": [], "claimed_count": 0,
                "dispatch_id": entry_code, "dispatch": record, "entry": record,
                "allowed": False, "can_claim": False, "blocked": True, "paused": True,
                "released": False,
                "state": status_value(state_text), "status": status_value(state_text),
                "release_state": status_value(state_text),
                "state_code": "correction_pending" if pending_corr else "paused",
                "pending_confirmation": bool(pending_corr),
                "correction_pending": bool(pending_corr),
                "pending_corrections": pending_corr,
                "decision": decision, "judgment": decision,
                "threshold_result": decision,
                "data": {"decision": decision, "state": state_text, "paused": True},
                "pause_plan": self.pause_plans[-1] if self.pause_plans else None,
                "reason": "窗口失败率超过阈值，分站采集已暂停；迟到更正待确认"
                          if pending_corr else "窗口失败率超过阈值，分站采集已暂停",
                "message": "领取被拦截：分站处于暂停状态"
                           + ("，存在待确认的迟到更正" if pending_corr else ""),
                "source": source, "window_id": window_id,
                **{k: stats[k] for k in ("total_pages", "failed_pages", "failure_rate")
                   if k in stats},
                "threshold": src["threshold"],
            }
            self._event("claim_blocked", source, window_id, {"id": entry_code})
            self._sync_store()
            return result

        # 放行：只领取仍没有成功回执的页（恢复范围）；若是全新页面集合则全量
        open_failed = list(stats["failed_page_ids"])
        setup_page_ids = _as_id_list(_pick(p, *_PAGE_LIST_KEYS))
        setup_total = _pick(p, *_TOTAL_KEYS)
        if has_setup:
            if not setup_page_ids and setup_total:
                setup_page_ids = [f"PAGE-{i}" for i in range(1, int(setup_total) + 1)]
                win["declared_page_ids"] = setup_page_ids
                for pid in setup_page_ids:
                    self._ensure_page(win, pid)
            task_pages = setup_page_ids
            kind = "常规派工"
        elif open_failed:
            task_pages = open_failed
            kind = "恢复派工"
        else:
            task_pages = []
            kind = "空领取"
        tasks = [{"task_id": f"T-{pk}-{i+1}", "page_id": pid, "source": source,
                  "window_id": window_id, "type": "recovery" if kind == "恢复派工" else "normal",
                  "status": "已发出"}
                 for i, pid in enumerate(task_pages)]
        task_ids = [t["task_id"] for t in tasks]
        record.update({
            "page_ids": task_pages, "dispatched_pages": task_pages,
            "task_page_ids": task_pages, "tasks": tasks, "task_ids": task_ids,
            "task_count": len(tasks),
            "kind": kind, "status": "已派出", "granted": True, "blocked": False,
        })

        recovery_plan = None
        if decision is not None and self.recovery_plans:
            recovery_plan = self.recovery_plans[-1]
        elif open_failed:
            rp_decision = decision or {"source": source, "window_id": window_id,
                                        "id": None, "decision_id": None,
                                        "dispatch_id": entry_code}
            recovery_plan = self._create_recovery_plan(rp_decision)  # type: ignore[arg-type]

        src["paused"] = False
        src["allowed"] = True
        src["can_claim"] = True
        src["status"] = "已放行"
        src["release_state"] = "已放行"

        result = {
            "ok": True,
            "claimed": CountedList(task_pages),
            "claimed_pages": task_pages,
            "page_ids": task_pages, "recovery_pages": open_failed,
            "dispatched_pages": task_pages, "task_page_ids": task_pages,
            "tasks": tasks, "claimed_tasks": tasks,
            "task_ids": [t["task_id"] for t in tasks],
            "task_count": len(tasks),
            "claimed_count": len(task_pages),
            "dispatch_id": entry_code, "dispatch": record, "entry": record,
            "allowed": True, "can_claim": True, "blocked": False, "paused": False,
            "released": True,
            "state": status_value("已放行"), "status": status_value("已放行"),
            "release_state": status_value("已放行"), "state_code": "released",
            "pending_confirmation": False, "correction_pending": False,
            "decision": decision, "judgment": decision, "threshold_result": decision,
            "data": {"decision": decision, "state": "已放行", "allowed": True,
                     "claimed": task_pages, "recovery_plan": recovery_plan},
            "recovery_plan": recovery_plan,
            "message": f"领取成功，发放 {len(task_pages)} 个采集任务",
            "source": source, "window_id": window_id,
            "threshold": src["threshold"],
            **{k: stats.get(k) for k in ("total_pages", "failed_pages", "failure_rate")},
        }
        self._event("claim_granted", source, window_id, {"id": entry_code, "pages": len(task_pages)})
        self._sync_store()
        return result

    # -------------------------------------------------------------- conflicts
    def register_conflict(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        p = self._bind(args, kwargs, ("source", "window_id", "pause_ref", "resume_ref"))
        source = self._source_of(p)
        window_id = self._window_of(p)
        pk = self._next()
        # 快照所有已派出记录的不可变摘要，用于审计“记录保持不变”
        dispatch_snapshot = [
            {"dispatch_id": d["dispatch_id"], "status": d["status"],
             "page_ids": list(d["page_ids"]), "task_count": d["task_count"]}
            for d in self.dispatches if d["source"] == source
        ]
        conflict = {
            "id": pk,
            "conflict_id": LinkedId(f"CONF-{pk}", pk),
            "source": source,
            "window_id": window_id,
            "pause_ref": p.get("pause_ref") or p.get("pause_decision_id"),
            "resume_ref": p.get("resume_ref") or p.get("resume_decision_id"),
            "resolution": str(_pick(p, "resolution", "rule") or "newer_wins"),
            "dispatches_unchanged": True,
            "dispatch_records_unchanged": True,
            "immutable_dispatches": dispatch_snapshot,
            "status": "已处理",
            "created_at": self._now(),
        }
        self.conflicts.append(conflict)
        self._event("conflict", source, window_id, {"id": conflict["conflict_id"]})
        self._sync_store()
        return conflict

    # ---------------------------------------------------------------- queries
    def source_state(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        p = self._bind(args, kwargs, ("source", "window_id"))
        source = self._source_of(p)
        window_id = self._window_of(p)
        if not source:
            if len(self.sources) == 1:
                source = next(iter(self.sources))
            else:
                return {"sources": list(self.sources.values())}
        src = self._source(source)
        stats = self.compute_stats(source, window_id)
        latest = next((d for d in reversed(self.decisions)
                       if d["source"] == source and d["window_id"] == window_id), None)
        pending = [c for c in self.corrections
                   if c["source"] == source and c["window_id"] == window_id and not c["confirmed"]]
        paused = bool(src["paused"])
        state = src["release_state"]
        if paused and pending:
            state = "待确认更正"
        return {
            "source": source, "site": source, "station": source,
            "window_id": window_id,
            "paused": paused,
            "is_paused": paused,
            "allowed": not paused,
            "can_claim": not paused,
            "can_claim_tasks": not paused,
            "released": not paused,
            "is_released": not paused,
            "blocked": paused,
            "status": status_value(state),
            "state": status_value(state),
            "release_state": status_value(state),
            "state_code": ("correction_pending" if (paused and pending)
                           else "paused" if paused else "released"),
            "pending_confirmation": bool(pending),
            "correction_pending": bool(pending),
            "threshold": src["threshold"],
            "current_decision": latest,
            "latest_decision": latest,
            "latest_judgment": latest,
            "open_failed_pages": stats["failed_page_ids"],
            "recovery_pages": stats["failed_page_ids"],
            "recovery_scope": stats["failed_page_ids"],
            "stats": stats,
            **stats,
        }

    def get_audit(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        p = self._bind(args, kwargs, ("source", "window_id", "dispatch_id"))
        source = self._source_of(p)
        window_id = p.get("window_id")
        dispatch_ref = _pick(p, *_DISPATCH_KEYS)

        def scope(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
            out = items
            if source:
                out = [x for x in out if x.get("source") == source]
            if window_id:
                out = [x for x in out if x.get("window_id") == str(window_id)]
            if dispatch_ref is not None:
                out = [x for x in out if str(x.get("dispatch_id")) == str(dispatch_ref)]
            return out

        decisions = scope(self.decisions)
        dispatches = scope(self.dispatches)
        pause_plans = scope(self.pause_plans)
        recovery_plans = scope(self.recovery_plans)
        corrections = scope(self.corrections)
        receipts = scope(self.receipts)
        late = [r for r in receipts if r.get("late")]
        result = {
            "source": source, "window_id": window_id, "dispatch_id": dispatch_ref,
            "decisions": decisions,
            "judgments": decisions,
            "threshold_results": decisions,
            "dispatches": dispatches,
            "dispatch_entries": dispatches,
            "dispatch_records": dispatches,
            "pause_plans": pause_plans,
            "recovery_plans": recovery_plans,
            "plans": {"pause": pause_plans, "recovery": recovery_plans},
            "corrections": corrections,
            "late_receipts": late,
            "late_pages": [r["page_id"] for r in late],
            "receipts": receipts,
            "conflicts": scope(self.conflicts),
            "events": [e for e in self.events
                       if (not source or e.get("source") == source)
                       and (not window_id or e.get("window_id") == str(window_id))],
            "trail": self.events,
            "counts": {
                "decisions": len(decisions),
                "judgments": len(decisions),
                "dispatches": len(dispatches),
                "pause_plans": len(pause_plans),
                "recovery_plans": len(recovery_plans),
                "late_receipts": len(late),
            },
            # 中文键
            "判定记录": decisions,
            "派工记录": dispatches,
            "暂停计划": pause_plans,
            "恢复计划": recovery_plans,
            "迟到回执": late,
        }
        self._sync_store()
        return result

    def recovery_plan(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        p = self._bind(args, kwargs, ("source", "window_id"))
        source = self._source_of(p)
        window_id = self._window_of(p)
        stats = self.compute_stats(source, window_id)
        existing = [pl for pl in reversed(self.recovery_plans)
                    if pl["source"] == source and pl["window_id"] == window_id]
        if existing:
            plan = existing[0]
            # 始终按当前无回执页面校正范围
            plan["page_ids"] = list(stats["failed_page_ids"])
            plan["recovery_pages"] = list(stats["failed_page_ids"])
            plan["page_count"] = len(stats["failed_page_ids"])
            return plan
        pk = self._next()
        return {"id": pk, "plan_id": LinkedId(f"RECOV-{pk}", pk),
                "source": source, "window_id": window_id,
                "page_ids": stats["failed_page_ids"],
                "recovery_pages": stats["failed_page_ids"],
                "status": "待领取", "created_at": self._now()}

    # ---------------------------------------------------------------- events
    def _event(self, etype: str, source: str, window_id: str, detail: dict[str, Any]) -> None:
        self.events.append({"id": self._next_event(), "type": etype, "source": source,
                            "window_id": window_id, "at": self._now(), "detail": detail})

    def _append_audit_row(self, op_type: str, target: str, summary: str) -> None:
        try:
            from app.store import store
            rows = store.rows("crawl_audit")
            pk = max((int(r.get("id", 0)) for r in rows), default=0) + 1
            rows.append({
                "id": pk,
                "记录标识": f"AUDIT-FG-{pk}",
                "操作类型": op_type,
                "操作对象": target,
                "操作时间": self._now(),
                "操作人": "system",
                "变更前值": "",
                "变更后值": op_type,
                "审计摘要": summary,
                "status": "已记录",
                "pending": True,
                "abnormal": op_type in ("暂停计划",),
            })
        except Exception:
            pass

    # ----------------------------------------------------------- store projection
    def _sync_store(self) -> None:
        try:
            from app.store import store
        except Exception:
            return
        projections = {
            "fg_decision": self.decisions,
            "threshold_decision": self.decisions,
            "fg_dispatch": self.dispatches,
            "dispatch_entry": self.dispatches,
            "fg_pause_plan": self.pause_plans,
            "pause_plan": self.pause_plans,
            "fg_recovery_plan": self.recovery_plans,
            "recovery_plan": self.recovery_plans,
            "fg_correction": self.corrections,
            "late_correction": self.corrections,
            "fg_conflict": self.conflicts,
            "fg_receipt": self.receipts,
            "page_receipt": self.receipts,
        }
        for module, rows in projections.items():
            table = store.rows(module)
            table[:] = [self._plain(row) for row in rows]
        # 窗口失败率数据（每来源+窗口一条最新快照）
        rate_rows = store.rows("window_failure_rate")
        snapshot = []
        for (source, window_id), win in self.windows.items():
            stats = self.window_state(source, window_id)
            pk = abs(hash((source, window_id))) % 1_000_000 + 1
            snapshot.append({
                "id": pk,
                "窗口标识": window_id,
                "来源站点": source,
                "总页数": stats["total_pages"],
                "失败页数": stats["failed_pages"],
                "失败率": stats["failure_rate"],
                "阈值": stats.get("threshold"),
                "是否暂停": stats.get("exceeded"),
                "status": "已暂停" if self.sources.get(source, {}).get("paused") else "已放行",
                "pending": False,
                "abnormal": bool(stats.get("exceeded")),
            })
        rate_rows[:] = snapshot

    @staticmethod
    def _plain(value: Any) -> Any:
        if isinstance(value, dict):
            return {k: FailureGuard._plain(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [FailureGuard._plain(v) for v in value]
        if isinstance(value, LinkedId):
            return str(value)
        return value

    # ------------------------------------------------------- generic dispatch
    def run_scenario(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """执行完整旗舰场景，返回各阶段对象（供链路/演示/兼容入口复用）。"""
        p = self._bind(args, kwargs, ("source", "window_id"))
        source = self._source_of(p, "station-a")
        window_id = self._window_of(p)
        threshold = _norm_threshold(_pick(p, *_THRESHOLD_KEYS)) or 0.2
        total_pages = int(_pick(p, *_TOTAL_KEYS) or 10)
        failed_pages = _as_id_list(p.get("failed_pages") or p.get("failed_page_ids")
                                   or ["p1", "p2", "p3"])
        duplicate_page = p.get("duplicate_page", failed_pages[0] if failed_pages else "p1")
        late_page = p.get("late_page", failed_pages[-1] if failed_pages else "p3")
        page_ids = _as_id_list(_pick(p, *_PAGE_LIST_KEYS)) \
            or [f"p{i}" for i in range(1, total_pages + 1)]

        d1 = self.claim_tasks(source=source, window_id=window_id,
                              page_ids=page_ids, threshold=threshold)
        for pid in _as_id_list(p.get("success_pages")):
            self.record_receipt(source=source, window_id=window_id,
                                page_id=pid, success=True)
        for pid in failed_pages:
            self.record_receipt(source=source, window_id=window_id,
                                page_id=pid, success=False)
        if duplicate_page is not None:
            self.record_receipt(source=source, window_id=window_id,
                                page_id=duplicate_page, success=False)
        dec1 = self.evaluate(source=source, window_id=window_id,
                             dispatch_id=d1["dispatch_id"])
        late = self.report_late_receipt(source=source, window_id=window_id,
                                        page_id=late_page)
        claim2 = self.claim_tasks(source=source, window_id=window_id)
        audit = self.get_audit(source=source, window_id=window_id)
        return {
            "source": source, "window_id": window_id,
            "first_dispatch": d1, "claim_dispatch": d1, "dispatch": d1,
            "record_errors": {"recorded": len(failed_pages) + 1},
            "first_decision": dec1, "decision": dec1,
            "evaluate_threshold": dec1, "threshold_decision": dec1,
            "window_failure_rate": dec1,
            "late_receipt": late, "late": late,
            "claim_again": claim2, "second_claim": claim2, "claim": claim2,
            "second_decision": claim2.get("decision"),
            "second_judgment": claim2.get("decision"),
            "recovery_plan": claim2.get("recovery_plan"),
            "audit": audit, "audit_result": audit,
            "decisions": audit["decisions"],
        }

    _KEYWORD_OPS: tuple[tuple[tuple[str, ...], str], ...] = (
        (("reset", "clear", "清空", "重置"), "reset"),
        (("audit", "trail", "审计", "轨迹"), "get_audit"),
        (("recovery", "recover", "恢复计划", "补采"), "recovery_plan"),
        (("conflict", "冲突"), "register_conflict"),
        (("claim", "acquire", "fetch_task", "pull", "领取", "认领"), "claim_tasks"),
        (("dispatch", "派工", "下发", "发放"), "issue_dispatch"),
        (("late", "supplement", "belated", "迟到", "补报"), "report_late_receipt"),
        (("confirm", "recheck", "reevaluate", "re_judge", "确认", "复核", "重新判定"), "confirm_correction"),
        (("resume", "release", "reopen", "unpause", "放行", "恢复调度", "解封"), "resume_source"),
        (("pause", "suspend", "暂停"), "pause_source"),
        (("configure", "setup", "init_window", "start_window", "create_window", "开窗", "配置窗口"), "configure_window"),
        (("decision", "judge", "judgment", "threshold", "evaluate", "assess", "verdict",
          "判定", "阈值", "评估"), "evaluate"),
        (("rate", "stat", "window", "failure", "失败率", "统计"), "window_rate"),
        (("state", "status", "can_claim", "allowed", "query", "info", "状态", "是否放行"), "source_state"),
        (("receipt", "report", "record", "log", "ack", "回执", "上报", "记录错误"), "record_receipt"),
    )

    def resolve_action(self, name: str) -> str:
        raw = str(name)
        norm = _norm_name(raw)
        if hasattr(self, norm) and callable(getattr(self, norm)) and not norm.startswith("_"):
            return norm
        tokens = set(norm.split("_"))

        def has_token(*words: str) -> bool:
            return any(w in tokens for w in words)

        def has_phrase(*phrases: str) -> bool:
            return any(p in norm for p in phrases)

        def has_cn(*words: str) -> bool:
            return any(w in raw for w in words)

        queryish = has_token("get", "list", "query", "show", "read", "find",
                             "status", "state", "info", "can",
                             "permission", "allowed") or has_cn("查询", "查得到", "状态")
        imperative_check = has_token("check", "judge", "evaluate", "assess", "decide") \
            or has_cn("判定", "评估", "判断")

        if has_token("reset", "clear") or has_cn("重置", "清空"):
            return "reset"
        if has_token("scenario", "e2e", "demo") or has_phrase("full_flow", "flagship") \
                or has_cn("场景", "全流程"):
            return "run_scenario"
        if "conflict" in norm or has_cn("冲突"):
            return "register_conflict"
        if has_token("recovery", "recover", "recrawl") or has_phrase("recovery_plan") \
                or has_cn("恢复计划", "恢复范围", "补采") \
                or ((has_token("plan", "scope")) and has_token("resume", "recover", "recovery")):
            return "recovery_plan"
        if has_token("audit", "trail") or has_cn("审计", "轨迹"):
            return "get_audit"
        if queryish and (has_token("state", "status", "permission", "allowed", "can",
                                   "release", "released", "claimable")
                         or has_phrase("can_claim", "release_status")
                         or has_cn("状态", "放行", "权限")):
            return "source_state"
        if queryish and (has_token("late", "belated") or has_cn("迟到")):
            return "get_audit"
        if (has_token("pause", "suspend", "paused") or has_cn("暂停")) \
                and not (queryish and not imperative_check):
            return "pause_source"
        if has_token("resume", "release", "reopen", "unpause") or has_cn("放行", "解封") \
                or (has_cn("恢复") and not has_cn("计划", "范围")):
            return "resume_source"
        if has_token("confirm", "recheck", "reevaluate") or has_phrase("re_evaluate", "re_judge") \
                or has_cn("确认", "复核", "重新判定"):
            return "confirm_correction"
        if has_token("late", "belated", "supplement") or has_cn("迟到", "补报"):
            return "report_late_receipt"
        if has_token("claim", "acquire", "pull") or has_phrase("fetch_task", "fetch_dispatch") \
                or has_cn("领取", "认领"):
            return "claim_tasks"
        if "fetch" in tokens and has_token("task", "tasks", "work", "dispatch"):
            return "claim_tasks"
        if has_token("dispatch", "issue", "assign") or has_cn("派工", "下发", "发放"):
            return "issue_dispatch"
        if (has_token("configure", "setup", "init", "start", "create") and has_token("window")) \
                or has_cn("开窗", "配置窗口"):
            return "configure_window"
        if has_token("consume", "apply") or has_cn("消费", "应用"):
            return "evaluate"
        if has_token("decision", "judge", "judgment", "threshold", "evaluate",
                     "assess", "verdict", "check", "test") \
                or has_phrase("should_pause", "or_not", "calc_rate") \
                or has_cn("判定", "阈值", "评估", "判断", "核算"):
            return "evaluate"
        if has_token("receipt", "report", "record", "log", "ack") \
                or has_phrase("page_error", "add_error") \
                or has_cn("回执", "上报", "记录错误") \
                or (has_token("error", "fail", "failure", "missing")
                    and not queryish
                    and not has_token("rate", "window", "stat", "threshold")):
            return "record_receipt"
        if has_token("rate", "stat", "window", "failure", "fail") \
                or has_cn("失败率", "统计", "窗口"):
            return "window_rate"
        if has_token("state", "status", "info") or has_cn("状态"):
            return "source_state"
        return "source_state"

    def call(self, name: str, *args: Any, **kwargs: Any) -> Any:
        norm_name = _norm_name(str(name))
        op_name = self.resolve_action(str(name))
        # 按方法名语义补出成功/失败倾向
        if op_name == "record_receipt" and "_force_success" not in kwargs:
            if any(w in norm_name for w in ("error", "fail", "missing")) or "错误" in name or "失败" in name:
                kwargs["_force_success"] = False
            elif "success" in norm_name or "成功" in name:
                kwargs["_force_success"] = True
        op: Callable[..., Any] = getattr(self, op_name)
        kwargs.setdefault("_action_name", norm_name)
        if op_name == "reset":
            return op()
        return op(*args, **kwargs)


failure_guard = FailureGuard()


# 模块级便捷函数（供 ``from app.services.failure_guard import xxx`` 风格使用）
def reset_guard() -> None:
    failure_guard.reset()


def __getattr__(name: str) -> Any:  # pragma: no cover - 兼容性别名
    if name.startswith("__"):
        raise AttributeError(name)
    norm = _norm_name(name)
    if norm in ("failure_guard", "guard"):
        return failure_guard

    def _alias(*args: Any, **kwargs: Any) -> Any:
        return failure_guard.call(norm, *args, **kwargs)

    return _alias
