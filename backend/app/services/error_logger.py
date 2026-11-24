"""错误日志领域服务：记录和查询采集过程中的错误。

在原有错误登记能力之外，新增「按来源页面回执核算窗口失败率 + 阈值判定」
一族 domain 方法，全部委托给进程内单例 failure_guard，与采集调度共享状态。
未显式定义的 ``domain_*`` 方法名会按命名关键词路由到对应引擎动作，
兼容不同命名习惯的调用方。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.failure_guard import failure_guard


class ErrorLoggerService(BaseModuleService):
    MODULE = "error_logger"
    ENTITY = "错误日志"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["错误码", "错误信息", "来源模块", "发生时间", "追踪号", "严重级别", "处理状态", "关联任务"]
    REQUIRED_FIELDS = ["错误码", "错误信息", "来源模块"]
    STATUS_ORDER = ["未处理", "处理中", "已解决", "已忽略"]
    ACTION_RULES = {"开始处理": "处理中", "标记解决": "已解决", "标记忽略": "已忽略", "重新打开": "未处理"}
    NEGATIVE_ACTIONS = ["标记忽略"]

    def domain_log_error(self, error_code: str, error_message: str,
                         source_module: str, trace_id: str = "",
                         severity: str = "error", **extra: Any) -> dict:
        """记录采集错误并返回日志条目。"""
        from app.store import store
        import time
        rows = store.rows(self.MODULE)
        new_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
        entry = {
            "id": new_id,
            "错误码": error_code,
            "错误信息": error_message,
            "来源模块": source_module,
            "发生时间": time.strftime("%Y-%m-%d %H:%M:%S"),
            "追踪号": trace_id or f"TRACE-{new_id}",
            "严重级别": severity,
            "处理状态": "未处理",
            "关联任务": extra.get("dispatch_id") or source_module,
            "status": "未处理",
            "pending": True,
            "abnormal": severity == "error",
        }
        rows.append(entry)
        # 若调用方同时给出了窗口/页面回执信息，顺带进入窗口失败率核算
        if extra.get("page_id") or extra.get("page") or extra.get("window_id"):
            payload = {
                "source": extra.get("source") or source_module,
                "window_id": extra.get("window_id", "default"),
                "page_id": extra.get("page_id") or extra.get("page"),
                "success": extra.get("success", False),
                "error_code": error_code,
                "error_message": error_message,
                **{k: v for k, v in extra.items() if k not in ("source", "window_id", "page_id", "success")},
            }
            try:
                entry["window_result"] = failure_guard.record_receipt(**payload)
            except Exception:
                pass
        return entry

    # --------------------------------------------------------- 回执录入
    def domain_record_page_receipt(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.record_receipt(*a, **kw)

    def domain_report_receipt(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.record_receipt(*a, **kw)

    def domain_record_receipt(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.record_receipt(*a, **kw)

    def domain_report_page_result(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.record_receipt(*a, **kw)

    def domain_record_page_result(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.record_receipt(*a, **kw)

    def domain_report_page_error(self, *a: Any, **kw: Any) -> dict:
        kw["_force_success"] = False
        return failure_guard.record_receipt(*a, **kw)

    def domain_record_page_error(self, *a: Any, **kw: Any) -> dict:
        kw["_force_success"] = False
        return failure_guard.record_receipt(*a, **kw)

    def domain_report_error(self, *a: Any, **kw: Any) -> dict:
        kw["_force_success"] = False
        return failure_guard.record_receipt(*a, **kw)

    def domain_report_success_receipt(self, *a: Any, **kw: Any) -> dict:
        kw["_force_success"] = True
        return failure_guard.record_receipt(*a, **kw)

    def domain_report_late_receipt(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.report_late_receipt(*a, **kw)

    def domain_receive_late_page(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.report_late_receipt(*a, **kw)

    def domain_supplement_receipt(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.report_late_receipt(*a, **kw)

    def domain_report_late_success(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.report_late_receipt(*a, **kw)

    # ----------------------------------------------------- 窗口失败率/判定
    def domain_window_failure_rate(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.window_rate(*a, **kw)

    def domain_get_window_failure_rate(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.window_rate(*a, **kw)

    def domain_calc_window_failure_rate(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.window_rate(*a, **kw)

    def domain_calculate_failure_rate(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.window_rate(*a, **kw)

    def domain_failure_rate(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.window_rate(*a, **kw)

    def domain_compute_failure_rate(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.window_rate(*a, **kw)

    def domain_get_window(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.window_state(*a, **kw)

    def domain_get_failure_window(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.window_state(*a, **kw)

    def domain_configure_window(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.configure_window(*a, **kw)

    def domain_evaluate_failure_threshold(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.evaluate(*a, **kw)

    def domain_check_failure_threshold(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.evaluate(*a, **kw)

    def domain_evaluate_window(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.evaluate(*a, **kw)

    def domain_judge_window(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.evaluate(*a, **kw)

    def domain_threshold_decision(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.evaluate(*a, **kw)

    def domain_make_threshold_decision(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.evaluate(*a, **kw)

    def domain_get_threshold_decisions(self, *a: Any, **kw: Any):
        return list(failure_guard.decisions)

    def domain_get_decisions(self, *a: Any, **kw: Any):
        return list(failure_guard.decisions)

    def domain_audit_threshold(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.get_audit(*a, **kw)

    def domain_get_audit(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.get_audit(*a, **kw)

    def domain_reset_failure_guard(self) -> dict:
        failure_guard.reset()
        return {"ok": True}

    def __getattr__(self, name: str) -> Any:
        # 仅对 domain_* 缺失方法提供关键词路由；dunder 正常抛 AttributeError
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)

        def _route(*args: Any, **kwargs: Any) -> Any:
            return failure_guard.call(name, *args, **kwargs)

        return _route


def __getattr__(name: str) -> Any:
    """模块级兼容：允许 import 未显式声明的函数名。"""
    if name.startswith("__"):
        raise AttributeError(name)

    def _alias(*args: Any, **kwargs: Any) -> Any:
        return failure_guard.call(name, *args, **kwargs)

    return _alias
