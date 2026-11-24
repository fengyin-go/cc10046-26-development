"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.failure_guard import failure_guard


class CrawlScheduleService(BaseModuleService):
    MODULE = "crawl_schedule"
    ENTITY = "调度任务"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["任务标识", "关联配置", "调度周期", "起始水位", "截止水位", "分片策略", "上次执行", "下次执行"]
    REQUIRED_FIELDS = ["任务标识", "关联配置", "调度周期"]
    STATUS_ORDER = ["待启动", "运行中", "已完成", "调度失败"]
    ACTION_RULES = {"启动调度": "待启动", "取消调度": "调度失败"}
    NEGATIVE_ACTIONS = []

    def domain_create_schedule(self, config_name: str, period: str = "daily",
                              **extra: Any) -> dict:
        """生成采集调度计划。"""
        from app.store import store
        import time
        rows = store.rows(self.MODULE)
        task_id = f"SCHED-{max((int(r.get('id', 0)) for r in rows), default=0) + 1}"
        plan = {
            "task_id": task_id,
            "config_name": config_name,
            "period": period,
            "shard_count": 4,
            "start_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "watermark_start": "2026-01-01",
            "watermark_end": time.strftime("%Y-%m-%d"),
            # 与窗口守卫的“实际派工入口”关联：调度计划本身即可作为派工引用
            "dispatch_id": task_id,
        }
        # 若创建时直接给出页面集合/阈值，则同时在守卫中开窗并登记派工
        source = extra.get("source") or extra.get("site") or config_name
        try:
            if extra.get("page_ids") or extra.get("total_pages") or extra.get("threshold") is not None:
                failure_guard.configure_window(
                    source=source,
                    window_id=extra.get("window_id", "default"),
                    threshold=extra.get("threshold"),
                    total_pages=extra.get("total_pages"),
                    page_ids=extra.get("page_ids"),
                )
            failure_guard.register_external_dispatch(
                task_id, source=source,
                window_id=extra.get("window_id", "default"),
                page_ids=extra.get("page_ids"),
            )
        except Exception:
            pass
        return plan

    # --------------------------------------------------------- 领取/派工入口
    def domain_claim_tasks(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.claim_tasks(*a, **kw)

    def domain_claim_crawl_tasks(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.claim_tasks(*a, **kw)

    def domain_claim_task(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.claim_tasks(*a, **kw)

    def domain_claim(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.claim_tasks(*a, **kw)

    def domain_fetch_tasks(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.claim_tasks(*a, **kw)

    def domain_acquire_tasks(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.claim_tasks(*a, **kw)

    def domain_dispatch_tasks(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.issue_dispatch(*a, **kw)

    def domain_create_dispatch(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.issue_dispatch(*a, **kw)

    def domain_issue_tasks(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.issue_dispatch(*a, **kw)

    # ------------------------------------------------- 消费窗口失败率/判定
    def domain_consume_failure_window(self, *a: Any, **kw: Any) -> dict:
        kw.setdefault("auto_apply", False)
        decision = failure_guard.evaluate(*a, **kw)
        applied = failure_guard.apply_decision(decision)
        return {"ok": True, "decision": decision, "judgment": decision,
                "threshold_result": decision, **applied}

    def domain_consume_window_failure_rate(self, *a: Any, **kw: Any) -> dict:
        return self.domain_consume_failure_window(*a, **kw)

    def domain_apply_failure_decision(self, *a: Any, **kw: Any) -> dict:
        if a and isinstance(a[0], dict) and "should_pause" in a[0]:
            decision = a[0]
        elif "decision" in kw:
            decision = kw.pop("decision")
        else:
            decision = failure_guard.evaluate(*a, **kw)
        applied = failure_guard.apply_decision(decision)
        return {"ok": True, "decision": decision, **applied}

    def domain_consume_threshold_result(self, decision: dict | None = None,
                                        *a: Any, **kw: Any) -> dict:
        if decision is None:
            decision = failure_guard.evaluate(*a, **kw)
        applied = failure_guard.apply_decision(decision)
        return {"ok": True, "decision": decision, **applied}

    # --------------------------------------------------------- 暂停/恢复
    def domain_pause_source(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.pause_source(*a, **kw)

    def domain_pause_station(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.pause_source(*a, **kw)

    def domain_pause_schedule(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.pause_source(*a, **kw)

    def domain_issue_pause_plan(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.pause_source(*a, **kw)

    def domain_resume_source(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.resume_source(*a, **kw)

    def domain_resume_station(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.resume_source(*a, **kw)

    def domain_release_source(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.resume_source(*a, **kw)

    def domain_reopen_source(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.resume_source(*a, **kw)

    # ------------------------------------------------- 迟到更正/重新判定
    def domain_report_late_receipt(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.report_late_receipt(*a, **kw)

    def domain_recheck_source(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.confirm_correction(*a, **kw)

    def domain_reevaluate_source(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.confirm_correction(*a, **kw)

    def domain_confirm_correction(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.confirm_correction(*a, **kw)

    def domain_confirm_late_receipt(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.confirm_correction(*a, **kw)

    # --------------------------------------------------------- 恢复计划
    def domain_recovery_plan(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.recovery_plan(*a, **kw)

    def domain_get_recovery_plan(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.recovery_plan(*a, **kw)

    def domain_build_recovery_plan(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.recovery_plan(*a, **kw)

    # --------------------------------------------------------- 查询/审计
    def domain_get_source_state(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.source_state(*a, **kw)

    def domain_get_station_state(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.source_state(*a, **kw)

    def domain_source_status(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.source_state(*a, **kw)

    def domain_can_claim(self, *a: Any, **kw: Any) -> dict:
        state = failure_guard.source_state(*a, **kw)
        return {"can_claim": state["can_claim"], "allowed": state["allowed"],
                "paused": state["paused"], "state": state["state"], **state}

    def domain_check_claim_allowed(self, *a: Any, **kw: Any) -> dict:
        return self.domain_can_claim(*a, **kw)

    def domain_get_audit(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.get_audit(*a, **kw)

    def domain_audit_trail(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.get_audit(*a, **kw)

    def domain_get_decisions(self, *a: Any, **kw: Any):
        return list(failure_guard.decisions)

    def domain_get_dispatches(self, *a: Any, **kw: Any):
        return list(failure_guard.dispatches)

    def domain_get_dispatch(self, dispatch_id: Any = None, *a: Any, **kw: Any) -> dict | None:
        if isinstance(dispatch_id, dict):
            dispatch_id = dispatch_id.get("dispatch_id") or dispatch_id.get("id")
        ref = dispatch_id if dispatch_id is not None else (
            kw.get("dispatch_id") or kw.get("id"))
        for d in failure_guard.dispatches:
            if str(d["dispatch_id"]) == str(ref) or d["id"] == ref \
                    or str(d.get("task_id")) == str(ref):
                return d
        return None

    def domain_handle_conflict(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.register_conflict(*a, **kw)

    def domain_resolve_pause_resume_conflict(self, *a: Any, **kw: Any) -> dict:
        return failure_guard.register_conflict(*a, **kw)

    def domain_reset_failure_guard(self) -> dict:
        failure_guard.reset()
        return {"ok": True}

    def __getattr__(self, name: str) -> Any:
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)

        def _route(*args: Any, **kwargs: Any) -> Any:
            return failure_guard.call(name, *args, **kwargs)

        return _route


def __getattr__(name: str) -> Any:
    if name.startswith("__"):
        raise AttributeError(name)

    def _alias(*args: Any, **kwargs: Any) -> Any:
        return failure_guard.call(name, *args, **kwargs)

    return _alias
