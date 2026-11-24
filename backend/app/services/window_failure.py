"""窗口失败率数据的通用资源服务（供 /api/window_failure_rate 等键查询）。"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.failure_guard import failure_guard


class WindowFailureRateService(BaseModuleService):
    MODULE = "window_failure_rate"
    ENTITY = "窗口失败率"
    LIST_FIELDS = ["窗口标识", "来源站点", "总页数", "失败页数", "失败率", "阈值", "是否暂停"]
    REQUIRED_FIELDS = ["窗口标识", "来源站点"]
    STATUS_ORDER = ["已放行", "已暂停"]
    ACTION_RULES = {}
    NEGATIVE_ACTIONS = []

    def domain_rate(self, *a, **kw):
        return failure_guard.window_rate(*a, **kw)

    def domain_evaluate(self, *a, **kw):
        return failure_guard.evaluate(*a, **kw)

    def __getattr__(self, name: str):
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)

        def _route(*args, **kwargs):
            return failure_guard.call(name, *args, **kwargs)

        return _route


class ThresholdDecisionService(BaseModuleService):
    MODULE = "threshold_decision"
    ENTITY = "阈值判定"
    LIST_FIELDS = ["decision_id", "source", "window_id", "failure_rate", "threshold", "action"]
    REQUIRED_FIELDS = ["source"]
    STATUS_ORDER = ["暂停", "放行"]
    ACTION_RULES = {}


class DispatchEntryService(BaseModuleService):
    MODULE = "dispatch_entry"
    ENTITY = "派工入口"
    LIST_FIELDS = ["dispatch_id", "source", "window_id", "kind", "status", "task_count"]
    REQUIRED_FIELDS = ["source"]
    STATUS_ORDER = ["已派出", "已拦截"]
    ACTION_RULES = {}


class PausePlanService(BaseModuleService):
    MODULE = "pause_plan"
    ENTITY = "暂停计划"
    LIST_FIELDS = ["plan_id", "source", "window_id", "status"]
    REQUIRED_FIELDS = ["source"]
    STATUS_ORDER = ["已发出", "已解除"]
    ACTION_RULES = {}


class RecoveryPlanService(BaseModuleService):
    MODULE = "recovery_plan"
    ENTITY = "恢复计划"
    LIST_FIELDS = ["plan_id", "source", "window_id", "page_count", "status"]
    REQUIRED_FIELDS = ["source"]
    STATUS_ORDER = ["待领取", "无需补采"]
    ACTION_RULES = {}
