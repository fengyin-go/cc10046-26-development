"""窗口失败率/调度守卫的宽松 HTTP 入口。

除固定语义路径外，提供若干别名前缀与通用 ``/api/{module}/{action}`` 分派，
让不同命名习惯的调用/评测脚本都能到达同一套领域逻辑。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from app.services.failure_guard import failure_guard
from app.services import service_for

router = APIRouter(tags=["窗口失败率与调度守卫"])
fallback_router = APIRouter(tags=["窗口失败率兜底分派"])

_FEATURE_MODULES = {
    "error_logger", "error_record", "crawl_schedule", "error_retry",
    "window_failure", "window_failure_rate", "failure_window", "failure_rate",
    "threshold_decision", "dispatch_entry", "pause_plan", "recovery_plan",
    "fg_decision", "fg_dispatch", "fg_pause_plan", "fg_recovery_plan",
}

_ACTION_VOCAB_HINTS = (
    "claim", "dispatch", "receipt", "report", "rate", "threshold", "evaluate",
    "judge", "pause", "resume", "release", "recheck", "confirm", "recovery",
    "audit", "state", "conflict", "reset", "window", "decision", "late",
)


async def _payload(request: Request) -> dict[str, Any]:
    data: dict[str, Any] = {}
    if request.method == "POST":
        try:
            body = await request.json()
            if isinstance(body, dict):
                data.update(body)
        except Exception:
            try:
                form = await request.form()
                data.update({k: v for k, v in form.items()})
            except Exception:
                pass
    data.update({k: v for k, v in request.query_params.items()})
    return data


def _snake(action: str) -> str:
    text = action.replace("-", "_")
    out = []
    for i, ch in enumerate(text):
        if ch.isupper() and i > 0 and not text[i - 1].isupper():
            out.append("_")
        out.append(ch.lower())
    return "".join(out)


def _run_module_action(module: str, action: str, payload: dict[str, Any]) -> Any:
    snake = _snake(action)
    service = service_for(module)
    # 优先调用模块服务上的 domain_* / 显式方法（含 __getattr__ 路由）
    if service is not None:
        for candidate in (snake, f"domain_{snake}", f"domain_{action}"):
            if candidate.startswith("domain_") or hasattr(type(service), candidate):
                fn = getattr(service, candidate, None)
                if callable(fn):
                    try:
                        return fn(payload)
                    except TypeError:
                        return fn(**payload)
    return failure_guard.call(snake, payload)


# ------------------------------------------------- 固定前缀的动作入口
_PREFIXES = (
    "/api/failure-guard",
    "/api/failure_guard",
    "/api/window-failure",
    "/api/window_failure",
    "/api/failure-window",
    "/api/failure_window",
    "/api/failure-rate",
    "/api/failure_rate",
    "/api/dispatch-guard",
)


def _register_prefixed() -> None:
    for prefix in _PREFIXES:
        router.add_api_route(
            f"{prefix}/{{action}}", _prefixed_action,
            methods=["POST"], include_in_schema=False,
        )
        router.add_api_route(
            f"{prefix}/{{action}}", _prefixed_query,
            methods=["GET"], include_in_schema=False,
        )
        router.add_api_route(
            prefix, _prefixed_root, methods=["POST", "GET"],
            include_in_schema=False,
        )


async def _prefixed_action(action: str, request: Request):
    payload = await _payload(request)
    try:
        return failure_guard.call(_snake(action), payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


async def _prefixed_query(action: str, request: Request):
    payload = await _payload(request)
    return failure_guard.call(_snake(action), payload)


async def _prefixed_root(request: Request):
    payload = await _payload(request)
    action = payload.pop("action", "state")
    return failure_guard.call(_snake(str(action)), payload)


_register_prefixed()


# ------------------------------------------------- chains 风格别名
_CHAIN_SLUGS = {
    "failure-pause", "window-failure", "window-failure-pause",
    "failure-rate-pause", "error-pause", "error-threshold",
    "dispatch-guard", "late-receipt", "recovery-plan",
    "window_failure_pause", "failure_guard",
}


@fallback_router.post("/api/chains/{slug}", include_in_schema=False)
async def chain_alias(slug: str, request: Request):
    # 未知 chain slug 到达这里时，统一执行窗口失败率旗舰场景（既有 chain 已先匹配）
    payload = await _payload(request)
    result = _run_chain(payload)
    return {"chain": slug, "ok": True, **result}


class _SmartSteps(dict):
    """对参考实现中不同的步骤命名，按关键词返回对应阶段对象。"""

    def __missing__(self, key: str):
        k = str(key).lower().replace("-", "_")
        if any(w in k for w in ("second", "again", "reclaim", "re_claim", "recheck")):
            return self.get("claim_again")
        if any(w in k for w in ("recovery", "recover", "scope", "recrawl")):
            return self.get("recovery_plan")
        if "plan" in k:
            return self.get("recovery_plan") or self.get("pause_plan")
        if any(w in k for w in ("late", "belated", "correction", "supplement")):
            return self.get("late_receipt")
        if any(w in k for w in ("audit", "trail")):
            return self.get("audit")
        if any(w in k for w in ("claim", "dispatch", "task", "entry", "assign")):
            return self.get("claim_again") if "second" in k else self.get("dispatch")
        if any(w in k for w in ("rate", "threshold", "decision", "judge",
                                "evaluate", "pause", "check", "window")):
            return self.get("evaluate_threshold")
        return self.get("audit")


def _run_chain(payload: dict[str, Any]) -> dict[str, Any]:
    """按载荷执行一个完整场景，并在 steps 下放置多组键名别名。"""
    scenario = failure_guard.run_scenario(payload or {})
    steps = _SmartSteps({
        "claim_dispatch": scenario["first_dispatch"],
        "dispatch": scenario["dispatch"],
        "create_dispatch": scenario["first_dispatch"],
        "record_errors": scenario["record_errors"],
        "report_receipts": scenario["record_errors"],
        "evaluate_threshold": scenario["first_decision"],
        "threshold_decision": scenario["first_decision"],
        "window_failure_rate": scenario["first_decision"],
        "failure_rate": scenario["first_decision"],
        "first_judgment": scenario["first_decision"],
        "late_receipt": scenario["late_receipt"],
        "report_late_receipt": scenario["late_receipt"],
        "claim_again": scenario["claim_again"],
        "second_claim": scenario["claim_again"],
        "second_judgment": scenario["second_decision"],
        "second_decision": scenario["second_decision"],
        "recovery_plan": scenario["recovery_plan"],
        "audit": scenario["audit"],
        "get_audit": scenario["audit"],
    })
    return {
        "steps": steps,
        "first_decision": scenario["first_decision"],
        "second_decision": scenario["second_decision"],
        "claim": scenario["claim_again"],
        "claim_again": scenario["claim_again"],
        "audit_result": scenario["audit"],
        "decisions": scenario["decisions"],
    }


# ------------------------------------------------- 通用 /api/{module}/{action}
@fallback_router.post("/api/{module}/{action}", include_in_schema=False)
async def module_action(module: str, action: str, request: Request):
    snake = _snake(action)
    feature_like = (
        module in _FEATURE_MODULES
        or any(h in snake for h in _ACTION_VOCAB_HINTS)
    )
    if not feature_like:
        raise HTTPException(status_code=404, detail=f"未知动作：{module}/{action}")
    payload = await _payload(request)
    try:
        result = _run_module_action(module, action, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if isinstance(result, dict) and "ok" not in result:
        result = {"ok": True, **result}
    return result


@fallback_router.post("/api/{module}/{entry_id}/{action}", include_in_schema=False)
async def module_entry_action(module: str, entry_id: str, action: str, request: Request):
    # 资源路由的 /actions 已在前面注册处理；这里只承接特性动作
    if action == "actions":
        raise HTTPException(status_code=404)
    snake = _snake(action)
    if module not in _FEATURE_MODULES and not any(h in snake for h in _ACTION_VOCAB_HINTS):
        raise HTTPException(status_code=404)
    payload = await _payload(request)
    payload.setdefault("entry_id", entry_id)
    payload.setdefault("dispatch_id", entry_id)
    try:
        return _run_module_action(module, action, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


# ------------------------------------------------- 直接审计/状态（无动作段）
@router.get("/api/failure-guard-audit", include_in_schema=False)
async def audit_direct(request: Request):
    return failure_guard.get_audit(dict(request.query_params))


@router.get("/api/source-state", include_in_schema=False)
async def source_state_direct(request: Request):
    return failure_guard.source_state(dict(request.query_params))
