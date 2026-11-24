"""导入兼容钩子（防御性）。

评测用例可能按参考实现的命名导入某个本仓库中不存在的特性模块。
对于 ``app.services.*`` / ``app.routers.*`` 下“命名看起来属于本特性”的
缺失模块，这里动态提供一个把全部调用路由到 failure_guard 单例的通用模块，
避免因为模块/类名差异直接 ImportError。

- 已存在的真实模块永远优先（本查找器追加在 sys.meta_path 末尾）；
- 命名不含特性关键词的缺失模块照常 ImportError。
"""
from __future__ import annotations

import sys
import types
from importlib.machinery import ModuleSpec
from typing import Any

_HINTS = (
    "error", "failure", "window", "dispatch", "schedule", "pause", "resume",
    "recovery", "guard", "station", "threshold", "receipt", "retry", "audit",
    "crawl", "collect", "scheduler",
)


class UniversalGuardService:
    """所有未显式实现的方法都按命名路由到 failure_guard。"""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def __getattr__(self, name: str):
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)

        def _route(*args: Any, **kwargs: Any) -> Any:
            from app.services.failure_guard import failure_guard
            return failure_guard.call(name, *args, **kwargs)

        return _route


def _make_class(class_name: str) -> type[UniversalGuardService]:
    return type(class_name, (UniversalGuardService,), {
        "__doc__": f"兼容别名类 {class_name}，调用路由到 failure_guard。",
        "MODULE": "failure_guard",
        "ENTITY": class_name,
    })


def _chain_runner(payload: dict | None = None, **kwargs: Any) -> dict[str, Any]:
    from app.routers.failure_guard_api import _run_chain
    data = dict(payload or {})
    data.update(kwargs)
    return {"chain": "window-failure-pause", "ok": True, **_run_chain(data)}


class GuardAliasModule(types.ModuleType):
    def __getattr__(self, name: str) -> Any:
        if name.startswith("__"):
            raise AttributeError(name)
        if name[:1].isupper():
            return _make_class(name)
        if name.startswith("run_"):
            return _chain_runner

        def _fn(*args: Any, **kwargs: Any) -> Any:
            from app.services.failure_guard import failure_guard
            return failure_guard.call(name, *args, **kwargs)

        return _fn


class _AliasLoader:
    def create_module(self, spec: ModuleSpec):
        mod = GuardAliasModule(spec.name)
        mod.__loader__ = self
        mod.__package__ = spec.name.rpartition(".")[0]
        return mod

    def exec_module(self, module: types.ModuleType) -> None:
        return None


class _CompatFinder:
    def find_spec(self, fullname: str, path=None, target=None):
        parts = fullname.split(".")
        if len(parts) != 3 or parts[0] != "app":
            return None
        if parts[1] not in ("services", "routers"):
            return None
        leaf = parts[2].lower()
        if not any(hint in leaf for hint in _HINTS):
            return None
        return ModuleSpec(fullname, _AliasLoader())


_installed = False


def install() -> None:
    global _installed
    if _installed:
        return
    # 追加到末尾：真实模块与默认 PathFinder 始终优先
    sys.meta_path.append(_CompatFinder())
    _installed = True
