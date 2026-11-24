"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from types import SimpleNamespace

from app.routers import resource
from app.routers import chains
from app.routers import failure_guard_api

# failure_guard_api.router 含静态前缀路由，必须排在 resource 前面，
# 否则 resource 的 GET /api/{module}/{entry_id} 会先以 int 校验拦截 /audit 等段。
# fallback_router 含宽松的 /api/{module}/{action} 兜底，必须放在最后。
ROUTERS = [
    SimpleNamespace(router=failure_guard_api.router),
    resource,
    chains,
    SimpleNamespace(router=failure_guard_api.fallback_router),
]
