from fastapi.routing import APIRouter

from knowledge_complex_backend.web.api import (
    complexity,
    echo,
    gpc,
    info,
    monitoring,
    redis,
)

api_router = APIRouter()
api_router.include_router(monitoring.router)
api_router.include_router(echo.router, prefix="/echo", tags=["echo"])
api_router.include_router(redis.router, prefix="/redis", tags=["redis"])
api_router.include_router(complexity.router, prefix="/complexity", tags=["complexity"])
api_router.include_router(gpc.router, prefix="/gpc", tags=["gpc"])
api_router.include_router(info.router, prefix="/info", tags=["info"])
