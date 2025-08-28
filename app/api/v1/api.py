from fastapi import APIRouter
from app.api.v1.endpoints import mortality, models, health

api_router = APIRouter()

api_router.include_router(
    mortality.router,
    prefix="/mortality",
    tags=["mortality"]
)

api_router.include_router(
    models.router,
    prefix="/models", 
    tags=["models"]
)

api_router.include_router(
    health.router,
    prefix="/health",
    tags=["health"]
)