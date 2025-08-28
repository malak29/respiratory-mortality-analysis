from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime
import psutil
import os
from typing import Dict, Any

from app.models.database import get_db
from app.core.config import settings
from app.core.logging_config import get_logger

logger = get_logger(__name__)
router = APIRouter()

@router.get("/")
async def health_check():
    """Basic health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow(),
        "version": settings.VERSION,
        "environment": settings.ENVIRONMENT
    }

@router.get("/detailed")
async def detailed_health_check(db: Session = Depends(get_db)) -> Dict[str, Any]:
    """Detailed health check including database and system metrics"""
    
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow(),
        "version": settings.VERSION,
        "environment": settings.ENVIRONMENT,
        "checks": {}
    }
    
    # Database health check
    try:
        db.execute(text("SELECT 1"))
        health_status["checks"]["database"] = {
            "status": "healthy",
            "response_time_ms": 0  # You can measure actual response time
        }
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        health_status["checks"]["database"] = {
            "status": "unhealthy",
            "error": str(e)
        }
        health_status["status"] = "degraded"
    
    # System metrics
    try:
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        health_status["checks"]["system"] = {
            "status": "healthy",
            "cpu_percent": cpu_percent,
            "memory_percent": memory.percent,
            "disk_percent": disk.percent,
            "available_memory_gb": round(memory.available / (1024**3), 2)
        }
        
        # Alert if resources are low
        if cpu_percent > 80 or memory.percent > 80 or disk.percent > 90:
            health_status["checks"]["system"]["status"] = "warning"
            health_status["status"] = "degraded"
            
    except Exception as e:
        logger.error(f"System metrics check failed: {e}")
        health_status["checks"]["system"] = {
            "status": "unhealthy",
            "error": str(e)
        }
    
    # Model availability check
    try:
        from app.services.ml_service import MLService
        ml_service = MLService()
        model = ml_service.load_active_model("random_forest", db)
        
        health_status["checks"]["ml_model"] = {
            "status": "healthy" if model is not None else "unhealthy",
            "active_model": ml_service.active_model_name if model else None
        }
        
        if model is None:
            health_status["status"] = "degraded"
            
    except Exception as e:
        logger.error(f"ML model check failed: {e}")
        health_status["checks"]["ml_model"] = {
            "status": "unhealthy",
            "error": str(e)
        }
    
    return health_status

@router.get("/metrics")
async def get_system_metrics():
    """Get system performance metrics for monitoring"""
    
    try:
        # CPU and Memory metrics
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        
        # Process info
        process = psutil.Process(os.getpid())
        process_memory = process.memory_info()
        
        return {
            "system": {
                "cpu_percent": cpu_percent,
                "memory_total_gb": round(memory.total / (1024**3), 2),
                "memory_used_gb": round(memory.used / (1024**3), 2),
                "memory_percent": memory.percent
            },
            "process": {
                "memory_rss_mb": round(process_memory.rss / (1024**2), 2),
                "memory_vms_mb": round(process_memory.vms / (1024**2), 2),
                "cpu_percent": process.cpu_percent()
            },
            "timestamp": datetime.utcnow()
        }
        
    except Exception as e:
        logger.error(f"Error getting system metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to get system metrics")

@router.get("/readiness")
async def readiness_check(db: Session = Depends(get_db)):
    """Kubernetes readiness probe endpoint"""
    
    try:
        # Check database connection
        db.execute(text("SELECT 1"))
        
        # Check if at least one model is available
        from app.services.ml_service import MLService
        ml_service = MLService()
        model = ml_service.load_active_model("random_forest", db)
        
        if model is None:
            raise HTTPException(status_code=503, detail="No active model available")
        
        return {"status": "ready"}
        
    except Exception as e:
        logger.error(f"Readiness check failed: {e}")
        raise HTTPException(status_code=503, detail="Service not ready")

@router.get("/liveness")
async def liveness_check():
    """Kubernetes liveness probe endpoint"""
    return {"status": "alive", "timestamp": datetime.utcnow()}