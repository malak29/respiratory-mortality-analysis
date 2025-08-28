from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
import pandas as pd
from datetime import datetime

from app.models.database import get_db, MLModel, PredictionLog
from app.schemas.mortality import (
    ModelInfo, TrainingRequest, TrainingResponse, ModelMetrics
)
from app.services.ml_service import MLService
from app.services.data_processor import DataProcessor
from app.tasks.training_tasks import train_model_task
from app.core.logging_config import get_logger

logger = get_logger(__name__)
router = APIRouter()

@router.get("/", response_model=List[ModelInfo])
async def list_models(
    model_type: Optional[str] = None,
    is_active: Optional[bool] = None,
    db: Session = Depends(get_db)
):
    """List all trained models with filtering options"""
    
    query = db.query(MLModel)
    
    if model_type:
        query = query.filter(MLModel.model_type == model_type)
    if is_active is not None:
        query = query.filter(MLModel.is_active == is_active)
    
    models = query.order_by(MLModel.created_at.desc()).all()
    
    return [
        ModelInfo(
            id=model.id,
            model_name=model.model_name,
            model_version=model.model_version,
            model_type=model.model_type,
            metrics=ModelMetrics(
                accuracy=model.accuracy,
                precision=model.precision,
                recall=model.recall,
                f1_score=model.f1_score,
                auc_score=model.auc_score
            ),
            is_active=model.is_active,
            created_at=model.created_at
        ) for model in models
    ]

@router.get("/{model_id}", response_model=ModelInfo)
async def get_model(
    model_id: int,
    db: Session = Depends(get_db)
):
    """Get specific model information"""
    
    model = db.query(MLModel).filter(MLModel.id == model_id).first()
    
    if not model:
        raise HTTPException(status_code=404, detail="Model not found")
    
    return ModelInfo(
        id=model.id,
        model_name=model.model_name,
        model_version=model.model_version,
        model_type=model.model_type,
        metrics=ModelMetrics(
            accuracy=model.accuracy,
            precision=model.precision,
            recall=model.recall,
            f1_score=model.f1_score,
            auc_score=model.auc_score
        ),
        is_active=model.is_active,
        created_at=model.created_at
    )

@router.post("/train", response_model=TrainingResponse)
async def start_model_training(
    request: TrainingRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Start asynchronous model training"""
    
    if request.model_type not in ['random_forest', 'logistic_regression']:
        raise HTTPException(
            status_code=400,
            detail="Invalid model type. Must be 'random_forest' or 'logistic_regression'"
        )
    
    # Check if there's enough data for training
    data_count = db.query(func.count(RespiratoryMortality.id)).scalar()
    
    if data_count < 1000:
        raise HTTPException(
            status_code=400,
            detail="Insufficient data for training. Minimum 1000 records required."
        )
    
    # Start background training task
    task_result = train_model_task.delay(
        model_type=request.model_type,
        hyperparameters=request.hyperparameters,
        experiment_name=request.experiment_name
    )
    
    logger.info(f"Started training task {task_result.id} for model type {request.model_type}")
    
    return TrainingResponse(
        model_id=0,  # Will be updated when training completes
        model_name=f"{request.model_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        metrics=ModelMetrics(
            accuracy=0.0,
            precision=0.0,
            recall=0.0,
            f1_score=0.0,
            auc_score=0.0
        ),
        mlflow_run_id="",
        training_status="started"
    )

@router.put("/{model_id}/activate")
async def activate_model(
    model_id: int,
    db: Session = Depends(get_db)
):
    """Activate a specific model"""
    
    model = db.query(MLModel).filter(MLModel.id == model_id).first()
    
    if not model:
        raise HTTPException(status_code=404, detail="Model not found")
    
    # Deactivate other models of same type
    db.query(MLModel).filter(
        MLModel.model_type == model.model_type,
        MLModel.is_active == True
    ).update({MLModel.is_active: False})
    
    # Activate selected model
    model.is_active = True
    db.commit()
    
    logger.info(f"Activated model {model.model_name}")
    
    return {"message": f"Model {model.model_name} activated successfully"}

@router.get("/{model_id}/feature-importance")
async def get_feature_importance(
    model_id: int,
    db: Session = Depends(get_db)
):
    """Get feature importance for a specific model"""
    
    model_info = db.query(MLModel).filter(MLModel.id == model_id).first()
    
    if not model_info:
        raise HTTPException(status_code=404, detail="Model not found")
    
    try:
        ml_service = MLService()
        model = ml_service.load_active_model(model_info.model_type, db)
        
        if model is None:
            raise HTTPException(status_code=404, detail="Model file not found")
        
        importance = ml_service.get_feature_importance()
        
        if importance is None:
            raise HTTPException(
                status_code=400,
                detail="Feature importance not available for this model type"
            )
        
        # Sort by importance
        sorted_importance = sorted(
            importance.items(),
            key=lambda x: x[1],
            reverse=True
        )
        
        return {
            "model_name": model_info.model_name,
            "feature_importance": [
                {
                    "feature": feature,
                    "importance": round(importance_value, 4)
                } for feature, importance_value in sorted_importance
            ]
        }
        
    except Exception as e:
        logger.error(f"Error getting feature importance: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/predictions/logs")
async def get_prediction_logs(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    model_id: Optional[int] = Query(None),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    db: Session = Depends(get_db)
):
    """Get prediction logs with filtering"""
    
    query = db.query(PredictionLog)
    
    if model_id:
        query = query.filter(PredictionLog.model_id == model_id)
    if start_date:
        query = query.filter(PredictionLog.created_at >= start_date)
    if end_date:
        query = query.filter(PredictionLog.created_at <= end_date)
    
    logs = query.order_by(PredictionLog.created_at.desc()).offset(skip).limit(limit).all()
    
    return {
        "logs": [
            {
                "id": log.id,
                "model_id": log.model_id,
                "prediction": log.prediction,
                "prediction_probability": log.prediction_probability,
                "execution_time_ms": log.execution_time_ms,
                "created_at": log.created_at
            } for log in logs
        ]
    }