from celery import current_task
import pandas as pd
from sqlalchemy.orm import Session
from app.tasks.celery_app import celery_app
from app.models.database import SessionLocal, RespiratoryMortality
from app.services.ml_service import MLService
from app.services.data_processor import DataProcessor
from typing import Dict, Any, Optional
from loguru import logger
import traceback
from datetime import datetime

@celery_app.task(bind=True)
def train_model_task(
    self,
    model_type: str = "random_forest",
    hyperparameters: Optional[Dict[str, Any]] = None,
    experiment_name: str = "respiratory_mortality_prediction"
):
    """Background task for training ML models"""
    
    db = SessionLocal()
    
    try:
        # Update task status
        current_task.update_state(
            state="PROGRESS",
            meta={"status": "Loading data", "progress": 10}
        )
        
        # Load data from database
        query = db.query(RespiratoryMortality).filter(
            RespiratoryMortality.deaths.isnot(None),
            RespiratoryMortality.population.isnot(None)
        )
        
        df = pd.read_sql(query.statement, db.bind)
        logger.info(f"Loaded {len(df)} records for training")
        
        if len(df) < 1000:
            raise ValueError("Insufficient data for training")
        
        current_task.update_state(
            state="PROGRESS", 
            meta={"status": "Processing data", "progress": 30}
        )
        
        # Initialize services
        data_processor = DataProcessor()
        ml_service = MLService()
        
        # Feature engineering
        df = data_processor.engineer_features(df)
        
        current_task.update_state(
            state="PROGRESS",
            meta={"status": "Preparing model data", "progress": 50}
        )
        
        # Prepare data for modeling
        X, y = data_processor.prepare_model_data(df, fit_encoders=True)
        
        # Save preprocessors
        data_processor.save_preprocessors("models/preprocessors")
        
        current_task.update_state(
            state="PROGRESS",
            meta={"status": "Training model", "progress": 70}
        )
        
        # Train model
        model, metrics, run_id = ml_service.train_model(
            X, y, model_type, hyperparameters, experiment_name
        )
        
        current_task.update_state(
            state="PROGRESS",
            meta={"status": "Saving model", "progress": 90}
        )
        
        # Save model
        model_name = f"{model_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        model_id = ml_service.save_model(
            model, model_name, model_type, metrics, 
            hyperparameters or {}, run_id, db
        )
        
        result = {
            "model_id": model_id,
            "model_name": model_name,
            "metrics": metrics,
            "mlflow_run_id": run_id,
            "training_status": "completed",
            "records_trained": len(df)
        }
        
        logger.info(f"Training completed for {model_name}")
        
        return result
        
    except Exception as e:
        error_msg = f"Training failed: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        
        current_task.update_state(
            state="FAILURE",
            meta={"error": error_msg, "traceback": traceback.format_exc()}
        )
        
        raise
        
    finally:
        db.close()

@celery_app.task(bind=True)
def hyperparameter_optimization_task(
    self,
    model_type: str = "random_forest"
):
    """Background task for hyperparameter optimization"""
    
    db = SessionLocal()
    
    try:
        current_task.update_state(
            state="PROGRESS",
            meta={"status": "Loading data for optimization", "progress": 20}
        )
        
        query = db.query(RespiratoryMortality).filter(
            RespiratoryMortality.deaths.isnot(None),
            RespiratoryMortality.population.isnot(None)
        )
        
        df = pd.read_sql(query.statement, db.bind)
        
        data_processor = DataProcessor()
        ml_service = MLService()
        
        # Feature engineering and data preparation
        df = data_processor.engineer_features(df)
        X, y = data_processor.prepare_model_data(df, fit_encoders=True)
        
        current_task.update_state(
            state="PROGRESS",
            meta={"status": "Running hyperparameter optimization", "progress": 50}
        )
        
        # Perform hyperparameter tuning
        best_params, best_score = ml_service.hyperparameter_tuning(X, y, model_type)
        
        current_task.update_state(
            state="PROGRESS",
            meta={"status": "Training optimized model", "progress": 80}
        )
        
        # Train model with best parameters
        model, metrics, run_id = ml_service.train_model(
            X, y, model_type, best_params, f"optimized_{model_type}"
        )
        
        model_name = f"optimized_{model_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        model_id = ml_service.save_model(
            model, model_name, model_type, metrics, best_params, run_id, db
        )
        
        result = {
            "model_id": model_id,
            "model_name": model_name,
            "best_parameters": best_params,
            "best_cv_score": best_score,
            "final_metrics": metrics,
            "optimization_status": "completed"
        }
        
        logger.info(f"Hyperparameter optimization completed for {model_type}")
        
        return result
        
    except Exception as e:
        error_msg = f"Hyperparameter optimization failed: {str(e)}"
        logger.error(error_msg)
        
        current_task.update_state(
            state="FAILURE",
            meta={"error": error_msg}
        )
        
        raise
        
    finally:
        db.close()

@celery_app.task
def cleanup_old_models():
    """Clean up old inactive models"""
    
    db = SessionLocal()
    
    try:
        from datetime import timedelta
        cutoff_date = datetime.now() - timedelta(days=30)
        
        # Remove old inactive models
        old_models = db.query(MLModel).filter(
            MLModel.is_active == False,
            MLModel.created_at < cutoff_date
        ).all()
        
        for model in old_models:
            # Remove model file
            import os
            model_path = f"models/{model.model_name}.joblib"
            if os.path.exists(model_path):
                os.remove(model_path)
            
            # Remove database record
            db.delete(model)
        
        db.commit()
        
        logger.info(f"Cleaned up {len(old_models)} old models")
        
        return {"cleaned_models": len(old_models)}
        
    except Exception as e:
        logger.error(f"Cleanup task failed: {e}")
        raise
        
    finally:
        db.close()