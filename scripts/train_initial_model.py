#!/usr/bin/env python3

import asyncio
import pandas as pd
from sqlalchemy.orm import Session
from app.models.database import SessionLocal, RespiratoryMortality
from app.services.ml_service import MLService
from app.services.data_processor import DataProcessor
from loguru import logger
import sys
import os

async def train_initial_model():
    """Train an initial model for the production system"""
    
    logger.info("Starting initial model training...")
    
    db = SessionLocal()
    
    try:
        # Load data from database
        query = db.query(RespiratoryMortality).filter(
            RespiratoryMortality.deaths.isnot(None),
            RespiratoryMortality.population.isnot(None)
        )
        
        df = pd.read_sql(query.statement, db.bind)
        logger.info(f"Loaded {len(df)} records for training")
        
        if len(df) < 1000:
            logger.error("Insufficient data for training. Need at least 1000 records.")
            return False
        
        # Initialize services
        data_processor = DataProcessor()
        ml_service = MLService()
        
        # Prepare data
        logger.info("Preparing data for modeling...")
        df = data_processor.engineer_features(df)
        X, y = data_processor.prepare_model_data(df, fit_encoders=True)
        
        # Save preprocessors
        os.makedirs("models/preprocessors", exist_ok=True)
        data_processor.save_preprocessors("models/preprocessors")
        
        # Train Random Forest model
        logger.info("Training Random Forest model...")
        model, metrics, run_id = ml_service.train_model(
            X, y, 
            model_type="random_forest",
            experiment_name="initial_production_model"
        )
        
        # Save model
        model_name = "production_random_forest_v1"
        model_id = ml_service.save_model(
            model, model_name, "random_forest", metrics,
            {"n_estimators": 100, "max_depth": 10, "random_state": 42},
            run_id, db
        )
        
        logger.info(f"Model training completed successfully!")
        logger.info(f"Model ID: {model_id}")
        logger.info(f"Model Name: {model_name}")
        logger.info(f"Accuracy: {metrics['accuracy']:.4f}")
        logger.info(f"F1 Score: {metrics['f1_score']:.4f}")
        
        # Train Logistic Regression model for comparison
        logger.info("Training Logistic Regression model...")
        lr_model, lr_metrics, lr_run_id = ml_service.train_model(
            X, y,
            model_type="logistic_regression", 
            experiment_name="initial_production_model"
        )
        
        lr_model_name = "production_logistic_regression_v1"
        lr_model_id = ml_service.save_model(
            lr_model, lr_model_name, "logistic_regression", lr_metrics,
            {"random_state": 42, "max_iter": 1000},
            lr_run_id, db
        )
        
        logger.info(f"Logistic Regression model training completed!")
        logger.info(f"LR Model ID: {lr_model_id}")
        logger.info(f"LR Accuracy: {lr_metrics['accuracy']:.4f}")
        
        return True
        
    except Exception as e:
        logger.error(f"Model training failed: {e}")
        return False
        
    finally:
        db.close()

if __name__ == "__main__":
    success = asyncio.run(train_initial_model())
    sys.exit(0 if success else 1)