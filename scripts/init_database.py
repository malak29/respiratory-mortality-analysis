#!/usr/bin/env python3

import asyncio
import pandas as pd
from sqlalchemy.orm import Session
from app.models.database import engine, Base, SessionLocal
from app.services.data_processor import DataProcessor
from app.core.config import settings
from loguru import logger
import sys
import os

async def init_database():
    """Initialize database with sample data and setup"""
    
    logger.info("Initializing database...")
    
    # Create tables
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created")
    
    # Check if we have a data file to load
    data_file = os.getenv("INIT_DATA_FILE", "data/final_dataset_cleaned.csv")
    
    if not os.path.exists(data_file):
        logger.warning(f"Data file {data_file} not found. Skipping data import.")
        return
    
    db = SessionLocal()
    
    try:
        # Check if data already exists
        from app.models.database import RespiratoryMortality
        existing_count = db.query(RespiratoryMortality).count()
        
        if existing_count > 0:
            logger.info(f"Database already contains {existing_count} records. Skipping import.")
            return
        
        # Load and process data
        logger.info(f"Loading data from {data_file}")
        data_processor = DataProcessor()
        
        df = data_processor.load_raw_data(data_file)
        df = data_processor.engineer_features(df)
        
        # Save to database
        data_processor.save_to_database(df, db)
        
        logger.info(f"Successfully imported {len(df)} records")
        
        # Create default model entry (placeholder)
        default_model = MLModel(
            model_name="default_random_forest",
            model_version="1.0",
            model_type="random_forest",
            accuracy=0.0,
            precision=0.0,
            recall=0.0,
            f1_score=0.0,
            auc_score=0.0,
            is_active=False,
            mlflow_run_id="",
            hyperparameters='{"n_estimators": 100, "random_state": 42}'
        )
        
        db.add(default_model)
        db.commit()
        
        logger.info("Database initialization completed successfully")
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        db.rollback()
        raise
        
    finally:
        db.close()

if __name__ == "__main__":
    asyncio.run(init_database())