#!/usr/bin/env python3

import asyncio
import pandas as pd
from sqlalchemy.orm import Session
from app.models.database import SessionLocal, MLModel, RespiratoryMortality
from app.services.ml_service import MLService
from app.services.data_processor import DataProcessor
import argparse
import sys
from loguru import logger
import json
import mlflow
from datetime import datetime, timedelta
import os
import shutil

class ModelManager:
    def __init__(self):
        self.ml_service = MLService()
        self.data_processor = DataProcessor()
    
    async def retrain_models(self, model_types: List[str] = None, use_optimization: bool = False):
        """Retrain models with latest data"""
        
        if model_types is None:
            model_types = ['random_forest', 'logistic_regression']
        
        db = SessionLocal()
        
        try:
            # Load latest data
            query = db.query(RespiratoryMortality).filter(
                RespiratoryMortality.deaths.isnot(None),
                RespiratoryMortality.population.isnot(None)
            )
            
            df = pd.read_sql(query.statement, db.bind)
            logger.info(f"Loaded {len(df)} records for retraining")
            
            if len(df) < 1000:
                logger.error("Insufficient data for retraining")
                return False
            
            # Feature engineering
            df = self.data_processor.engineer_features(df)
            X, y = self.data_processor.prepare_model_data(df, fit_encoders=True)
            
            # Save updated preprocessors
            os.makedirs("models/preprocessors", exist_ok=True)
            self.data_processor.save_preprocessors("models/preprocessors")
            
            results = {}
            
            for model_type in model_types:
                logger.info(f"Retraining {model_type} model...")
                
                try:
                    # Hyperparameter optimization if requested
                    hyperparameters = None
                    if use_optimization:
                        logger.info(f"Running hyperparameter optimization for {model_type}")
                        hyperparameters, _ = self.ml_service.hyperparameter_tuning(X, y, model_type)
                    
                    # Train model
                    model, metrics, run_id = self.ml_service.train_model(
                        X, y, model_type, hyperparameters, f"retrain_{model_type}"
                    )
                    
                    # Save model
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    model_name = f"retrained_{model_type}_{timestamp}"
                    
                    model_id = self.ml_service.save_model(
                        model, model_name, model_type, metrics,
                        hyperparameters or {}, run_id, db
                    )
                    
                    results[model_type] = {
                        'model_id': model_id,
                        'model_name': model_name,
                        'metrics': metrics,
                        'success': True
                    }
                    
                    logger.info(f"Successfully retrained {model_type}: {metrics}")
                    
                except Exception as e:
                    logger.error(f"Failed to retrain {model_type}: {e}")
                    results[model_type] = {'success': False, 'error': str(e)}
            
            return results
            
        except Exception as e:
            logger.error(f"Retraining failed: {e}")
            return False
            
        finally:
            db.close()
    
    async def compare_models(self):
        """Compare performance of all models"""
        
        db = SessionLocal()
        
        try:
            models = db.query(MLModel).order_by(MLModel.created_at.desc()).all()
            
            if not models:
                logger.info("No models found in database")
                return
            
            logger.info("Model Performance Comparison:")
            logger.info("-" * 80)
            logger.info(f"{'Model Name':<30} {'Type':<15} {'Accuracy':<10} {'F1-Score':<10} {'Active':<8}")
            logger.info("-" * 80)
            
            for model in models:
                logger.info(
                    f"{model.model_name:<30} "
                    f"{model.model_type:<15} "
                    f"{model.accuracy:<10.4f} "
                    f"{model.f1_score:<10.4f} "
                    f"{'Yes' if model.is_active else 'No':<8}"
                )
            
            # Find best model by F1 score
            best_model = max(models, key=lambda m: m.f1_score)
            logger.info(f"\nBest performing model: {best_model.model_name} (F1: {best_model.f1_score:.4f})")
            
        except Exception as e:
            logger.error(f"Model comparison failed: {e}")
            
        finally:
            db.close()
    
    async def cleanup_old_models(self, retention_days: int = 30, keep_best: int = 3):
        """Clean up old models while keeping the best performers"""
        
        db = SessionLocal()
        
        try:
            cutoff_date = datetime.now() - timedelta(days=retention_days)
            
            # Get old models
            old_models = db.query(MLModel).filter(
                MLModel.created_at < cutoff_date,
                MLModel.is_active == False
            ).all()
            
            # Group by model type and keep best performers
            model_types = {}
            for model in old_models:
                if model.model_type not in model_types:
                    model_types[model.model_type] = []
                model_types[model.model_type].append(model)
            
            deleted_count = 0
            
            for model_type, models in model_types.items():
                # Sort by F1 score and keep best
                models.sort(key=lambda m: m.f1_score, reverse=True)
                models_to_delete = models[keep_best:]
                
                for model in models_to_delete:
                    # Remove model file
                    model_path = f"models/{model.model_name}.joblib"
                    if os.path.exists(model_path):
                        os.remove(model_path)
                        logger.info(f"Deleted model file: {model_path}")
                    
                    # Remove database record
                    db.delete(model)
                    deleted_count += 1
            
            db.commit()
            logger.info(f"Cleaned up {deleted_count} old models")
            
        except Exception as e:
            logger.error(f"Model cleanup failed: {e}")
            
        finally:
            db.close()
    
    async def export_model_artifacts(self, output_dir: str):
        """Export all model artifacts for backup"""
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Copy models directory
        if os.path.exists("models"):
            shutil.copytree("models", os.path.join(output_dir, "models"), dirs_exist_ok=True)
        
        # Export model metadata
        db = SessionLocal()
        
        try:
            models = db.query(MLModel).all()
            
            model_metadata = []
            for model in models:
                model_metadata.append({
                    'model_name': model.model_name,
                    'model_type': model.model_type,
                    'accuracy': model.accuracy,
                    'precision': model.precision,
                    'recall': model.recall,
                    'f1_score': model.f1_score,
                    'auc_score': model.auc_score,
                    'is_active': model.is_active,
                    'created_at': model.created_at.isoformat(),
                    'hyperparameters': model.hyperparameters
                })
            
            # Save metadata
            with open(os.path.join(output_dir, "model_metadata.json"), 'w') as f:
                json.dump(model_metadata, f, indent=2)
            
            logger.info(f"Exported {len(models)} model artifacts to {output_dir}")
            
        finally:
            db.close()

async def main():
    parser = argparse.ArgumentParser(description='Model management utility')
    parser.add_argument('action', choices=['retrain', 'compare', 'cleanup', 'export'])
    parser.add_argument('--model-types', nargs='+', default=['random_forest', 'logistic_regression'])
    parser.add_argument('--optimize', action='store_true', help='Use hyperparameter optimization')
    parser.add_argument('--retention-days', type=int, default=30)
    parser.add_argument('--keep-best', type=int, default=3)
    parser.add_argument('--output-dir', default='model_artifacts')
    
    args = parser.parse_args()
    
    manager = ModelManager()
    
    if args.action == 'retrain':
        await manager.retrain_models(args.model_types, args.optimize)
    elif args.action == 'compare':
        await manager.compare_models()
    elif args.action == 'cleanup':
        await manager.cleanup_old_models(args.retention_days, args.keep_best)
    elif args.action == 'export':
        await manager.export_model_artifacts(args.output_dir)

if __name__ == "__main__":
    asyncio.run(main())