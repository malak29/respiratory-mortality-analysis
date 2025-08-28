import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, classification_report
import pandas as pd
import numpy as np
from typing import Dict, Any, Tuple, Optional
import joblib
import os
from datetime import datetime
from sqlalchemy.orm import Session
from app.models.database import MLModel
from app.core.config import settings
from loguru import logger
import json

class MLService:
    def __init__(self):
        mlflow.set_tracking_uri(settings.MLFLOW_TRACKING_URI)
        self.models = {
            'random_forest': RandomForestClassifier,
            'logistic_regression': LogisticRegression
        }
        self.active_model = None
        self.active_model_name = None
        
    def train_model(
        self, 
        X_train: pd.DataFrame, 
        y_train: pd.Series, 
        model_type: str = 'random_forest',
        hyperparameters: Optional[Dict] = None,
        experiment_name: str = "respiratory_mortality_prediction"
    ) -> Tuple[Any, Dict[str, float]]:
        """Train and evaluate ML model with MLflow tracking"""
        
        mlflow.set_experiment(experiment_name)
        
        default_params = {
            'random_forest': {
                'n_estimators': 100,
                'max_depth': 10,
                'min_samples_split': 5,
                'random_state': 42
            },
            'logistic_regression': {
                'random_state': 42,
                'max_iter': 1000
            }
        }
        
        if hyperparameters is None:
            hyperparameters = default_params.get(model_type, {})
        
        X_train_split, X_val, y_train_split, y_val = train_test_split(
            X_train, y_train, test_size=0.2, random_state=42
        )
        
        with mlflow.start_run(run_name=f"{model_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
            model_class = self.models[model_type]
            model = model_class(**hyperparameters)
            
            model.fit(X_train_split, y_train_split)
            
            y_pred = model.predict(X_val)
            y_pred_proba = model.predict_proba(X_val)[:, 1] if hasattr(model, 'predict_proba') else None
            
            metrics = {
                'accuracy': accuracy_score(y_val, y_pred),
                'precision': precision_score(y_val, y_pred, average='weighted'),
                'recall': recall_score(y_val, y_pred, average='weighted'),
                'f1_score': f1_score(y_val, y_pred, average='weighted'),
                'auc_score': roc_auc_score(y_val, y_pred_proba) if y_pred_proba is not None else 0.0
            }
            
            mlflow.log_params(hyperparameters)
            mlflow.log_metrics(metrics)
            mlflow.sklearn.log_model(model, "model")
            
            run_id = mlflow.active_run().info.run_id
            
            logger.info(f"Model {model_type} trained with metrics: {metrics}")
            
            return model, metrics, run_id
    
    def hyperparameter_tuning(
        self, 
        X_train: pd.DataFrame, 
        y_train: pd.Series, 
        model_type: str = 'random_forest'
    ) -> Tuple[Dict, float]:
        """Perform hyperparameter tuning using GridSearchCV"""
        
        param_grids = {
            'random_forest': {
                'n_estimators': [50, 100, 200],
                'max_depth': [5, 10, 15, None],
                'min_samples_split': [2, 5, 10],
                'min_samples_leaf': [1, 2, 4]
            },
            'logistic_regression': {
                'C': [0.1, 1.0, 10.0],
                'penalty': ['l1', 'l2'],
                'solver': ['liblinear', 'saga']
            }
        }
        
        model_class = self.models[model_type]
        base_model = model_class(random_state=42)
        
        grid_search = GridSearchCV(
            base_model,
            param_grids[model_type],
            cv=5,
            scoring='f1_weighted',
            n_jobs=-1
        )
        
        grid_search.fit(X_train, y_train)
        
        logger.info(f"Best parameters for {model_type}: {grid_search.best_params_}")
        logger.info(f"Best cross-validation score: {grid_search.best_score_}")
        
        return grid_search.best_params_, grid_search.best_score_
    
    def save_model(
        self, 
        model: Any, 
        model_name: str, 
        model_type: str, 
        metrics: Dict[str, float],
        hyperparameters: Dict,
        mlflow_run_id: str,
        db: Session,
        model_path: str = "models"
    ):
        """Save model to filesystem and database"""
        
        os.makedirs(model_path, exist_ok=True)
        
        model_file_path = os.path.join(model_path, f"{model_name}.joblib")
        joblib.dump(model, model_file_path)
        
        # Deactivate previous models of same type
        db.query(MLModel).filter(
            MLModel.model_type == model_type,
            MLModel.is_active == True
        ).update({MLModel.is_active: False})
        
        # Save new model info to database
        db_model = MLModel(
            model_name=model_name,
            model_version="1.0",
            model_type=model_type,
            accuracy=metrics.get('accuracy', 0.0),
            precision=metrics.get('precision', 0.0),
            recall=metrics.get('recall', 0.0),
            f1_score=metrics.get('f1_score', 0.0),
            auc_score=metrics.get('auc_score', 0.0),
            is_active=True,
            mlflow_run_id=mlflow_run_id,
            hyperparameters=json.dumps(hyperparameters)
        )
        
        db.add(db_model)
        db.commit()
        db.refresh(db_model)
        
        logger.info(f"Model {model_name} saved successfully")
        
        return db_model.id
    
    def load_active_model(self, model_type: str, db: Session, model_path: str = "models"):
        """Load the active model of specified type"""
        
        db_model = db.query(MLModel).filter(
            MLModel.model_type == model_type,
            MLModel.is_active == True
        ).first()
        
        if not db_model:
            logger.warning(f"No active {model_type} model found")
            return None
        
        model_file_path = os.path.join(model_path, f"{db_model.model_name}.joblib")
        
        if not os.path.exists(model_file_path):
            logger.error(f"Model file not found: {model_file_path}")
            return None
        
        model = joblib.load(model_file_path)
        self.active_model = model
        self.active_model_name = db_model.model_name
        
        logger.info(f"Loaded active model: {db_model.model_name}")
        
        return model
    
    def predict(self, features: pd.DataFrame, return_probabilities: bool = False) -> np.ndarray:
        """Make predictions using the active model"""
        
        if self.active_model is None:
            raise ValueError("No active model loaded. Please load a model first.")
        
        predictions = self.active_model.predict(features)
        
        if return_probabilities and hasattr(self.active_model, 'predict_proba'):
            probabilities = self.active_model.predict_proba(features)
            return predictions, probabilities
        
        return predictions
    
    def get_feature_importance(self) -> Optional[Dict[str, float]]:
        """Get feature importance from the active model"""
        
        if self.active_model is None:
            return None
        
        if hasattr(self.active_model, 'feature_importances_'):
            # For tree-based models
            feature_names = ['county', 'ten_year_age_groups', 'gender', 'year', 'population', 'state']
            importance_dict = dict(zip(feature_names, self.active_model.feature_importances_))
            return importance_dict
        elif hasattr(self.active_model, 'coef_'):
            # For linear models
            feature_names = ['county', 'ten_year_age_groups', 'gender', 'year', 'population', 'state']
            coefficients = self.active_model.coef_[0] if len(self.active_model.coef_.shape) > 1 else self.active_model.coef_
            importance_dict = dict(zip(feature_names, np.abs(coefficients)))
            return importance_dict
        
        return None