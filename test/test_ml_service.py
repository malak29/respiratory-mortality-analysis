import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from app.services.ml_service import MLService

class TestMLService:
    
    @pytest.fixture
    def ml_service(self):
        return MLService()
    
    @pytest.fixture
    def sample_training_data(self):
        """Sample data for ML training"""
        np.random.seed(42)
        n_samples = 1000
        
        X = pd.DataFrame({
            'county': np.random.randint(0, 50, n_samples),
            'ten_year_age_groups': np.random.randint(0, 5, n_samples),
            'gender': np.random.randint(0, 2, n_samples),
            'year': np.random.randint(0, 22, n_samples),
            'population': np.random.randn(n_samples),
            'state': np.random.randint(0, 51, n_samples)
        })
        
        y = pd.Series(np.random.randint(0, 2, n_samples))
        
        return X, y

    @patch('mlflow.start_run')
    @patch('mlflow.log_params')
    @patch('mlflow.log_metrics')
    @patch('mlflow.sklearn.log_model')
    def test_train_model_random_forest(
        self, 
        mock_log_model,
        mock_log_metrics,
        mock_log_params,
        mock_start_run,
        ml_service, 
        sample_training_data
    ):
        """Test Random Forest model training"""
        X, y = sample_training_data
        
        # Mock MLflow run context
        mock_run = MagicMock()
        mock_run.info.run_id = "test_run_123"
        mock_start_run.return_value.__enter__.return_value = mock_run
        
        model, metrics, run_id = ml_service.train_model(X, y, "random_forest")
        
        # Check model is trained
        assert model is not None
        assert hasattr(model, 'predict')
        
        # Check metrics are calculated
        assert 'accuracy' in metrics
        assert 'precision' in metrics
        assert 'recall' in metrics
        assert 'f1_score' in metrics
        assert 'auc_score' in metrics
        
        # Check MLflow logging
        mock_log_params.assert_called_once()
        mock_log_metrics.assert_called_once()
        mock_log_model.assert_called_once()

    def test_hyperparameter_tuning(self, ml_service, sample_training_data):
        """Test hyperparameter optimization"""
        X, y = sample_training_data
        
        best_params, best_score = ml_service.hyperparameter_tuning(X, y, "random_forest")
        
        assert isinstance(best_params, dict)
        assert isinstance(best_score, float)
        assert 'n_estimators' in best_params
        assert 'max_depth' in best_params

    def test_predict_no_model_loaded(self, ml_service, sample_training_data):
        """Test prediction without loading model"""
        X, _ = sample_training_data
        
        with pytest.raises(ValueError, match="No active model loaded"):
            ml_service.predict(X.head())

    @patch('joblib.load')
    def test_load_active_model_success(self, mock_load, ml_service, test_db, mock_trained_model):
        """Test successful model loading"""
        # Create mock model in database
        from app.models.database import MLModel
        
        db_model = MLModel(
            model_name="test_model",
            model_version="1.0",
            model_type="random_forest",
            accuracy=0.75,
            precision=0.73,
            recall=0.77,
            f1_score=0.75,
            auc_score=0.82,
            is_active=True,
            mlflow_run_id="test_run_123",
            hyperparameters='{"n_estimators": 100}'
        )
        
        test_db.add(db_model)
        test_db.commit()
        
        # Mock file loading
        mock_load.return_value = mock_trained_model
        
        # Mock file existence
        with patch('os.path.exists', return_value=True):
            loaded_model = ml_service.load_active_model("random_forest", test_db)
        
        assert loaded_model is not None
        assert ml_service.active_model == mock_trained_model

    def test_get_feature_importance(self, ml_service, mock_trained_model):
        """Test getting feature importance"""
        ml_service.active_model = mock_trained_model
        
        importance = ml_service.get_feature_importance()
        
        assert isinstance(importance, dict)
        assert len(importance) == 6  # Number of features
        
        # Check all importance values are present
        for feature in ['county', 'ten_year_age_groups', 'gender', 'year', 'population', 'state']:
            assert feature in importance

    def test_predict_with_probabilities(self, ml_service, mock_trained_model, sample_training_data):
        """Test prediction with probability output"""
        X, _ = sample_training_data
        ml_service.active_model = mock_trained_model
        
        predictions, probabilities = ml_service.predict(X.head(), return_probabilities=True)
        
        assert len(predictions) == 5
        assert len(probabilities) == 5
        assert all(pred in [0, 1] for pred in predictions)