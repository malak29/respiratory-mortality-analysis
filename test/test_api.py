import pytest
from fastapi.testclient import TestClient
from app.models.database import RespiratoryMortality, MLModel
import json

class TestMortalityAPI:
    
    def test_health_check(self, client):
        """Test basic health check endpoint"""
        response = client.get("/api/v1/health/")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data
        assert "version" in data

    def test_get_mortality_data_empty(self, client):
        """Test getting mortality data when database is empty"""
        response = client.get("/api/v1/mortality/data")
        assert response.status_code == 200
        assert response.json() == []

    def test_get_mortality_data_with_records(self, client, test_db, sample_mortality_data):
        """Test getting mortality data with records in database"""
        # Create test record
        record = RespiratoryMortality(**sample_mortality_data)
        test_db.add(record)
        test_db.commit()
        
        response = client.get("/api/v1/mortality/data")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["county"] == "Test County"

    def test_get_mortality_data_with_filters(self, client, test_db, sample_mortality_data):
        """Test mortality data filtering"""
        # Create test records
        record1 = RespiratoryMortality(**sample_mortality_data)
        
        record2_data = sample_mortality_data.copy()
        record2_data["state"] = "New York"
        record2_data["gender"] = "Female"
        record2 = RespiratoryMortality(**record2_data)
        
        test_db.add_all([record1, record2])
        test_db.commit()
        
        # Test state filter
        response = client.get("/api/v1/mortality/data?state=California")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["state"] == "California"
        
        # Test gender filter
        response = client.get("/api/v1/mortality/data?gender=Female")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["gender"] == "Female"

    def test_get_summary_statistics(self, client, test_db, sample_mortality_data):
        """Test summary statistics endpoint"""
        # Create test records
        record = RespiratoryMortality(**sample_mortality_data)
        test_db.add(record)
        test_db.commit()
        
        response = client.get("/api/v1/mortality/statistics/summary")
        assert response.status_code == 200
        data = response.json()
        
        assert "total_records" in data
        assert "total_deaths" in data
        assert "gender_distribution" in data
        assert data["total_records"] == 1
        assert data["total_deaths"] == 25

    def test_prediction_no_active_model(self, client):
        """Test prediction when no active model exists"""
        prediction_data = {
            "county": "Test County",
            "ten_year_age_groups": "65-74 years",
            "gender": "Male",
            "year": 2020,
            "population": 10000,
            "state": "California"
        }
        
        response = client.post("/api/v1/mortality/predict", json=prediction_data)
        assert response.status_code == 404
        assert "No active model found" in response.json()["detail"]

    def test_list_models_empty(self, client):
        """Test listing models when none exist"""
        response = client.get("/api/v1/models/")
        assert response.status_code == 200
        assert response.json() == []

    def test_get_model_not_found(self, client):
        """Test getting non-existent model"""
        response = client.get("/api/v1/models/999")
        assert response.status_code == 404
        assert response.json()["detail"] == "Model not found"

class TestModelAPI:
    
    def test_list_models_with_data(self, client, test_db):
        """Test listing models with existing models"""
        # Create test model
        model = MLModel(
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
        
        test_db.add(model)
        test_db.commit()
        
        response = client.get("/api/v1/models/")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["model_name"] == "test_model"

    def test_activate_model(self, client, test_db):
        """Test activating a model"""
        # Create test model
        model = MLModel(
            model_name="test_model",
            model_version="1.0",
            model_type="random_forest",
            accuracy=0.75,
            precision=0.73,
            recall=0.77,
            f1_score=0.75,
            auc_score=0.82,
            is_active=False,
            mlflow_run_id="test_run_123",
            hyperparameters='{"n_estimators": 100}'
        )
        
        test_db.add(model)
        test_db.commit()
        model_id = model.id
        
        response = client.put(f"/api/v1/models/{model_id}/activate")
        assert response.status_code == 200
        
        # Verify model is activated
        test_db.refresh(model)
        assert model.is_active == True

    def test_start_training_insufficient_data(self, client):
        """Test training with insufficient data"""
        training_data = {
            "model_type": "random_forest",
            "experiment_name": "test_experiment"
        }
        
        response = client.post("/api/v1/models/train", json=training_data)
        assert response.status_code == 400
        assert "Insufficient data" in response.json()["detail"]

    def test_invalid_model_type(self, client):
        """Test training with invalid model type"""
        training_data = {
            "model_type": "invalid_model",
            "experiment_name": "test_experiment"
        }
        
        response = client.post("/api/v1/models/train", json=training_data)
        assert response.status_code == 400
        assert "Invalid model type" in response.json()["detail"]