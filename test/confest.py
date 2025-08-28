import pytest
import asyncio
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi.testclient import TestClient
import pandas as pd
from app.main import app
from app.models.database import Base, get_db
from app.core.config import settings
import os
import tempfile

# Test database URL
TEST_DATABASE_URL = "sqlite:///./test_respiratory.db"

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="session")
def test_engine():
    """Create test database engine"""
    engine = create_engine(TEST_DATABASE_URL, connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    yield engine
    Base.metadata.drop_all(bind=engine)

@pytest.fixture(scope="function")
def test_db(test_engine):
    """Create test database session"""
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)
    
    connection = test_engine.connect()
    transaction = connection.begin()
    session = TestingSessionLocal(bind=connection)
    
    yield session
    
    session.close()
    transaction.rollback()
    connection.close()

@pytest.fixture(scope="function")
def client(test_db):
    """Create test client"""
    def override_get_db():
        try:
            yield test_db
        finally:
            test_db.close()
    
    app.dependency_overrides[get_db] = override_get_db
    
    with TestClient(app) as test_client:
        yield test_client
    
    app.dependency_overrides.clear()

@pytest.fixture
def sample_mortality_data():
    """Sample mortality data for testing"""
    return {
        "county": "Test County",
        "ten_year_age_groups": "65-74 years",
        "gender": "Male",
        "year": 2020,
        "icd_10_113_cause_list": "Chronic lower respiratory diseases",
        "deaths": 25,
        "population": 10000,
        "crude_rate": "250.0",
        "state": "California"
    }

@pytest.fixture
def sample_dataframe():
    """Sample DataFrame for testing"""
    return pd.DataFrame([
        {
            "county": "Los Angeles County",
            "ten_year_age_groups": "75-84 years",
            "gender": "Female",
            "year": 2019,
            "icd_10_113_cause_list": "Chronic lower respiratory diseases",
            "deaths": 150,
            "population": 50000,
            "state": "California"
        },
        {
            "county": "Cook County",
            "ten_year_age_groups": "65-74 years", 
            "gender": "Male",
            "year": 2019,
            "icd_10_113_cause_list": "Pneumonia",
            "deaths": 75,
            "population": 30000,
            "state": "Illinois"
        }
    ])

@pytest.fixture
def mock_trained_model():
    """Mock trained model for testing"""
    class MockModel:
        def predict(self, X):
            return [1] * len(X)
        
        def predict_proba(self, X):
            return [[0.3, 0.7]] * len(X)
        
        @property
        def feature_importances_(self):
            return [0.4, 0.3, 0.1, 0.1, 0.05, 0.05]
    
    return MockModel()

@pytest.fixture(scope="session")
def temp_model_dir():
    """Temporary directory for model files"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir