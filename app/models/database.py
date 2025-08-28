from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.core.config import settings

engine = create_engine(settings.DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class RespiratoryMortality(Base):
    __tablename__ = "respiratory_mortality"
    
    id = Column(Integer, primary_key=True, index=True)
    county = Column(String, index=True)
    ten_year_age_groups = Column(String, index=True)
    gender = Column(String, index=True)
    year = Column(Integer, index=True)
    icd_10_113_cause_list = Column(Text)
    deaths = Column(Integer)
    population = Column(Integer)
    crude_rate = Column(String)
    state = Column(String, index=True)
    mortality_rate = Column(Float)
    high_mortality = Column(Boolean)
    is_male = Column(Boolean)
    mortality_category = Column(String)
    population_density = Column(String)
    is_west_coast = Column(Boolean, default=False)
    is_east_coast = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    __table_args__ = (
        Index('idx_state_year_gender', 'state', 'year', 'gender'),
        Index('idx_mortality_category', 'mortality_category'),
        Index('idx_age_gender', 'ten_year_age_groups', 'gender'),
    )

class MLModel(Base):
    __tablename__ = "ml_models"
    
    id = Column(Integer, primary_key=True, index=True)
    model_name = Column(String, unique=True, index=True)
    model_version = Column(String)
    model_type = Column(String)
    accuracy = Column(Float)
    precision = Column(Float)
    recall = Column(Float)
    f1_score = Column(Float)
    auc_score = Column(Float)
    is_active = Column(Boolean, default=False)
    mlflow_run_id = Column(String)
    hyperparameters = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class PredictionLog(Base):
    __tablename__ = "prediction_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    model_id = Column(Integer, index=True)
    input_features = Column(Text)
    prediction = Column(Float)
    prediction_probability = Column(Float)
    execution_time_ms = Column(Float)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()