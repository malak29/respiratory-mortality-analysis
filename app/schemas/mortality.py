from pydantic import BaseModel, validator
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum

class GenderEnum(str, Enum):
    male = "Male"
    female = "Female"

class AgeGroupEnum(str, Enum):
    age_45_54 = "45-54 years"
    age_55_64 = "55-64 years" 
    age_65_74 = "65-74 years"
    age_75_84 = "75-84 years"
    age_85_plus = "85+ years"

class MortalityCategoryEnum(str, Enum):
    very_low = "Very Low"
    low = "Low"
    medium = "Medium"
    high = "High"
    very_high = "Very High"

class PopulationDensityEnum(str, Enum):
    low = "Low Density"
    medium = "Medium Density"
    high = "High Density"

class MortalityDataCreate(BaseModel):
    county: str
    ten_year_age_groups: AgeGroupEnum
    gender: GenderEnum
    year: int
    icd_10_113_cause_list: str
    deaths: int
    population: int
    crude_rate: Optional[str] = None
    state: str

    @validator('year')
    def validate_year(cls, v):
        if v < 1999 or v > 2030:
            raise ValueError('Year must be between 1999 and 2030')
        return v

    @validator('deaths')
    def validate_deaths(cls, v):
        if v < 0:
            raise ValueError('Deaths must be non-negative')
        return v

    @validator('population')
    def validate_population(cls, v):
        if v <= 0:
            raise ValueError('Population must be positive')
        return v

class MortalityDataResponse(BaseModel):
    id: int
    county: str
    ten_year_age_groups: str
    gender: str
    year: int
    icd_10_113_cause_list: str
    deaths: int
    population: int
    crude_rate: Optional[str]
    state: str
    mortality_rate: Optional[float]
    high_mortality: Optional[bool]
    is_male: Optional[bool]
    mortality_category: Optional[str]
    population_density: Optional[str]
    is_west_coast: Optional[bool]
    is_east_coast: Optional[bool]
    created_at: datetime
    updated_at: Optional[datetime]

    class Config:
        from_attributes = True

class PredictionRequest(BaseModel):
    county: str
    ten_year_age_groups: AgeGroupEnum
    gender: GenderEnum
    year: int
    population: int
    state: str

class PredictionResponse(BaseModel):
    prediction: int
    probability: float
    risk_level: str
    features_used: Dict[str, Any]
    model_name: str
    prediction_timestamp: datetime

class BatchPredictionRequest(BaseModel):
    data: List[PredictionRequest]

class BatchPredictionResponse(BaseModel):
    predictions: List[PredictionResponse]
    total_processed: int

class ModelMetrics(BaseModel):
    accuracy: float
    precision: float
    recall: float
    f1_score: float
    auc_score: float

class ModelInfo(BaseModel):
    id: int
    model_name: str
    model_version: str
    model_type: str
    metrics: ModelMetrics
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True

class TrainingRequest(BaseModel):
    model_type: str = "random_forest"
    hyperparameters: Optional[Dict[str, Any]] = None
    experiment_name: str = "respiratory_mortality_prediction"

class TrainingResponse(BaseModel):
    model_id: int
    model_name: str
    metrics: ModelMetrics
    mlflow_run_id: str
    training_status: str