import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sqlalchemy.orm import Session
from app.models.database import RespiratoryMortality
import joblib
import os
from loguru import logger

class DataProcessor:
    def __init__(self):
        self.scaler = StandardScaler()
        self.label_encoders = {}
        self.categorical_columns = [
            'county', 'ten_year_age_groups', 'gender', 
            'icd_10_113_cause_list', 'state'
        ]
        
    def load_raw_data(self, file_path: str) -> pd.DataFrame:
        """Load and validate raw CSV data"""
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Loaded {len(df)} records from {file_path}")
            return self._validate_data(df)
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise
    
    def _validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate data quality and handle missing values"""
        initial_count = len(df)
        
        df = df.dropna()
        final_count = len(df)
        
        if initial_count > final_count:
            logger.warning(f"Dropped {initial_count - final_count} rows with missing values")
        
        required_columns = [
            'county', 'ten_year_age_groups', 'gender', 'year',
            'icd_10_113_cause_list', 'deaths', 'population', 'state'
        ]
        
        missing_cols = set(required_columns) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        return df
    
    def engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply feature engineering transformations"""
        df = df.copy()
        
        df['mortality_rate'] = (df['deaths'] / df['population']) * 100000
        df['high_mortality'] = (df['mortality_rate'] > df['mortality_rate'].quantile(0.75)).astype(int)
        df['is_male'] = (df['gender'] == 'Male').astype(int)
        
        df['mortality_category'] = pd.qcut(
            df['mortality_rate'], 
            q=5, 
            labels=['Very Low', 'Low', 'Medium', 'High', 'Very High']
        )
        
        df['population_density'] = pd.qcut(
            df['population'], 
            q=3, 
            labels=['Low Density', 'Medium Density', 'High Density']
        )
        
        west_coast_states = ['California', 'Oregon', 'Washington']
        east_coast_states = ['New York', 'Massachusetts', 'Florida']
        
        df['is_west_coast'] = df['state'].isin(west_coast_states).astype(int)
        df['is_east_coast'] = df['state'].isin(east_coast_states).astype(int)
        
        return df
    
    def prepare_model_data(self, df: pd.DataFrame, fit_encoders: bool = True) -> Tuple[pd.DataFrame, pd.Series]:
        """Prepare data for machine learning models"""
        df_encoded = df.copy()
        
        if fit_encoders:
            for column in self.categorical_columns:
                if column in df_encoded.columns:
                    le = LabelEncoder()
                    df_encoded[column] = le.fit_transform(df_encoded[column])
                    self.label_encoders[column] = le
        else:
            for column in self.categorical_columns:
                if column in df_encoded.columns and column in self.label_encoders:
                    df_encoded[column] = self.label_encoders[column].transform(df_encoded[column])
        
        feature_columns = ['county', 'ten_year_age_groups', 'gender', 'year', 'population', 'state']
        X = df_encoded[feature_columns]
        
        if fit_encoders:
            X[['year', 'population']] = self.scaler.fit_transform(X[['year', 'population']])
        else:
            X[['year', 'population']] = self.scaler.transform(X[['year', 'population']])
        
        y = (df_encoded['deaths'] > df_encoded['deaths'].median()).astype(int)
        
        return X, y
    
    def save_preprocessors(self, path: str):
        """Save fitted preprocessors"""
        os.makedirs(path, exist_ok=True)
        
        joblib.dump(self.scaler, os.path.join(path, 'scaler.joblib'))
        joblib.dump(self.label_encoders, os.path.join(path, 'label_encoders.joblib'))
        
        logger.info(f"Preprocessors saved to {path}")
    
    def load_preprocessors(self, path: str):
        """Load fitted preprocessors"""
        self.scaler = joblib.load(os.path.join(path, 'scaler.joblib'))
        self.label_encoders = joblib.load(os.path.join(path, 'label_encoders.joblib'))
        
        logger.info(f"Preprocessors loaded from {path}")
    
    def save_to_database(self, df: pd.DataFrame, db: Session, batch_size: int = 1000):
        """Save processed data to database in batches"""
        total_records = len(df)
        logger.info(f"Saving {total_records} records to database")
        
        for i in range(0, total_records, batch_size):
            batch = df.iloc[i:i+batch_size]
            records = []
            
            for _, row in batch.iterrows():
                record = RespiratoryMortality(
                    county=row.get('county'),
                    ten_year_age_groups=row.get('ten_year_age_groups'),
                    gender=row.get('gender'),
                    year=int(row.get('year', 0)),
                    icd_10_113_cause_list=row.get('icd_10_113_cause_list'),
                    deaths=int(row.get('deaths', 0)),
                    population=int(row.get('population', 0)),
                    crude_rate=row.get('crude_rate'),
                    state=row.get('state'),
                    mortality_rate=float(row.get('mortality_rate', 0)),
                    high_mortality=bool(row.get('high_mortality', False)),
                    is_male=bool(row.get('is_male', False)),
                    mortality_category=row.get('mortality_category'),
                    population_density=row.get('population_density'),
                    is_west_coast=bool(row.get('is_west_coast', False)),
                    is_east_coast=bool(row.get('is_east_coast', False))
                )
                records.append(record)
            
            db.add_all(records)
            db.commit()
            logger.info(f"Saved batch {i//batch_size + 1}/{(total_records-1)//batch_size + 1}")