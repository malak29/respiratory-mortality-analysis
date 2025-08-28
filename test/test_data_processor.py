import pytest
import pandas as pd
import numpy as np
from app.services.data_processor import DataProcessor
from unittest.mock import patch, MagicMock

class TestDataProcessor:
    
    @pytest.fixture
    def processor(self):
        """Create DataProcessor instance"""
        return DataProcessor()
    
    def test_validate_data_success(self, processor, sample_dataframe):
        """Test successful data validation"""
        validated_df = processor._validate_data(sample_dataframe)
        assert len(validated_df) == len(sample_dataframe)
        assert validated_df.isnull().sum().sum() == 0

    def test_validate_data_missing_columns(self, processor):
        """Test validation with missing required columns"""
        incomplete_df = pd.DataFrame({"county": ["Test"], "year": [2020]})
        
        with pytest.raises(ValueError, match="Missing required columns"):
            processor._validate_data(incomplete_df)

    def test_validate_data_with_nulls(self, processor, sample_dataframe):
        """Test validation removes null values"""
        df_with_nulls = sample_dataframe.copy()
        df_with_nulls.loc[0, 'deaths'] = None
        
        validated_df = processor._validate_data(df_with_nulls)
        assert len(validated_df) == 1  # One row should be dropped

    def test_engineer_features(self, processor, sample_dataframe):
        """Test feature engineering"""
        engineered_df = processor.engineer_features(sample_dataframe)
        
        # Check new columns exist
        expected_columns = [
            'mortality_rate', 'high_mortality', 'is_male', 
            'mortality_category', 'population_density',
            'is_west_coast', 'is_east_coast'
        ]
        
        for col in expected_columns:
            assert col in engineered_df.columns
        
        # Check calculations
        expected_mortality_rate = (sample_dataframe.iloc[0]['deaths'] / 
                                 sample_dataframe.iloc[0]['population']) * 100000
        actual_mortality_rate = engineered_df.iloc[0]['mortality_rate']
        assert abs(expected_mortality_rate - actual_mortality_rate) < 0.01

    def test_prepare_model_data_fit(self, processor, sample_dataframe):
        """Test model data preparation with fitting encoders"""
        engineered_df = processor.engineer_features(sample_dataframe)
        X, y = processor.prepare_model_data(engineered_df, fit_encoders=True)
        
        # Check shapes
        assert len(X) == len(engineered_df)
        assert len(y) == len(engineered_df)
        
        # Check that encoders were fitted
        assert len(processor.label_encoders) > 0
        assert 'gender' in processor.label_encoders

    def test_prepare_model_data_transform(self, processor, sample_dataframe):
        """Test model data preparation with existing encoders"""
        engineered_df = processor.engineer_features(sample_dataframe)
        
        # First fit encoders
        X_fit, y_fit = processor.prepare_model_data(engineered_df, fit_encoders=True)
        
        # Then transform using fitted encoders
        X_transform, y_transform = processor.prepare_model_data(engineered_df, fit_encoders=False)
        
        # Results should be identical
        pd.testing.assert_frame_equal(X_fit, X_transform)
        pd.testing.assert_series_equal(y_fit, y_transform)

    @patch('joblib.dump')
    @patch('os.makedirs')
    def test_save_preprocessors(self, mock_makedirs, mock_dump, processor):
        """Test saving preprocessors"""
        processor.save_preprocessors("/test/path")
        
        mock_makedirs.assert_called_once_with("/test/path", exist_ok=True)
        assert mock_dump.call_count == 2

    @patch('joblib.load')
    def test_load_preprocessors(self, mock_load, processor):
        """Test loading preprocessors"""
        mock_scaler = MagicMock()
        mock_encoders = {"gender": MagicMock()}
        
        mock_load.side_effect = [mock_scaler, mock_encoders]
        
        processor.load_preprocessors("/test/path")
        
        assert processor.scaler == mock_scaler
        assert processor.label_encoders == mock_encoders

class TestDataValidation:
    
    def test_year_validation(self):
        """Test year range validation"""
        from app.schemas.mortality import MortalityDataCreate
        
        # Valid year
        valid_data = {
            "county": "Test",
            "ten_year_age_groups": "65-74 years",
            "gender": "Male",
            "year": 2020,
            "icd_10_113_cause_list": "Test cause",
            "deaths": 10,
            "population": 1000,
            "state": "California"
        }
        
        mortality_data = MortalityDataCreate(**valid_data)
        assert mortality_data.year == 2020
        
        # Invalid year
        invalid_data = valid_data.copy()
        invalid_data["year"] = 1990
        
        with pytest.raises(ValueError):
            MortalityDataCreate(**invalid_data)

    def test_deaths_validation(self):
        """Test deaths validation"""
        from app.schemas.mortality import MortalityDataCreate
        
        valid_data = {
            "county": "Test",
            "ten_year_age_groups": "65-74 years", 
            "gender": "Male",
            "year": 2020,
            "icd_10_113_cause_list": "Test cause",
            "deaths": -5,  # Invalid
            "population": 1000,
            "state": "California"
        }
        
        with pytest.raises(ValueError):
            MortalityDataCreate(**valid_data)