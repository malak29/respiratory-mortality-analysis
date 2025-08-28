from celery import current_task
import pandas as pd
from sqlalchemy.orm import Session
from app.tasks.celery_app import celery_app
from app.models.database import SessionLocal
from app.services.data_processor import DataProcessor
from typing import List, Dict, Any
from loguru import logger
import traceback
import io
import boto3
from app.core.config import settings

@celery_app.task(bind=True)
def process_csv_upload_task(self, file_content: str, filename: str):
    """Background task for processing uploaded CSV files"""
    
    db = SessionLocal()
    
    try:
        current_task.update_state(
            state="PROGRESS",
            meta={"status": "Reading CSV file", "progress": 10}
        )
        
        # Read CSV from string content
        df = pd.read_csv(io.StringIO(file_content))
        logger.info(f"Processing CSV file {filename} with {len(df)} records")
        
        current_task.update_state(
            state="PROGRESS",
            meta={"status": "Validating data", "progress": 30}
        )
        
        # Initialize data processor and validate
        data_processor = DataProcessor()
        df = data_processor._validate_data(df)
        
        current_task.update_state(
            state="PROGRESS", 
            meta={"status": "Engineering features", "progress": 50}
        )
        
        # Feature engineering
        df = data_processor.engineer_features(df)
        
        current_task.update_state(
            state="PROGRESS",
            meta={"status": "Saving to database", "progress": 70}
        )
        
        # Save to database
        data_processor.save_to_database(df, db)
        
        result = {
            "filename": filename,
            "records_processed": len(df),
            "status": "completed"
        }
        
        logger.info(f"Successfully processed {filename}")
        
        return result
        
    except Exception as e:
        error_msg = f"CSV processing failed: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        
        current_task.update_state(
            state="FAILURE",
            meta={"error": error_msg}
        )
        
        raise
        
    finally:
        db.close()

@celery_app.task(bind=True)
def batch_data_analysis_task(self, analysis_type: str, parameters: Dict[str, Any]):
    """Background task for heavy data analysis operations"""
    
    db = SessionLocal()
    
    try:
        current_task.update_state(
            state="PROGRESS",
            meta={"status": "Loading data for analysis", "progress": 20}
        )
        
        # Load data based on parameters
        query = db.query(RespiratoryMortality)
        
        if "start_year" in parameters:
            query = query.filter(RespiratoryMortality.year >= parameters["start_year"])
        if "end_year" in parameters:
            query = query.filter(RespiratoryMortality.year <= parameters["end_year"])
        if "states" in parameters:
            query = query.filter(RespiratoryMortality.state.in_(parameters["states"]))
        
        df = pd.read_sql(query.statement, db.bind)
        
        current_task.update_state(
            state="PROGRESS",
            meta={"status": f"Running {analysis_type} analysis", "progress": 60}
        )
        
        results = {}
        
        if analysis_type == "temporal_trends":
            results = analyze_temporal_trends(df)
        elif analysis_type == "geographic_analysis":
            results = analyze_geographic_patterns(df)
        elif analysis_type == "demographic_analysis":
            results = analyze_demographic_patterns(df)
        elif analysis_type == "cause_analysis":
            results = analyze_cause_patterns(df)
        
        current_task.update_state(
            state="PROGRESS",
            meta={"status": "Finalizing results", "progress": 90}
        )
        
        # Store results in S3 or local storage
        if settings.S3_BUCKET:
            store_results_s3(results, analysis_type, parameters)
        
        logger.info(f"Analysis {analysis_type} completed successfully")
        
        return {
            "analysis_type": analysis_type,
            "records_analyzed": len(df),
            "results": results,
            "status": "completed"
        }
        
    except Exception as e:
        error_msg = f"Analysis failed: {str(e)}"
        logger.error(error_msg)
        
        current_task.update_state(
            state="FAILURE",
            meta={"error": error_msg}
        )
        
        raise
        
    finally:
        db.close()

def analyze_temporal_trends(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze mortality trends over time"""
    
    yearly_stats = df.groupby('year').agg({
        'deaths': ['sum', 'mean'],
        'mortality_rate': ['mean', 'median'],
        'population': 'sum'
    }).round(2)
    
    gender_trends = df.groupby(['year', 'gender']).agg({
        'deaths': 'sum',
        'mortality_rate': 'mean'
    }).round(2)
    
    return {
        "yearly_statistics": yearly_stats.to_dict(),
        "gender_trends": gender_trends.to_dict(),
        "total_years": len(df['year'].unique()),
        "data_range": {
            "start_year": int(df['year'].min()),
            "end_year": int(df['year'].max())
        }
    }

def analyze_geographic_patterns(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze geographic distribution of mortality"""
    
    state_stats = df.groupby('state').agg({
        'deaths': 'sum',
        'mortality_rate': 'mean',
        'population': 'sum'
    }).sort_values('deaths', ascending=False)
    
    county_stats = df.groupby(['state', 'county']).agg({
        'deaths': 'sum', 
        'mortality_rate': 'mean'
    }).sort_values('mortality_rate', ascending=False)
    
    regional_analysis = df.groupby(['is_west_coast', 'is_east_coast']).agg({
        'deaths': 'sum',
        'mortality_rate': 'mean'
    })
    
    return {
        "state_rankings": state_stats.head(20).to_dict(),
        "highest_risk_counties": county_stats.head(50).to_dict(),
        "regional_comparison": regional_analysis.to_dict()
    }

def analyze_demographic_patterns(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze demographic patterns in mortality"""
    
    age_gender_stats = df.groupby(['ten_year_age_groups', 'gender']).agg({
        'deaths': 'sum',
        'mortality_rate': 'mean'
    }).round(2)
    
    age_distribution = df.groupby('ten_year_age_groups').agg({
        'deaths': 'sum',
        'mortality_rate': ['mean', 'std']
    }).round(2)
    
    return {
        "age_gender_breakdown": age_gender_stats.to_dict(),
        "age_distribution": age_distribution.to_dict(),
        "gender_totals": df.groupby('gender')['deaths'].sum().to_dict()
    }

def analyze_cause_patterns(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze patterns by cause of death"""
    
    cause_stats = df.groupby('icd_10_113_cause_list').agg({
        'deaths': 'sum',
        'mortality_rate': 'mean'
    }).sort_values('deaths', ascending=False)
    
    cause_trends = df.groupby(['year', 'icd_10_113_cause_list']).agg({
        'deaths': 'sum'
    }).round(2)
    
    return {
        "cause_rankings": cause_stats.to_dict(),
        "temporal_cause_trends": cause_trends.to_dict(),
        "total_causes": len(df['icd_10_113_cause_list'].unique())
    }

def store_results_s3(results: Dict[str, Any], analysis_type: str, parameters: Dict[str, Any]):
    """Store analysis results in S3"""
    
    if not settings.AWS_ACCESS_KEY_ID or not settings.S3_BUCKET:
        return
    
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION
        )
        
        import json
        result_json = json.dumps(results, default=str, indent=2)
        
        key = f"analysis_results/{analysis_type}/{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        s3_client.put_object(
            Bucket=settings.S3_BUCKET,
            Key=key,
            Body=result_json,
            ContentType='application/json'
        )
        
        logger.info(f"Analysis results stored in S3: {key}")
        
    except Exception as e:
        logger.error(f"Failed to store results in S3: {e}")