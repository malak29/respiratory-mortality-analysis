#!/usr/bin/env python3

import pandas as pd
import asyncio
from sqlalchemy.orm import Session
from app.models.database import SessionLocal, RespiratoryMortality
from app.services.data_processor import DataProcessor
import argparse
import sys
from loguru import logger
from typing import List, Dict, Any
import csv
from pathlib import Path

class DataMigration:
    def __init__(self):
        self.data_processor = DataProcessor()
        
    async def migrate_csv_files(self, input_dir: str, pattern: str = "*.csv"):
        """Migrate multiple CSV files from directory"""
        
        db = SessionLocal()
        
        try:
            input_path = Path(input_dir)
            csv_files = list(input_path.glob(pattern))
            
            if not csv_files:
                logger.warning(f"No CSV files found in {input_dir}")
                return
            
            logger.info(f"Found {len(csv_files)} CSV files to migrate")
            
            total_records = 0
            
            for csv_file in csv_files:
                logger.info(f"Processing {csv_file.name}")
                
                try:
                    df = self.data_processor.load_raw_data(str(csv_file))
                    
                    # Check if this data already exists
                    existing_records = self.check_existing_data(df, db)
                    if existing_records > 0:
                        logger.warning(f"Found {existing_records} existing records, skipping duplicates")
                        df = self.remove_duplicates(df, db)
                    
                    if len(df) == 0:
                        logger.info(f"No new records to import from {csv_file.name}")
                        continue
                    
                    # Apply feature engineering
                    df = self.data_processor.engineer_features(df)
                    
                    # Save to database
                    self.data_processor.save_to_database(df, db)
                    
                    total_records += len(df)
                    logger.info(f"Migrated {len(df)} records from {csv_file.name}")
                    
                except Exception as e:
                    logger.error(f"Error processing {csv_file.name}: {e}")
                    continue
            
            logger.info(f"Migration completed. Total records migrated: {total_records}")
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            raise
            
        finally:
            db.close()
    
    def check_existing_data(self, df: pd.DataFrame, db: Session) -> int:
        """Check how many records already exist in database"""
        
        if df.empty:
            return 0
        
        # Sample check with first few records
        sample_df = df.head(100)
        existing_count = 0
        
        for _, row in sample_df.iterrows():
            exists = db.query(RespiratoryMortality).filter(
                RespiratoryMortality.county == row['county'],
                RespiratoryMortality.state == row['state'],
                RespiratoryMortality.year == row['year'],
                RespiratoryMortality.gender == row['gender'],
                RespiratoryMortality.ten_year_age_groups == row['ten_year_age_groups']
            ).first()
            
            if exists:
                existing_count += 1
        
        # Estimate total existing records
        return int((existing_count / len(sample_df)) * len(df))
    
    def remove_duplicates(self, df: pd.DataFrame, db: Session) -> pd.DataFrame:
        """Remove records that already exist in database"""
        
        unique_records = []
        
        for _, row in df.iterrows():
            exists = db.query(RespiratoryMortality).filter(
                RespiratoryMortality.county == row['county'],
                RespiratoryMortality.state == row['state'], 
                RespiratoryMortality.year == row['year'],
                RespiratoryMortality.gender == row['gender'],
                RespiratoryMortality.ten_year_age_groups == row['ten_year_age_groups'],
                RespiratoryMortality.deaths == row['deaths']
            ).first()
            
            if not exists:
                unique_records.append(row)
        
        if unique_records:
            return pd.DataFrame(unique_records)
        else:
            return pd.DataFrame()
    
    async def export_data(self, output_file: str, filters: Dict[str, Any] = None):
        """Export data from database to CSV"""
        
        db = SessionLocal()
        
        try:
            query = db.query(RespiratoryMortality)
            
            # Apply filters
            if filters:
                if 'start_year' in filters:
                    query = query.filter(RespiratoryMortality.year >= filters['start_year'])
                if 'end_year' in filters:
                    query = query.filter(RespiratoryMortality.year <= filters['end_year'])
                if 'states' in filters:
                    query = query.filter(RespiratoryMortality.state.in_(filters['states']))
                if 'gender' in filters:
                    query = query.filter(RespiratoryMortality.gender == filters['gender'])
            
            # Load data
            df = pd.read_sql(query.statement, db.bind)
            
            # Export to CSV
            df.to_csv(output_file, index=False)
            
            logger.info(f"Exported {len(df)} records to {output_file}")
            
        except Exception as e:
            logger.error(f"Export failed: {e}")
            raise
            
        finally:
            db.close()
    
    async def validate_data_integrity(self):
        """Validate data integrity in database"""
        
        db = SessionLocal()
        
        try:
            logger.info("Starting data integrity validation...")
            
            issues = []
            
            # Check for negative deaths
            negative_deaths = db.query(RespiratoryMortality).filter(
                RespiratoryMortality.deaths < 0
            ).count()
            
            if negative_deaths > 0:
                issues.append(f"Found {negative_deaths} records with negative deaths")
            
            # Check for zero population
            zero_population = db.query(RespiratoryMortality).filter(
                RespiratoryMortality.population <= 0
            ).count()
            
            if zero_population > 0:
                issues.append(f"Found {zero_population} records with zero/negative population")
            
            # Check for invalid years
            invalid_years = db.query(RespiratoryMortality).filter(
                (RespiratoryMortality.year < 1999) | (RespiratoryMortality.year > 2025)
            ).count()
            
            if invalid_years > 0:
                issues.append(f"Found {invalid_years} records with invalid years")
            
            # Check for missing engineered features
            missing_features = db.query(RespiratoryMortality).filter(
                RespiratoryMortality.mortality_rate.is_(None)
            ).count()
            
            if missing_features > 0:
                issues.append(f"Found {missing_features} records with missing engineered features")
            
            if issues:
                logger.warning("Data integrity issues found:")
                for issue in issues:
                    logger.warning(f"  - {issue}")
                return False
            else:
                logger.info("Data integrity validation passed")
                return True
                
        except Exception as e:
            logger.error(f"Data validation failed: {e}")
            raise
            
        finally:
            db.close()

async def main():
    parser = argparse.ArgumentParser(description='Data migration utility')
    parser.add_argument('action', choices=['import', 'export', 'validate'])
    parser.add_argument('--input-dir', help='Input directory for CSV files')
    parser.add_argument('--output-file', help='Output file for export')
    parser.add_argument('--pattern', default='*.csv', help='File pattern for import')
    parser.add_argument('--start-year', type=int, help='Start year filter for export')
    parser.add_argument('--end-year', type=int, help='End year filter for export')
    parser.add_argument('--states', nargs='+', help='State filter for export')
    
    args = parser.parse_args()
    
    migrator = DataMigration()
    
    if args.action == 'import':
        if not args.input_dir:
            logger.error("--input-dir required for import")
            sys.exit(1)
        await migrator.migrate_csv_files(args.input_dir, args.pattern)
        
    elif args.action == 'export':
        if not args.output_file:
            logger.error("--output-file required for export")
            sys.exit(1)
        
        filters = {}
        if args.start_year:
            filters['start_year'] = args.start_year
        if args.end_year:
            filters['end_year'] = args.end_year
        if args.states:
            filters['states'] = args.states
        
        await migrator.export_data(args.output_file, filters)
        
    elif args.action == 'validate':
        success = await migrator.validate_data_integrity()
        sys.exit(0 if success else 1)

if __name__ == "__main__":
    asyncio.run(main())