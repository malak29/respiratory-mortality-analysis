#!/usr/bin/env python3

import os
import sys
import subprocess
import boto3
from datetime import datetime, timedelta
import argparse
from loguru import logger
from app.core.config import settings

class BackupManager:
    def __init__(self):
        self.s3_client = None
        if settings.AWS_ACCESS_KEY_ID and settings.S3_BUCKET:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
                region_name=settings.AWS_REGION
            )
    
    def create_database_backup(self, backup_path: str = None) -> str:
        """Create PostgreSQL database backup"""
        
        if backup_path is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = f"backups/db_backup_{timestamp}.sql"
        
        os.makedirs(os.path.dirname(backup_path), exist_ok=True)
        
        # Parse database URL
        db_url = settings.DATABASE_URL
        if db_url.startswith('postgresql://'):
            db_url = db_url.replace('postgresql://', '')
        
        # Extract connection details
        parts = db_url.split('@')
        user_pass = parts[0]
        host_db = parts[1]
        
        user, password = user_pass.split(':')
        host_port, database = host_db.split('/')
        host, port = host_port.split(':') if ':' in host_port else (host_port, '5432')
        
        # Create backup using pg_dump
        env = os.environ.copy()
        env['PGPASSWORD'] = password
        
        cmd = [
            'pg_dump',
            '-h', host,
            '-p', port,
            '-U', user,
            '-d', database,
            '--verbose',
            '--no-owner',
            '--no-privileges',
            '-f', backup_path
        ]
        
        try:
            result = subprocess.run(cmd, env=env, capture_output=True, text=True, check=True)
            logger.info(f"Database backup created: {backup_path}")
            
            # Upload to S3 if configured
            if self.s3_client and settings.S3_BUCKET:
                self.upload_to_s3(backup_path, f"database_backups/{os.path.basename(backup_path)}")
            
            return backup_path
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Database backup failed: {e.stderr}")
            raise

    def restore_database_backup(self, backup_path: str):
        """Restore PostgreSQL database from backup"""
        
        if not os.path.exists(backup_path):
            logger.error(f"Backup file not found: {backup_path}")
            return False
        
        # Parse database URL (same as backup)
        db_url = settings.DATABASE_URL
        if db_url.startswith('postgresql://'):
            db_url = db_url.replace('postgresql://', '')
        
        parts = db_url.split('@')
        user_pass = parts[0]
        host_db = parts[1]
        
        user, password = user_pass.split(':')
        host_port, database = host_db.split('/')
        host, port = host_port.split(':') if ':' in host_port else (host_port, '5432')
        
        env = os.environ.copy()
        env['PGPASSWORD'] = password
        
        # Drop and recreate database
        cmd_drop = [
            'psql',
            '-h', host,
            '-p', port,
            '-U', user,
            '-d', 'postgres',
            '-c', f'DROP DATABASE IF EXISTS {database};'
        ]
        
        cmd_create = [
            'psql',
            '-h', host,
            '-p', port, 
            '-U', user,
            '-d', 'postgres',
            '-c', f'CREATE DATABASE {database};'
        ]
        
        cmd_restore = [
            'psql',
            '-h', host,
            '-p', port,
            '-U', user,
            '-d', database,
            '-f', backup_path
        ]
        
        try:
            subprocess.run(cmd_drop, env=env, check=True)
            subprocess.run(cmd_create, env=env, check=True)
            subprocess.run(cmd_restore, env=env, check=True)
            
            logger.info(f"Database restored from: {backup_path}")
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Database restore failed: {e}")
            return False

    def backup_models(self, models_path: str = "models") -> str:
        """Create backup of ML models and preprocessors"""
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_name = f"models_backup_{timestamp}.tar.gz"
        backup_path = f"backups/{backup_name}"
        
        os.makedirs("backups", exist_ok=True)
        
        cmd = ['tar', '-czf', backup_path, '-C', '.', models_path]
        
        try:
            subprocess.run(cmd, check=True)
            logger.info(f"Models backup created: {backup_path}")
            
            # Upload to S3
            if self.s3_client and settings.S3_BUCKET:
                self.upload_to_s3(backup_path, f"model_backups/{backup_name}")
            
            return backup_path
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Models backup failed: {e}")
            raise

    def upload_to_s3(self, local_path: str, s3_key: str):
        """Upload file to S3"""
        
        if not self.s3_client:
            logger.warning("S3 not configured, skipping upload")
            return
        
        try:
            self.s3_client.upload_file(local_path, settings.S3_BUCKET, s3_key)
            logger.info(f"Uploaded {local_path} to s3://{settings.S3_BUCKET}/{s3_key}")
            
        except Exception as e:
            logger.error(f"S3 upload failed: {e}")

    def cleanup_old_backups(self, backup_dir: str = "backups", retention_days: int = 30):
        """Clean up old backup files"""
        
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        
        if not os.path.exists(backup_dir):
            return
        
        deleted_count = 0
        for filename in os.listdir(backup_dir):
            file_path = os.path.join(backup_dir, filename)
            if os.path.isfile(file_path):
                file_time = datetime.fromtimestamp(os.path.getctime(file_path))
                if file_time < cutoff_date:
                    os.remove(file_path)
                    deleted_count += 1
                    logger.info(f"Deleted old backup: {filename}")
        
        logger.info(f"Cleaned up {deleted_count} old backup files")

def main():
    parser = argparse.ArgumentParser(description='Backup and restore utility')
    parser.add_argument('action', choices=['backup-db', 'restore-db', 'backup-models', 'cleanup'])
    parser.add_argument('--file', help='Backup file path (for restore)')
    parser.add_argument('--retention-days', type=int, default=30, help='Retention period for cleanup')
    
    args = parser.parse_args()
    
    backup_manager = BackupManager()
    
    if args.action == 'backup-db':
        backup_manager.create_database_backup()
    elif args.action == 'restore-db':
        if not args.file:
            logger.error("--file parameter required for restore")
            sys.exit(1)
        backup_manager.restore_database_backup(args.file)
    elif args.action == 'backup-models':
        backup_manager.backup_models()
    elif args.action == 'cleanup':
        backup_manager.cleanup_old_backups(retention_days=args.retention_days)

if __name__ == "__main__":
    main()