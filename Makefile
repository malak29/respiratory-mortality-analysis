.PHONY: help build test lint clean deploy backup

DOCKER_IMAGE = respiratory-mortality
DOCKER_TAG = latest
COMPOSE_FILE = docker-compose.yml

help:
	@echo "Available commands:"
	@echo "  build          Build Docker images"
	@echo "  test           Run all tests"
	@echo "  lint          Run code quality checks"
	@echo "  dev            Start development environment"
	@echo "  prod           Start production environment"
	@echo "  deploy         Deploy to production"
	@echo "  backup         Create backup"
	@echo "  restore        Restore from backup"
	@echo "  migrate        Run database migrations"
	@echo "  train          Train initial models"
	@echo "  clean          Clean up containers and volumes"
	@echo "  logs           Show application logs"
	@echo "  monitor        Open monitoring dashboard"

build:
	@echo "Building Docker images..."
	docker-compose -f $(COMPOSE_FILE) build --no-cache

test:
	@echo "Running tests..."
	python -m pytest tests/ -v --cov=app --cov-report=html --cov-report=term-missing
	@echo "Running load tests..."
	k6 run tests/performance/load_test.js

lint:
	@echo "Running code quality checks..."
	black --check app/ tests/ scripts/
	flake8 app/ tests/ scripts/ --max-line-length=88 --ignore=E203,W503
	mypy app/ --ignore-missing-imports
	bandit -r app/ -f json -o security-report.json

dev:
	@echo "Starting development environment..."
	docker-compose -f $(COMPOSE_FILE) up --build

prod:
	@echo "Starting production environment..."
	docker-compose -f docker-compose.prod.yml up -d

deploy:
	@echo "Deploying to production..."
	kubectl apply -f k8s/
	kubectl rollout status deployment/respiratory-api -n respiratory-mortality
	kubectl rollout status deployment/celery-worker -n respiratory-mortality

backup:
	@echo "Creating backup..."
	python scripts/backup_restore.py backup-db
	python scripts/backup_restore.py backup-models

restore:
	@echo "Restoring from backup..."
	@read -p "Enter backup file path: " backup_file; \
	python scripts/backup_restore.py restore-db --file $$backup_file

migrate:
	@echo "Running database migrations..."
	alembic upgrade head

init-db:
	@echo "Initializing database..."
	python scripts/init_database.py

train:
	@echo "Training initial models..."
	python scripts/train_initial_model.py

retrain:
	@echo "Retraining models with optimization..."
	python scripts/model_management.py retrain --optimize

import-data:
	@echo "Importing data from directory..."
	@read -p "Enter data directory path: " data_dir; \
	python scripts/data_migration.py import --input-dir $$data_dir

validate-data:
	@echo "Validating data integrity..."
	python scripts/data_migration.py validate

clean:
	@echo "Cleaning up containers and volumes..."
	docker-compose -f $(COMPOSE_FILE) down -v --remove-orphans
	docker system prune -f

logs:
	@echo "Showing application logs..."
	docker-compose -f $(COMPOSE_FILE) logs -f api

logs-worker:
	@echo "Showing worker logs..."
	docker-compose -f $(COMPOSE_FILE) logs -f worker

monitor:
	@echo "Opening monitoring dashboard..."
	@echo "Grafana: http://localhost:3000 (admin/admin)"
	@echo "Prometheus: http://localhost:9090"
	@echo "Flower (Celery): http://localhost:5555"
	@echo "MLflow: http://localhost:5000"

setup-dev:
	@echo "Setting up development environment..."
	python -m venv venv
	source venv/bin/activate && pip install -r requirements.txt
	cp .env.example .env
	@echo "Please edit .env file with your configuration"

format:
	@echo "Formatting code..."
	black app/ tests/ scripts/
	isort app/ tests/ scripts/

security-scan:
	@echo "Running security scan..."
	bandit -r app/ -f json -o security-report.json
	safety check --json --output safety-report.json

performance-test:
	@echo "Running performance tests..."
	k6 run tests/performance/load_test.js --out json=performance-results.json

stress-test:
	@echo "Running stress tests..."
	k6 run tests/performance/load_test.js --vus=500 --duration=5m

scale-up:
	@echo "Scaling up production deployment..."
	kubectl scale deployment respiratory-api --replicas=5 -n respiratory-mortality
	kubectl scale deployment celery-worker --replicas=4 -n respiratory-mortality

scale-down:
	@echo "Scaling down production deployment..."
	kubectl scale deployment respiratory-api --replicas=2 -n respiratory-mortality
	kubectl scale deployment celery-worker --replicas=2 -n respiratory-mortality

status:
	@echo "Checking deployment status..."
	kubectl get pods -n respiratory-mortality
	kubectl get services -n respiratory-mortality
	kubectl get ingress -n respiratory-mortality

install-tools:
	@echo "Installing development tools..."
	pip install black isort flake8 mypy bandit safety pytest-cov
	
	@echo "Installing k6 for load testing..."
	@echo "Please visit https://k6.io/docs/get-started/installation/ for installation instructions"

db-shell:
	@echo "Opening database shell..."
	docker-compose -f $(COMPOSE_FILE) exec db psql -U postgres -d respiratory_db

redis-cli:
	@echo "Opening Redis CLI..."
	docker-compose -f $(COMPOSE_FILE) exec redis redis-cli

api-shell:
	@echo "Opening API container shell..."
	docker-compose -f $(COMPOSE_FILE) exec api /bin/bash

worker-shell:
	@echo "Opening worker container shell..."
	docker-compose -f $(COMPOSE_FILE) exec worker /bin/bash