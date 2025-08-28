from celery import Celery
from app.core.config import settings

celery_app = Celery(
    "respiratory_mortality",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
    include=["app.tasks.training_tasks", "app.tasks.data_tasks"]
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_routes={
        "app.tasks.training_tasks.*": {"queue": "ml_training"},
        "app.tasks.data_tasks.*": {"queue": "data_processing"}
    },
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_max_tasks_per_child=1000
)