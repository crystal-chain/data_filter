# celery_app.py
import os
from celery import Celery

broker_url = os.getenv("CELERY_BROKER_URL", "pyamqp://guest@rabbitmq//")
result_backend = os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/0")

celery_app = Celery("tasks", broker=broker_url, backend=result_backend)

celery_app.conf.update(
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
)

# Importer le module tasks pour enregistrer la tâche
import tasks
