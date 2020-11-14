import logging
import os
from typing import List

from celery import Celery, bootsteps
from django.conf import settings
from kombu import Exchange, Queue

logger = logging.getLogger(__name__)

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'app.settings')

app = Celery('app')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()


class DeclareDeadLetterQueue(bootsteps.StartStopStep):
    requires = {'celery.worker.components:Pool'}

    def start(self, worker):
        queues_to_create: List[Queue] = []
        for queue in settings.CELERY_TASK_QUEUES:
            if 'x-dead-letter-routing-key' not in queue.queue_arguments:
                continue

            dead_letter_routing_key = queue.queue_arguments['x-dead-letter-routing-key']
            dead_letter_exchange_name = queue.queue_arguments['x-dead-letter-exchange']

            dead_letter_exchange = Exchange(dead_letter_exchange_name, type='direct')
            dead_letter_queue = Queue(
                dead_letter_routing_key, dead_letter_exchange, routing_key=dead_letter_routing_key,
                queue_arguments={'x-message-ttl': 604800000}
            )
            queues_to_create.append(dead_letter_queue)

        if not queues_to_create:
            return

        with worker.app.pool.acquire() as conn:
            for queue in queues_to_create:
                queue.bind(conn).declare()


app.steps['worker'].add(DeclareDeadLetterQueue)
