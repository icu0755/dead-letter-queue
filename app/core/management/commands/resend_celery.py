from django.core.management.base import BaseCommand
from kombu import Connection, Queue
from kombu.mixins import ConsumerProducerMixin

from app.settings import CELERY_BROKER_URL


queue = Queue('celery-default', routing_key='celery-default', no_declare=True)
dead_letter_queue = Queue('celery-default-XQ', routing_key='celery-default-XQ', no_declare=True)


class Command(BaseCommand):
    def handle(self, *args, **options):
        with Connection(CELERY_BROKER_URL) as connection:
            worker = Worker(connection, dead_letter_queue, queue, 1)
            worker.run()


class Worker(ConsumerProducerMixin):
    def __init__(self, connection, queue_from, queue_to, read_limit=1000):
        self.connection = connection
        self.queue_from = queue_from
        self.queue_to = queue_to
        self.read_limit = read_limit
        self.read_count = 0

    def get_consumers(self, Consumer, channel):
        return [Consumer(
            queues=[self.queue_from],
            on_message=self.on_request,
            accept={'application/json'},
            prefetch_count=1,
        )]

    def on_request(self, message):
        self.producer.publish(
            exchange='',
            routing_key=self.queue_to.routing_key,
            retry=True,
            body=message.body,
            headers=dict(message.headers, retries=0),  # reset retries count
            content_type=message.content_type,
            content_encoding=message.content_encoding,
        )
        message.ack()
        self.read_count += 1
        print(f'requeue message #{self.read_count}: {message!r}')

    @property
    def should_stop(self):
        return self.read_limit <= self.read_count
