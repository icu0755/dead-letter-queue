from celery import Celery, Task
from celery.exceptions import Reject, Retry
from celery.utils.serialization import raise_with_context
from celery.worker.request import Request
from kombu import Exchange, Queue


app = Celery('main', broker='amqp://rabbitmq:rabbitmq@localhost//')

default_queue_name = 'default'
default_exchange_name = 'default'
default_routing_key = 'default'
deadletter_suffix = 'deadletter'
deadletter_queue_name = default_queue_name + f'.{deadletter_suffix}'
deadletter_exchange_name = default_exchange_name + f'.{deadletter_suffix}'
deadletter_routing_key = default_routing_key + f'.{deadletter_suffix}'

default_exchange = Exchange(default_exchange_name, type='direct')
default_queue = Queue(
    default_queue_name,
    default_exchange,
    routing_key=default_routing_key,
    queue_arguments={
        'x-dead-letter-exchange': deadletter_exchange_name,
        'x-dead-letter-routing-key': deadletter_routing_key
    })


deadletter_exchange = Exchange(deadletter_exchange_name, type='direct')
deadletter_queue = Queue(
    deadletter_queue_name,
    deadletter_exchange,
    routing_key=deadletter_routing_key,
)


app.conf.task_queues = (default_queue,)
app.conf.task_default_queue = default_queue_name
app.conf.task_default_exchange = default_exchange_name
app.conf.task_default_routing_key = default_routing_key


@app.task
def add(x, y):
    return x + y


class MyRequest(Request):
    pass


class MyTask(Task):
    def retry(self, args=None, kwargs=None, exc=None, throw=True, eta=None, countdown=None, max_retries=None,
              **options):
        request = self.request
        retries = request.retries + 1
        max_retries = self.max_retries if max_retries is None else max_retries

        # Not in worker or emulated by (apply/always_eager),
        # so just raise the original exception.
        if request.called_directly:
            # raises orig stack if PyErr_Occurred,
            # and augments with exc' if that argument is defined.
            raise_with_context(exc or Retry('Task can be retried', None))

        if max_retries is not None and retries > max_retries:
            pass

        return super().retry(args, kwargs, exc, throw, eta, countdown, max_retries, **options)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        # raise Reject(exc, requeue=False)
        print('{0!r} failed: {1!r}'.format(task_id, exc))


@app.task(
    base=MyTask,
    acks_late=True,
    max_retries=1,
    retry_backoff=4,
    autoretry_for=(ZeroDivisionError,),
    foo='foo',
)
def div(x, y):
    add.s(2, 2).apply_async(queue=deadletter_queue)
    return x / y


if '__main__' == __name__:
    # add.delay(1, 2)
    # div.delay(2, 1)
    div.delay(2, 0)
