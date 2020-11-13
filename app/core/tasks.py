from contextlib import AbstractContextManager

from celery import Task, shared_task
from celery.exceptions import Ignore, Reject, Retry, TaskPredicate
from celery.utils.time import get_exponential_backoff_interval
from celery.worker.request import Request
from dramatiq import actor


@actor(max_retries=2, queue_name='divide.XQ')
def divide(x, y):
    print(f'divide:{x},{y}')
    raise ValueError('foo')


class MyRequest(Request):
    pass


class MyTask(Task):
    Request = MyRequest

    def retry(self, args=None, kwargs=None, exc=None, throw=True, eta=None, countdown=None, max_retries=None,
              **options):
        try:
            return super().retry(args, kwargs, exc, throw, eta, countdown, max_retries, **options)
        except (Ignore, Reject, Retry):
            raise
        except Exception as exc:
            raise Reject(exc, requeue=False)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        print('{0!r} failed: {1!r}'.format(task_id, exc))


@shared_task(
    bind=True,  # binds task to the first arg
    max_retries=2,
    default_retry_delay=3,
    acks_late=True,  # enable Reject to work
    autoretry_for=(ZeroDivisionError,),
    retry_backoff=10,
)
def celery_div(self, x, y):
    with reject_on_error(self):
        print(f'celery_div:{x},{y}')
        # raise ValueError('foo')
        print(f'div {x}/{y} = {x / y}')


class reject_on_error(AbstractContextManager):
    def __init__(self, task: Task):
        self._task = task

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            return False

        if not issubclass(exc_type, self._task.autoretry_for):
            # reject if not in autoretry_for
            raise Reject(exc_value, requeue=False)

        retry_backoff = int(
            getattr(self._task, 'retry_backoff', False)
        )
        retry_backoff_max = int(
            getattr(self._task, 'retry_backoff_max', 600)
        )
        retry_jitter = getattr(self._task, 'retry_jitter', True)
        countdown = None
        if retry_backoff:
            countdown = get_exponential_backoff_interval(
                            factor=retry_backoff,
                            retries=self._task.request.retries,
                            maximum=retry_backoff_max,
                            full_jitter=retry_jitter)
        print(f'countdown={countdown}')

        try:
            raise self._task.retry(exc=exc_value, countdown=countdown)
        except TaskPredicate:
            # pass through celery specific exceptions
            raise
        except Exception as exc:
            # reject if max_retries exceeded
            raise Reject(exc, requeue=False) from exc
