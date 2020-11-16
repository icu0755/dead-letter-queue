from functools import wraps

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


def reject_on_error_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        task = args[0]
        assert isinstance(task, Task), 'bind=True must be set to enable retries'
        assert task.acks_late, 'acks_late=True must be set to send rejected tasks to the dead letter queue'

        autoretry_for = tuple(
            getattr(task, 'autoretry_for', ())
        )
        retry_backoff = int(
            getattr(task, 'retry_backoff', False)
        )
        retry_backoff_max = int(
            getattr(task, 'retry_backoff_max', 600)
        )
        retry_jitter = getattr(task, 'retry_jitter', True)

        countdown = None
        if retry_backoff:
            countdown = get_exponential_backoff_interval(
                factor=retry_backoff,
                retries=task.request.retries,
                maximum=retry_backoff_max,
                full_jitter=retry_jitter)

        try:
            if not autoretry_for:
                return func(*args, **kwargs)
            else:
                try:
                    return func(*args, **kwargs)
                except autoretry_for as retry_exc:
                    raise task.retry(exc=retry_exc, countdown=countdown)
        except TaskPredicate:
            # pass through celery specific exceptions
            raise
        except Exception as exc:
            # reject if max_retries exceeded
            raise Reject(exc, requeue=False) from exc

    return wrapper


@shared_task(
    bind=True,  # binds task to the first arg
    base=MyTask,
    max_retries=2,
    default_retry_delay=3,
    acks_late=True,  # enable Reject to work
    autoretry_for=(ZeroDivisionError,),
    retry_backoff=10,
)
@reject_on_error_decorator
def celery_div(self, x, y):
    print(f'celery_div:{x},{y}')
    # raise ValueError('foo')
    print(f'div {x}/{y} = {x / y}')
