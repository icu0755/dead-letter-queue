from celery import group
from django.core.management.base import BaseCommand

from core.tasks import celery_div, get_random, tasks_composition


class Command(BaseCommand):
    def handle(self, *args, **options):
        self.check_groups()
        # self.check_dead_letter()
        # tasks_composition.delay()

    def check_dead_letter(self):
        celery_div.delay(6, 0)

    def check_groups(self):
        # group1 runs 2 tasks in parallel. it is considered to be finished
        # when all tasks and retries were finished
        group1 = group(get_random.si(0).set(countdown=2), get_random.si(1).set(countdown=4))

        # group2 runs 2 tasks in parallel, when group1 is finished
        group2 = group(get_random.si(10), get_random.si(11))
        chain = group1 | group2
        chain.delay()
