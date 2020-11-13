from django.core.management.base import BaseCommand

from core.tasks import celery_div


class Command(BaseCommand):
    def handle(self, *args, **options):
        celery_div.delay(6, 0)
