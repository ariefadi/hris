import json
from django.core.management.base import BaseCommand
from management.database import data_mysql
from management.list_adx_policy_events import sync_adx_policy_events as _sync_adx_policy_events


def sync_adx_policy_events(db, days=180, max_per_user=200, sync_date=None, account_filter=None):
    return _sync_adx_policy_events(
        db,
        days=days,
        max_per_user=max_per_user,
        sync_date=sync_date,
        account_filter=account_filter,
    )


class Command(BaseCommand):
    help = "Tarik email Policy AdX/Ad Manager dari Gmail (app_credentials) ke tabel adx_policy_events"

    def add_arguments(self, parser):
        parser.add_argument('--days', type=int, default=180)
        parser.add_argument('--max-per-user', type=int, default=200)

    def handle(self, *args, **options):
        db = data_mysql()
        result = sync_adx_policy_events(
            db,
            days=int(options.get('days') or 180),
            max_per_user=int(options.get('max_per_user') or 200),
        )
        self.stdout.write(json.dumps(result, indent=2, ensure_ascii=False))
