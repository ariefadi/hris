import json
from datetime import datetime, timedelta
from django.core.management.base import BaseCommand
from django.utils import timezone
# from . import scoring_concept
from management.scoring_concept import score_site_country, _load_join_history

class Command(BaseCommand):
    help = 'Run scoring per hour'

    def add_arguments(self, parser):
        parser.add_argument('--date', type=str, help='YYYY-MM-DD')
        parser.add_argument('--domain', type=str, help='domain')
        parser.add_argument('--run-hour', type=int, help='0-23, contoh: 3, 6, 9')

    def handle(self, *args, **options):
        try:
            target_date = options.get('date') or timezone.now().date().isoformat()
            domain = (options.get('domain') or '').strip().lower()
            run_hour = options.get('run_hour')
            if run_hour is None:
                run_hour = timezone.localtime().hour
            run_hour = max(0, min(23, int(run_hour)))

            target_dt = datetime.strptime(target_date, '%Y-%m-%d').date()
            history_df = _load_join_history(target_dt, domain, run_hour=run_hour)
            if history_df.empty:
                self.stdout.write(f"[WARN] No data untuk run_hour={run_hour}, fallback H-1")
                target_dt = target_dt - timedelta(days=1)
            result = score_site_country(
                target_date=target_dt,
                domain=domain if domain else None,
                compatibility_mode=False,
                write_results=True,
                run_hour=run_hour,
            )

            self.stdout.write(self.style.SUCCESS(f"[DONE] scoring selesai (date={target_dt}, run_hour={run_hour})"))
            self.stdout.write(json.dumps(result, indent=2, default=str))

        except Exception as e:
            self.stdout.write(self.style.ERROR(str(e)))