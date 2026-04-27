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

    def handle(self, *args, **options):
        try:
            target_date = options.get('date') or timezone.now().date().isoformat()
            domain = (options.get('domain') or '').strip().lower()
            target_dt = datetime.strptime(target_date, '%Y-%m-%d').date()
            history_df = _load_join_history(target_dt, domain)
            if history_df.empty:
                self.stdout.write("[WARN] No data, fallback H-1")
                target_dt = target_dt - timedelta(days=1)
            result = score_site_country(
                target_date=target_dt,
                domain=domain if domain else None,  # 🔥 FIX
                compatibility_mode=False,
                write_results=True,
            )

            self.stdout.write(self.style.SUCCESS(f"[DONE] scoring selesai"))
            self.stdout.write(json.dumps(result, indent=2, default=str))

        except Exception as e:
            self.stdout.write(self.style.ERROR(str(e)))