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
        parser.add_argument('--run-hour', type=int, help='0-23 (opsional, default: jam server saat command mulai)')

    def handle(self, *args, **options):
        try:
            target_date = options.get('date') or timezone.now().date().isoformat()
            domain = (options.get('domain') or '').strip().lower()
            run_hour_opt = options.get('run_hour')
            if run_hour_opt is None:
                run_hour = int(timezone.localtime().hour)
            else:
                run_hour = max(0, min(23, int(run_hour_opt)))

            target_dt = datetime.strptime(target_date, '%Y-%m-%d').date()

            def pick_latest_hour_for_date(dt):
                hist_all = _load_join_history(dt, domain, run_hour=None)
                if hist_all.empty or 'date' not in hist_all.columns:
                    return None
                day_rows = hist_all[hist_all['date'].eq(dt)].copy()
                if day_rows.empty or 'run_hour' not in day_rows.columns:
                    return None
                hours = day_rows['run_hour'].dropna().astype(int)
                if hours.empty:
                    return None
                return int(hours.max())

            resolved_run_hour = run_hour
            history_pref = _load_join_history(target_dt, domain, run_hour=resolved_run_hour)
            has_current_pref = (not history_pref.empty) and ('date' in history_pref.columns) and bool(history_pref['date'].eq(target_dt).any())

            if not has_current_pref:
                latest_hour = pick_latest_hour_for_date(target_dt)
                if latest_hour is not None:
                    self.stdout.write(f"[INFO] run_hour={resolved_run_hour} kosong, pakai run_hour terakhir tersedia: {latest_hour}")
                    resolved_run_hour = latest_hour
                else:
                    self.stdout.write(f"[WARN] Tidak ada data di {target_dt}, fallback H-1")
                    target_dt = target_dt - timedelta(days=1)
                    latest_hour_h1 = pick_latest_hour_for_date(target_dt)
                    if latest_hour_h1 is not None:
                        resolved_run_hour = latest_hour_h1
                        self.stdout.write(f"[INFO] Pakai tanggal fallback {target_dt} run_hour={resolved_run_hour}")

            result = score_site_country(
                target_date=target_dt,
                domain=domain if domain else None,
                compatibility_mode=False,
                write_results=True,
                run_hour=resolved_run_hour,
            )

            self.stdout.write(self.style.SUCCESS(f"[DONE] scoring selesai (date={target_dt}, run_hour={resolved_run_hour}, mode=profit_first)"))
            self.stdout.write(json.dumps(result, indent=2, default=str))

        except Exception as e:
            self.stdout.write(self.style.ERROR(str(e)))