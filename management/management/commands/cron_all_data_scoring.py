from django.core.management.base import BaseCommand
from datetime import datetime
from management.database import data_mysql
class Command(BaseCommand):
    help = "Sync Fact Domain Country by Tanggal"
    def add_arguments(self, parser):
        parser.add_argument(
            '--tanggal',
            type=str,
            default=None,
            help='Format YYYY-MM-DD'
        )
    def handle(self, *args, **kwargs):
        try:
            db = data_mysql()
            tanggal = kwargs.get("tanggal")
            # =========================
            # DEFAULT = TODAY
            # =========================
            if not tanggal:
                tanggal = datetime.now().strftime("%Y-%m-%d")

            self.stdout.write(self.style.WARNING(
                f"[CRON] Processing tanggal: {tanggal}"
            ))
            # =========================
            # 1. GET DATA (PAKAI PARAMETER)
            # =========================
            result = db.get_all_data_meta_adx_adsense_country_detail_by_params(tanggal)
            if not result.get("status"):
                self.stdout.write(self.style.ERROR(
                    f"[CRON] Query failed: {result.get('error')}"
                ))
                return
            rows = result.get("data", [])
            self.stdout.write(self.style.WARNING(
                f"[CRON] Rows fetched: {len(rows)}"
            ))
            if not rows:
                self.stdout.write(self.style.SUCCESS(
                    "[CRON] No data to insert"
                ))
                return
            # =========================
            # 2. DELETE FACT TABLE BY DATE
            # =========================
            del_result = db.delete_fact_join_hourly_by_date(tanggal)
            if not del_result.get("status"):
                self.stdout.write(self.style.ERROR(
                    f"[CRON] Delete failed: {del_result.get('error')}"
                ))
                return
            self.stdout.write(self.style.WARNING(
                f"[CRON] Deleted: {del_result.get('message')}"
            ))
            # =========================
            # 3. INSERT FACT TABLE
            # =========================
            insert_result = db.insert_bulk_fact_domain(rows)
            if not insert_result.get("status"):
                self.stdout.write(self.style.ERROR(
                    f"[CRON] Insert failed: {insert_result.get('error')}"
                ))
                return
            self.stdout.write(self.style.SUCCESS(
                f"[CRON] SUCCESS: {insert_result.get('message')}"
            ))
        except Exception as e:
            self.stdout.write(self.style.ERROR(
                f"[CRON ERROR] {str(e)}"
            ))