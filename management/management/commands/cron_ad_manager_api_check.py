from django.core.management.base import BaseCommand

from management.ad_manager_api import refresh_ad_manager_api_versions


class Command(BaseCommand):
    help = (
        'Cek versi Google Ad Manager API yang aktif dan refresh cache otomatis. '
        'Jalankan via cron setiap 6 jam agar pipeline AdX tidak terhenti saat Google update versi API.'
    )

    def handle(self, *args, **options):
        result = refresh_ad_manager_api_versions()
        if not result.get('status'):
            self.stdout.write(self.style.ERROR('Tidak ada versi Ad Manager API yang aktif.'))
            return

        primary = result.get('primary_version')
        versions = ', '.join(result.get('versions') or [])
        previous = result.get('previous_versions') or []

        if result.get('changed'):
            self.stdout.write(self.style.WARNING(
                f'Versi Ad Manager API berubah: {previous[0] if previous else "-"} -> {primary}'
            ))
        else:
            self.stdout.write(self.style.SUCCESS(
                f'Versi Ad Manager API aktif: {primary}'
            ))

        self.stdout.write(f'Fallback versions: {versions}')
