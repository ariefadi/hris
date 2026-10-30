from datetime import datetime, timedelta

from django.core.management.base import BaseCommand, CommandError

from management.sheets_roi import (
    already_published,
    build_roi_rows,
    mark_published,
    publish_rows,
    pull_status,
)


class Command(BaseCommand):
    help = 'Update spreadsheet ROI TODAY setelah penarikan Ads, AdX, dan AdSense pada jam yang sama selesai.'

    def add_arguments(self, parser):
        parser.add_argument('--tanggal', type=str, default='')
        parser.add_argument('--force', action='store_true')
        parser.add_argument('--check', action='store_true')
        parser.add_argument(
            '--riwayat-dari',
            type=str,
            default='',
            help='Isi ulang tab RIWAYAT dari tanggal ini (YYYY-MM-DD) sampai tanggal laporan.',
        )

    def handle(self, *args, **options):
        now = datetime.now()
        status = pull_status(now)
        if options['check']:
            self.stdout.write(
                'Siklus %s siap=%s menunggu=%s'
                % (status['cycle_key'], status['ready'], ','.join(status['pending']) or '-')
            )
            return

        backfill_from = (options.get('riwayat_dari') or '').strip()
        if not options['force'] and not backfill_from:
            if not status['ready']:
                self.stdout.write(
                    'Menunggu penarikan selesai (%s): %s'
                    % (status['cycle_key'], ', '.join(status['pending']))
                )
                return
            if already_published(status['cycle_key']):
                self.stdout.write('Spreadsheet siklus %s sudah diperbarui.' % status['cycle_key'])
                return

        report_date = (options.get('tanggal') or '').strip() or status['cycle'].strftime('%Y-%m-%d')
        rows = build_roi_rows(report_date)

        history = {}
        if backfill_from:
            try:
                day = datetime.strptime(backfill_from, '%Y-%m-%d').date()
                last = datetime.strptime(report_date, '%Y-%m-%d').date()
            except ValueError:
                raise CommandError('Format tanggal harus YYYY-MM-DD')
            while day < last:
                history[day.strftime('%Y-%m-%d')] = build_roi_rows(day.strftime('%Y-%m-%d'))
                day += timedelta(days=1)

        result = publish_rows(report_date, rows, history)
        if not backfill_from:
            mark_published(status['cycle_key'], result['url'], result['rows'])
        self.stdout.write(self.style.SUCCESS(
            'ROI TODAY diperbarui: %s baris; RIWAYAT %s baris (%s tanggal), %s'
            % (result['rows'], result['history_rows'], result['history_dates'], result['url'])
        ))
