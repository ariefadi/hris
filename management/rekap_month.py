from datetime import datetime, date
import calendar


def shift_month(year, month, delta):
    month += int(delta)
    while month < 1:
        month += 12
        year -= 1
    while month > 12:
        month -= 12
        year += 1
    return year, month


def month_bounds(year, month):
    last_day = calendar.monthrange(int(year), int(month))[1]
    start = f"{int(year):04d}-{int(month):02d}-01"
    end = f"{int(year):04d}-{int(month):02d}-{last_day:02d}"
    return start, end


def parse_pull_date(value):
    if not value:
        return date.today()
    raw = str(value).strip()
    try:
        return datetime.strptime(raw, '%Y-%m-%d').date()
    except ValueError as exc:
        raise ValueError(f"Format --tanggal-tarik tidak valid: {raw} (gunakan YYYY-MM-DD)") from exc


def resolve_rekap_target_month(today, tahun, bulan, force):
    if tahun or bulan:
        if not tahun or not bulan:
            raise ValueError('Gunakan --tahun dan --bulan bersama-sama.')
        year = int(str(tahun).strip())
        month = int(str(bulan).strip())
        if month < 1 or month > 12:
            raise ValueError(f'Bulan tidak valid: {bulan}')
        return year, month

    if not force and not (1 <= today.day <= 10):
        return None, None
    return shift_month(today.year, today.month, -1)
