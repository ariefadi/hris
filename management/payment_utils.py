"""Helpers for AdSense/AdX payment pull and transaction CSV import."""
from __future__ import annotations

import csv
import hashlib
import io
import re
from calendar import monthrange
from datetime import date, datetime


CURRENCY_SYMBOLS = {
    '$': 'USD',
    '€': 'EUR',
    '£': 'GBP',
    '¥': 'JPY',
    '₹': 'INR',
    '₩': 'KRW',
    'Rp': 'IDR',
    'rp': 'IDR',
}

KNOWN_CURRENCIES = {
    'USD', 'IDR', 'EUR', 'GBP', 'JPY', 'SGD', 'AUD', 'INR', 'KRW', 'MYR', 'THB', 'VND', 'PHP',
}

HEADER_ALIASES = {
    'date': {
        'date', 'tanggal', 'transaction date', 'txn date', 'payment date',
        'posted date', 'activity date', 'transaction_date', 'dates',
    },
    'description': {
        'description', 'deskripsi', 'details', 'detail', 'memo', 'note',
        'keterangan', 'transaction description', 'item', 'transaction',
        'activity', 'kegiatan',
    },
    'type': {
        'type', 'jenis', 'transaction type', 'category', 'tipe',
        'activity type',
    },
    'amount': {
        'amount', 'jumlah', 'value', 'net', 'total', 'net amount',
        'payment amount', 'transaction amount', 'amt', 'nominal', 'nilai',
        'local amount', 'gross amount',
    },
    'debit': {'debit', 'debet', 'debits', 'you paid', 'you sent', 'paid'},
    'credit': {'credit', 'kredit', 'credits', 'you received', 'received'},
    'balance': {'balance', 'saldo', 'ending balance'},
    'currency': {'currency', 'mata uang', 'curr', 'currency code'},
    'invoice': {'invoice', 'invoice no', 'invoice number', 'invoice #'},
}

POLICY_HINTS = (
    'invalid activity', 'invalid traffic', 'invalid click', 'invalid impression',
    'ivt', 'invalid', 'policy', 'kebijakan', 'compliance', 'non-compliance',
    'non compliance',
)
PAYMENT_HINTS = ('payment', 'payout', 'disbursement', 'transfer', 'paid')
EARNING_HINTS = ('earning', 'revenue', 'finalized', 'accrued', 'pendapatan')
ADJUSTMENT_HINTS = (
    'adjustment', 'adj', 'rounding', 'advertiser', 'default', 'clawback',
    'refund', 'correction',
)


def _as_text(value):
    if value is None:
        return ''
    if isinstance(value, bytes):
        try:
            return value.decode('utf-8')
        except Exception:
            return value.decode('latin-1', errors='ignore')
    return str(value).strip()


def parse_money(text):
    raw = _as_text(text)
    if not raw or raw in {'-', '—', '–'}:
        return 0.0, ''

    currency = ''
    code_match = re.search(r'\b([A-Z]{3})\b', raw)
    if code_match:
        currency = code_match.group(1)
    for symbol, code in CURRENCY_SYMBOLS.items():
        if symbol in raw:
            currency = currency or code
            break

    negative = raw.startswith('-') or raw.startswith('(') or raw.endswith('-')
    cleaned = re.sub(r'[^0-9,.\-]', '', raw)

    def _is_thousands(sep, text):
        if sep not in text:
            return False
        left, right = text.split(sep, 1)
        return right.isdigit() and len(right) == 3 and left.replace('-', '').replace(sep, '').isdigit()

    if cleaned.count('.') >= 2 and cleaned.count(',') == 0:
        cleaned = cleaned.replace('.', '')
    elif cleaned.count(',') >= 2 and cleaned.count('.') == 0:
        cleaned = cleaned.replace(',', '')
    elif cleaned.count(',') == 1 and cleaned.count('.') == 0:
        if _is_thousands(',', cleaned):
            cleaned = cleaned.replace(',', '')
        else:
            cleaned = cleaned.replace(',', '.')
    elif cleaned.count('.') == 1 and cleaned.count(',') == 0:
        if _is_thousands('.', cleaned):
            cleaned = cleaned.replace('.', '')
    elif ',' in cleaned and '.' in cleaned:
        if cleaned.rfind(',') > cleaned.rfind('.'):
            cleaned = cleaned.replace('.', '').replace(',', '.')
        else:
            cleaned = cleaned.replace(',', '')
    else:
        cleaned = cleaned.replace(',', '')
    cleaned = cleaned.replace('(', '').replace(')', '')
    try:
        value = float(cleaned or 0)
    except (TypeError, ValueError):
        value = 0.0
    if negative and value > 0:
        value = -value
    return round(value, 4), currency


def infer_payment_currency(currency, amount_text='', description=''):
    code = _as_text(currency).upper()
    if code in KNOWN_CURRENCIES:
        return code
    blob = f'{_as_text(amount_text)} {_as_text(description)}'
    if re.search(r'\bUSD\b|\$', blob, re.I):
        return 'USD'
    if re.search(r'\bIDR\b|\bRp\b', blob, re.I):
        return 'IDR'
    if re.search(r'\bEUR\b|€', blob, re.I):
        return 'EUR'
    if re.search(r'\bGBP\b|£', blob, re.I):
        return 'GBP'
    if re.search(r'\bSGD\b', blob, re.I):
        return 'SGD'
    return code or ''


def parse_csv_date(value):
    raw = _as_text(value)
    if not raw:
        return None
    raw = raw.replace('.', '-')
    for fmt in (
        '%Y-%m-%d', '%Y/%m/%d', '%d-%m-%Y', '%d/%m/%Y',
        '%m/%d/%Y', '%d %b %Y', '%d %B %Y', '%b %d, %Y', '%B %d, %Y',
        '%b %d %Y', '%B %d %Y', '%Y-%m-%d %H:%M:%S',
    ):
        try:
            return datetime.strptime(raw[:19] if ' ' in raw else raw, fmt).date().isoformat()
        except ValueError:
            continue
    iso = raw[:10]
    if re.match(r'^\d{4}-\d{2}-\d{2}$', iso):
        return iso
    return None


def parse_payment_resource(name, date_obj=None):
    resource = _as_text(name)
    key = resource.rsplit('/', 1)[-1] if resource else ''
    is_youtube = key.startswith('youtube')
    is_unpaid = key.endswith('unpaid')
    payment_date = None
    if isinstance(date_obj, dict):
        year = date_obj.get('year')
        month = date_obj.get('month')
        day = date_obj.get('day')
        if year and month and day:
            payment_date = f'{int(year):04d}-{int(month):02d}-{int(day):02d}'
    if not is_unpaid:
        date_part = key.replace('youtube-', '')
        if re.match(r'^\d{8}$', date_part):
            payment_date = payment_date or f'{date_part[0:4]}-{date_part[4:6]}-{date_part[6:8]}'
            key = ('youtube-' if is_youtube else '') + payment_date
        elif re.match(r'^\d{4}-\d{2}-\d{2}$', date_part):
            payment_date = payment_date or date_part
    return {
        'payment_name': resource,
        'payment_key': key or 'unknown',
        'channel': 'youtube' if is_youtube else 'adsense',
        'status': 'unpaid' if is_unpaid else 'paid',
        'payment_date': payment_date,
    }


def classify_transaction(txn_type, description, amount=0):
    blob = f'{_as_text(txn_type)} {_as_text(description)}'.lower()
    if any(hint in blob for hint in POLICY_HINTS):
        return 'invalid_activity'
    if any(hint in blob for hint in PAYMENT_HINTS):
        return 'payment'
    if any(hint in blob for hint in EARNING_HINTS):
        return 'earning'
    if any(hint in blob for hint in ADJUSTMENT_HINTS):
        return 'adjustment'
    try:
        numeric = float(amount or 0)
    except (TypeError, ValueError):
        numeric = 0.0
    if numeric < 0:
        return 'adjustment'
    return 'other'


def earning_period_for_payment(payment_date):
    """AdSense/AdX payout around the 21st usually covers the previous calendar month."""
    parsed = parse_csv_date(payment_date)
    if not parsed:
        return None, None
    year, month, _day = [int(part) for part in parsed.split('-')]
    if month == 1:
        start = date(year - 1, 12, 1)
    else:
        start = date(year, month - 1, 1)
    end = date(start.year, start.month, monthrange(start.year, start.month)[1])
    return start.isoformat(), end.isoformat()


def normalize_header(value):
    text = _as_text(value).lower().replace('_', ' ').replace('#', ' ')
    text = re.sub(r'[\(\)\[\]\{\}:]', ' ', text)
    return re.sub(r'\s+', ' ', text).strip()


def _header_field(norm):
    if not norm:
        return None
    for field, aliases in HEADER_ALIASES.items():
        if norm in aliases:
            return field
    if norm.startswith('amount') or norm.startswith('jumlah') or ' amount' in f' {norm}':
        return 'amount'
    token = norm.upper()
    if token in KNOWN_CURRENCIES:
        return 'amount'
    parts = norm.split()
    if parts and parts[-1].upper() in KNOWN_CURRENCIES and ('amount' in norm or 'jumlah' in norm or len(parts) == 1):
        return 'amount'
    return None


def map_csv_headers(headers):
    mapped = {}
    for idx, header in enumerate(headers or []):
        field = _header_field(normalize_header(header))
        if field and field not in mapped:
            mapped[field] = idx
    return mapped


def filename_period_date(filename):
    match = re.search(r'(20\d{2})(0[1-9]|1[0-2])', _as_text(filename))
    if not match:
        return None
    return f'{match.group(1)}-{match.group(2)}-01'


def normalize_source_file(name):
    text = _as_text(name)
    return re.sub(r'\s*\(\d+\)\s*(?=\.[^.]+$)', '', text).strip()


def infer_amount_column(rows, mapped, header_idx):
    if mapped.get('amount') is not None or mapped.get('debit') is not None or mapped.get('credit') is not None:
        return mapped
    data_rows = rows[header_idx + 1:]
    if not data_rows:
        return mapped
    used = set(idx for idx in mapped.values() if idx is not None)
    width = max((len(row) for row in data_rows), default=0)
    best_idx = None
    best_score = 0
    for idx in range(width):
        if idx in used:
            continue
        score = 0
        for row in data_rows:
            raw = _as_text(_cell(row, idx))
            if not raw or not re.search(r'\d', raw):
                continue
            value, currency = parse_money(raw)
            score += 1
            if currency:
                score += 1
            if value:
                score += 2
        if score > best_score:
            best_score = score
            best_idx = idx
    if best_idx is not None and best_score > 0:
        mapped = dict(mapped)
        mapped['amount'] = best_idx
    return mapped


def _read_rows(text, delimiter=None):
    sample = text[:4096]
    if delimiter:
        reader = csv.reader(io.StringIO(text), delimiter=delimiter)
        return [row for row in reader]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=',;\t|')
    except csv.Error:
        dialect = csv.excel
    reader = csv.reader(io.StringIO(text), dialect)
    return [row for row in reader]


def _header_row_index(rows):
    best_idx = 0
    best_score = -1
    for idx, row in enumerate(rows[:40]):
        mapped = map_csv_headers(row)
        score = len(mapped)
        if 'date' in mapped:
            score += 2
        if 'description' in mapped:
            score += 2
        if 'amount' in mapped or 'debit' in mapped or 'credit' in mapped:
            score += 3
        if score > best_score:
            best_score = score
            best_idx = idx
    return best_idx if best_score >= 2 else None


def _nonzero_count(items):
    count = 0
    for item in items or []:
        try:
            if abs(float(item.get('amount_value') or 0)) > 0:
                count += 1
        except (TypeError, ValueError):
            continue
    return count


def _merge_split_us_dates(rows):
    fixed = []
    for row in rows or []:
        cells = list(row or [])
        if len(cells) >= 3:
            left = _as_text(cells[0])
            right = _as_text(cells[1])
            if re.match(r'^[A-Za-z]{3,9}\s+\d{1,2}$', left) and re.match(r'^(19|20)\d{2}$', right):
                cells = [f'{left}, {right}'] + cells[2:]
        fixed.append(cells)
    return fixed


def _rows_to_items(rows, filename=''):
    if not rows:
        return []
    rows = _merge_split_us_dates(rows)
    header_idx = _header_row_index(rows)
    if header_idx is None:
        return []
    mapped = infer_amount_column(rows, map_csv_headers(rows[header_idx]), header_idx)
    fallback_date = filename_period_date(filename)
    items = []
    for row in rows[header_idx + 1:]:
        item = _row_from_mapped(row, mapped, fallback_date=fallback_date)
        if item:
            items.append(item)
    return items


def parse_payment_csv(file_obj, filename=''):
    name = _as_text(filename).lower()
    if name.endswith(('.xlsx', '.xls')):
        return _parse_excel_payment_file(file_obj, filename)
    raw = file_obj.read()
    if isinstance(raw, bytes):
        try:
            text = raw.decode('utf-8-sig')
        except UnicodeDecodeError:
            try:
                text = raw.decode('utf-16')
            except UnicodeDecodeError:
                text = raw.decode('latin-1', errors='ignore')
    else:
        text = str(raw or '')
    candidates = [_read_rows(text)]
    for delimiter in (',', ';', '\t', '|'):
        candidates.append(_read_rows(text, delimiter=delimiter))
    best_items = []
    best_score = -1
    for rows in candidates:
        items = _rows_to_items(rows, filename=filename)
        score = (_nonzero_count(items) * 10) + len(items)
        if score > best_score:
            best_score = score
            best_items = items
    if not best_items:
        raise ValueError(
            'Header CSV tidak dikenali. Pastikan ada kolom Description/Item dan Amount.'
        )
    return best_items


def _parse_excel_payment_file(file_obj, filename):
    try:
        import pandas as pd
    except Exception as exc:
        raise ValueError('File Excel membutuhkan pandas. Unggah CSV dari halaman Payments/Transactions.') from exc
    try:
        df = pd.read_excel(file_obj)
    except Exception as exc:
        raise ValueError(f'Gagal membaca file Excel {filename}: {exc}') from exc
    if df is None or df.empty:
        return []
    values = df.fillna('').astype(str).values.tolist()
    headers = [str(col) for col in list(df.columns)]
    rows = [headers] + values
    mapped = infer_amount_column(rows, map_csv_headers(headers), 0)
    if 'description' not in mapped and 'amount' not in mapped and 'date' not in mapped:
        raise ValueError('Header Excel tidak dikenali. Gunakan export CSV Transactions/Payments.')
    fallback_date = filename_period_date(filename)
    parsed_rows = []
    for row in values:
        item = _row_from_mapped(row, mapped, fallback_date=fallback_date)
        if item:
            parsed_rows.append(item)
    return parsed_rows


def _cell(row, idx):
    if idx is None or idx < 0 or idx >= len(row):
        return ''
    return row[idx]


def _row_from_mapped(row, mapped, fallback_date=None):
    if not row or not any(_as_text(cell) for cell in row):
        return None
    txn_date = parse_csv_date(_cell(row, mapped.get('date'))) or fallback_date
    description = _as_text(_cell(row, mapped.get('description')))
    txn_type = _as_text(_cell(row, mapped.get('type')))
    currency = _as_text(_cell(row, mapped.get('currency'))).upper()
    invoice_no = _as_text(_cell(row, mapped.get('invoice')))
    amount, amount_currency = parse_money(_cell(row, mapped.get('amount')))
    debit, debit_currency = parse_money(_cell(row, mapped.get('debit')))
    credit, credit_currency = parse_money(_cell(row, mapped.get('credit')))
    balance, balance_currency = parse_money(_cell(row, mapped.get('balance')))
    if 'amount' not in mapped:
        amount = credit - debit
    if not description:
        # Google account activity sometimes has description in the first non-date cell.
        for idx, cell in enumerate(row):
            if idx == mapped.get('date') or idx == mapped.get('amount'):
                continue
            text = _as_text(cell)
            if text and not re.fullmatch(r'[-0-9.,$€£¥Rp ]+', text):
                description = text
                break
    currency = currency or amount_currency or debit_currency or credit_currency or balance_currency
    if not txn_date and not description and not txn_type and amount == 0 and debit == 0 and credit == 0:
        return None
    category = classify_transaction(txn_type, description, amount)
    return {
        'txn_date': txn_date,
        'txn_type': txn_type,
        'description': description,
        'category': category,
        'amount_value': amount,
        'debit_value': debit,
        'credit_value': credit,
        'balance_value': balance if 'balance' in mapped else None,
        'currency': currency,
        'invoice_no': invoice_no,
    }


def transaction_row_hash(source, account_id, item):
    payload = '|'.join([
        _as_text(source).lower(),
        _as_text(account_id),
        _as_text((item or {}).get('txn_date')),
        _as_text((item or {}).get('txn_type')),
        _as_text((item or {}).get('description')),
        '%.4f' % float((item or {}).get('amount_value') or 0),
        '%.4f' % float((item or {}).get('debit_value') or 0),
        '%.4f' % float((item or {}).get('credit_value') or 0),
    ])
    return hashlib.sha1(payload.encode('utf-8')).hexdigest()


def fetch_adsense_payments_for_mail(user_mail):
    from .utils_adsense import get_user_adsense_client

    client_result = get_user_adsense_client(user_mail)
    if not client_result.get('status'):
        return client_result

    service = client_result.get('service')
    if service is None:
        return {'status': False, 'error': 'AdSense client tidak tersedia'}

    try:
        accounts_resp = service.accounts().list().execute() or {}
    except Exception as exc:
        return {'status': False, 'error': f'Gagal mengambil akun AdSense: {exc}'}

    accounts = accounts_resp.get('accounts') or []
    if not accounts:
        return {'status': False, 'error': 'Tidak ada akun AdSense pada kredensial ini'}

    payments = []
    errors = []
    for account in accounts:
        parent = _as_text(account.get('name'))
        display_name = _as_text(account.get('displayName') or account.get('name'))
        pub_id = parent.rsplit('/', 1)[-1] if parent else ''
        try:
            resp = service.accounts().payments().list(parent=parent).execute() or {}
        except Exception as exc:
            errors.append(f'{pub_id or parent}: {exc}')
            continue
        for payment in resp.get('payments') or []:
            meta = parse_payment_resource(payment.get('name'), payment.get('date'))
            amount_text = _as_text(payment.get('amount'))
            amount_value, currency = parse_money(amount_text)
            payments.append({
                'publisher_account': pub_id,
                'account_display': display_name,
                'amount_text': amount_text,
                'amount_value': amount_value,
                'currency': currency,
                **meta,
            })
    if not payments and errors:
        return {'status': False, 'error': '; '.join(errors), 'data': []}
    return {
        'status': True,
        'data': payments,
        'errors': errors,
        'account_count': len(accounts),
    }


