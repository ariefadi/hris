"""Persistence helpers for publisher payment + transaction tables."""
from __future__ import annotations

from datetime import datetime

from management.database import data_mysql
from management.adsense_currency import convert_to_idr, get_idr_rate
from management.payment_utils import (
    earning_period_for_payment,
    infer_payment_currency,
    normalize_source_file,
    transaction_row_hash,
)


PAYMENT_TABLE = 'data_publisher_payment'
TXN_TABLE = 'data_publisher_payment_txn'


def _now():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _db(db=None):
    return db or data_mysql()


def ensure_payment_tables(db=None):
    db = _db(db)
    payment_sql = f"""
        CREATE TABLE IF NOT EXISTS {PAYMENT_TABLE} (
            payment_id BIGINT NOT NULL AUTO_INCREMENT,
            source VARCHAR(16) NOT NULL,
            account_id VARCHAR(64) NOT NULL,
            account_name VARCHAR(255) DEFAULT NULL,
            user_mail VARCHAR(255) DEFAULT NULL,
            publisher_account VARCHAR(128) DEFAULT NULL,
            payment_name VARCHAR(255) NOT NULL,
            payment_key VARCHAR(64) NOT NULL,
            channel VARCHAR(16) NOT NULL DEFAULT 'adsense',
            status VARCHAR(16) NOT NULL,
            payment_date DATE DEFAULT NULL,
            amount_text VARCHAR(64) DEFAULT NULL,
            amount_value DECIMAL(18,4) NOT NULL DEFAULT 0,
            currency VARCHAR(16) DEFAULT NULL,
            source_kind VARCHAR(16) NOT NULL DEFAULT 'api',
            synced_at DATETIME DEFAULT NULL,
            mdd DATETIME DEFAULT NULL,
            PRIMARY KEY (payment_id),
            UNIQUE KEY uk_publisher_payment (source, account_id, payment_key, channel),
            KEY idx_publisher_payment_status (source, status, payment_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    txn_sql = f"""
        CREATE TABLE IF NOT EXISTS {TXN_TABLE} (
            txn_id BIGINT NOT NULL AUTO_INCREMENT,
            source VARCHAR(16) NOT NULL,
            account_id VARCHAR(64) NOT NULL,
            account_name VARCHAR(255) DEFAULT NULL,
            user_mail VARCHAR(255) DEFAULT NULL,
            txn_date DATE DEFAULT NULL,
            txn_type VARCHAR(64) DEFAULT NULL,
            description TEXT,
            category VARCHAR(32) NOT NULL DEFAULT 'other',
            amount_value DECIMAL(18,4) NOT NULL DEFAULT 0,
            debit_value DECIMAL(18,4) NOT NULL DEFAULT 0,
            credit_value DECIMAL(18,4) NOT NULL DEFAULT 0,
            balance_value DECIMAL(18,4) DEFAULT NULL,
            currency VARCHAR(16) DEFAULT NULL,
            invoice_no VARCHAR(64) DEFAULT NULL,
            source_file VARCHAR(255) DEFAULT NULL,
            row_hash CHAR(40) NOT NULL,
            imported_at DATETIME DEFAULT NULL,
            PRIMARY KEY (txn_id),
            UNIQUE KEY uk_publisher_payment_txn (row_hash),
            KEY idx_publisher_payment_txn (source, account_id, txn_date),
            KEY idx_publisher_payment_txn_cat (source, category)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    if not db.execute_query(payment_sql):
        raise RuntimeError(db.last_error or f'Gagal membuat tabel {PAYMENT_TABLE}')
    if not db.execute_query(txn_sql):
        raise RuntimeError(db.last_error or f'Gagal membuat tabel {TXN_TABLE}')
    db.commit()
    return True


def _account_filter_sql(alias, account_ids):
    ids = [str(item).strip() for item in (account_ids or []) if str(item).strip()]
    if not ids:
        return '', []
    placeholders = ','.join(['%s'] * len(ids))
    return f' AND {alias}.account_id IN ({placeholders})', ids


def list_payment_credentials(source, admin=None):
    db = _db()
    admin = admin or {}
    is_super = str(admin.get('super_st') or '1') != '0'
    user_id = admin.get('user_id')
    if source == 'adsense':
        if is_super:
            rs = db.get_all_adsense_account_data()
        else:
            rs = db.get_all_adsense_account_data_user(user_id)
    else:
        if is_super:
            rs = db.get_all_adx_account_data()
        else:
            rs = db.get_all_adx_account_data_user(user_id)

    rows = (rs or {}).get('data') if isinstance(rs, dict) and rs.get('status') else []
    if not rows:
        if is_super:
            creds = db.get_all_app_credentials()
        else:
            creds = db.get_all_app_credentials_user(user_id)
        rows = (creds or {}).get('data') if isinstance(creds, dict) and creds.get('status') else []

    accounts = []
    seen = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        account_id = str(row.get('account_id') or '').strip()
        if not account_id or account_id in seen:
            continue
        seen.add(account_id)
        accounts.append({
            'account_id': account_id,
            'account_name': str(row.get('account_name') or '').strip() or account_id,
            'user_mail': str(row.get('user_mail') or '').strip(),
            'network_code': str(row.get('network_code') or '').strip(),
            'publisher_account': str(
                row.get('publisher_account')
                or row.get('adsense_id')
                or row.get('publisher_id')
                or row.get('network_code')
                or ''
            ).strip(),
        })
    accounts.sort(key=lambda item: (item.get('account_name') or '').lower())
    return accounts


def upsert_payments(source, account, payments, source_kind='api', db=None):
    db = _db(db)
    ensure_payment_tables(db)
    now = _now()
    saved = 0
    sql = f"""
        INSERT INTO {PAYMENT_TABLE} (
            source, account_id, account_name, user_mail, publisher_account,
            payment_name, payment_key, channel, status, payment_date,
            amount_text, amount_value, currency, source_kind, synced_at, mdd
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            account_name = VALUES(account_name),
            user_mail = VALUES(user_mail),
            publisher_account = VALUES(publisher_account),
            payment_name = VALUES(payment_name),
            status = VALUES(status),
            payment_date = VALUES(payment_date),
            amount_text = VALUES(amount_text),
            amount_value = VALUES(amount_value),
            currency = VALUES(currency),
            source_kind = VALUES(source_kind),
            synced_at = VALUES(synced_at),
            mdd = VALUES(mdd)
    """
    for payment in payments or []:
        ok = db.execute_query(sql, (
            source,
            account.get('account_id'),
            account.get('account_name'),
            account.get('user_mail'),
            payment.get('publisher_account') or account.get('network_code') or '',
            payment.get('payment_name') or payment.get('payment_key'),
            payment.get('payment_key'),
            payment.get('channel') or source,
            payment.get('status') or 'paid',
            payment.get('payment_date'),
            payment.get('amount_text'),
            float(payment.get('amount_value') or 0),
            payment.get('currency') or '',
            source_kind,
            now,
            now,
        ))
        if ok:
            saved += 1
    db.commit()
    return saved


def _delete_previous_import(db, source, account_id, source_file):
    normalized = normalize_source_file(source_file)
    stem = normalized.rsplit('.', 1)[0] if normalized else ''
    if not account_id or not stem:
        return
    db.execute_query(
        f"""
        DELETE FROM {TXN_TABLE}
        WHERE source = %s
          AND account_id = %s
          AND (
            source_file = %s
            OR source_file = %s
            OR source_file LIKE %s
          )
        """,
        (source, account_id, source_file, normalized, f'{stem}%'),
    )
    db.commit()


def insert_transactions(source, account, items, source_file='', db=None):
    db = _db(db)
    ensure_payment_tables(db)
    now = _now()
    inserted = 0
    skipped = 0
    _delete_previous_import(db, source, account.get('account_id'), source_file)
    sql = f"""
        INSERT INTO {TXN_TABLE} (
            source, account_id, account_name, user_mail, txn_date, txn_type,
            description, category, amount_value, debit_value, credit_value,
            balance_value, currency, invoice_no, source_file, row_hash, imported_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            account_name = VALUES(account_name),
            category = VALUES(category),
            amount_value = VALUES(amount_value),
            debit_value = VALUES(debit_value),
            credit_value = VALUES(credit_value),
            currency = VALUES(currency),
            imported_at = VALUES(imported_at),
            source_file = VALUES(source_file)
    """
    for item in items or []:
        row_hash = transaction_row_hash(source, account.get('account_id'), item)
        ok = db.execute_query(sql, (
            source,
            account.get('account_id'),
            account.get('account_name'),
            account.get('user_mail'),
            item.get('txn_date'),
            item.get('txn_type') or '',
            item.get('description') or '',
            item.get('category') or 'other',
            float(item.get('amount_value') or 0),
            float(item.get('debit_value') or 0),
            float(item.get('credit_value') or 0),
            item.get('balance_value'),
            item.get('currency') or '',
            item.get('invoice_no') or '',
            source_file[:255],
            row_hash,
            now,
        ))
        if not ok:
            continue
        rowcount = getattr(db.cur_hris, 'rowcount', 0) or 0
        if rowcount == 1:
            inserted += 1
        else:
            skipped += 1
    db.commit()
    return inserted, skipped


def list_payments(source, account_ids=None, status=None, start_date=None, end_date=None, db=None):
    db = _db(db)
    ensure_payment_tables(db)
    sql = f"""
        SELECT *
        FROM {PAYMENT_TABLE} p
        WHERE p.source = %s
    """
    params = [source]
    extra, extra_params = _account_filter_sql('p', account_ids)
    sql += extra
    params.extend(extra_params)
    if status:
        sql += ' AND p.status = %s'
        params.append(status)
    if start_date:
        sql += ' AND (p.payment_date IS NULL OR p.payment_date >= %s)'
        params.append(start_date)
    if end_date:
        sql += ' AND (p.payment_date IS NULL OR p.payment_date <= %s)'
        params.append(end_date)
    sql += ' ORDER BY p.status ASC, p.payment_date DESC, p.account_name ASC'
    if not db.execute_query(sql, tuple(params)):
        return []
    return db.fetch_all() or []


def _activity_months(start_date, end_date):
    from datetime import date as date_cls
    from datetime import datetime as datetime_cls

    def _parse(value):
        text = str(value or '').strip()[:10]
        try:
            return datetime_cls.strptime(text, '%Y-%m-%d').date()
        except ValueError:
            return None

    start = _parse(start_date)
    end = _parse(end_date)
    if not start and not end:
        return []
    if start and end and end < start:
        start, end = end, start
    start = start or end
    end = end or start
    months = []
    cursor = date_cls(start.year, start.month, 1)
    last = date_cls(end.year, end.month, 1)
    while cursor <= last:
        months.append(f'{cursor.year:04d}{cursor.month:02d}')
        if cursor.month == 12:
            cursor = date_cls(cursor.year + 1, 1, 1)
        else:
            cursor = date_cls(cursor.year, cursor.month + 1, 1)
    return months


def list_transactions(
    source,
    account_ids=None,
    start_date=None,
    end_date=None,
    category=None,
    policy_only=False,
    db=None,
):
    db = _db(db)
    ensure_payment_tables(db)
    sql = f"""
        SELECT *
        FROM {TXN_TABLE} t
        WHERE t.source = %s
    """
    params = [source]
    extra, extra_params = _account_filter_sql('t', account_ids)
    sql += extra
    params.extend(extra_params)
    months = _activity_months(start_date, end_date)
    if start_date or end_date:
        date_parts = ['t.txn_date IS NULL']
        range_parts = []
        if start_date:
            range_parts.append('t.txn_date >= %s')
            params.append(start_date)
        if end_date:
            # Ending balance Google biasanya tanggal 1 bulan berikutnya.
            range_parts.append('t.txn_date <= DATE_ADD(%s, INTERVAL 1 DAY)')
            params.append(end_date)
        if range_parts:
            date_parts.append('(' + ' AND '.join(range_parts) + ')')
        for month in months:
            date_parts.append('t.source_file LIKE %s')
            params.append(f'%{month}%')
        sql += ' AND (' + ' OR '.join(date_parts) + ')'
    if category:
        sql += ' AND t.category = %s'
        params.append(category)
    if policy_only:
        sql += " AND t.category IN ('invalid_activity', 'policy', 'adjustment')"
    sql += ' ORDER BY t.txn_date DESC, t.txn_id DESC'
    if not db.execute_query(sql, tuple(params)):
        return []
    return db.fetch_all() or []


def _sum_estimated_revenue(db, source, account_id, start_date, end_date):
    if source == 'adsense':
        sql = """
            SELECT COALESCE(SUM(CAST(data_adsense_revenue AS DECIMAL(18,4))), 0) AS total_revenue
            FROM data_adsense_domain
            WHERE account_id = %s
              AND DATE(data_adsense_tanggal) BETWEEN %s AND %s
        """
    else:
        sql = """
            SELECT COALESCE(SUM(CAST(data_adx_domain_revenue AS DECIMAL(18,4))), 0) AS total_revenue
            FROM data_adx_domain
            WHERE account_id = %s
              AND DATE(data_adx_domain_tanggal) BETWEEN %s AND %s
        """
    try:
        if not db.execute_query(sql, (account_id, start_date, end_date)):
            return None
        row = db.cur_hris.fetchone() or {}
        return float(row.get('total_revenue') or 0)
    except Exception:
        return None


def _row_to_idr(item, amount_keys):
    currency = infer_payment_currency(
        item.get('currency'),
        item.get('amount_text'),
        item.get('description'),
    ) or 'IDR'
    original_amount = float(item.get('amount_value') or 0)
    rate = get_idr_rate(currency)
    converted = currency != 'IDR' and rate != 1
    item['currency_original'] = currency
    item['amount_original'] = original_amount
    item['fx_rate'] = rate
    item['fx_converted'] = converted
    for key in amount_keys:
        if key in item and item.get(key) is not None:
            item[key] = round(convert_to_idr(item.get(key), currency), 4)
    item['currency'] = 'IDR'
    if converted:
        item['amount_text'] = ''
    return item


def enrich_payments_with_estimates(source, rows, db=None):
    db = _db(db)
    enriched = []
    for row in rows or []:
        item = dict(row)
        start_date, end_date = earning_period_for_payment(item.get('payment_date'))
        estimated = None
        if item.get('status') == 'paid' and start_date and end_date:
            estimated = _sum_estimated_revenue(db, source, item.get('account_id'), start_date, end_date)
        _row_to_idr(item, ('amount_value',))
        paid = float(item.get('amount_value') or 0)
        item['earning_period_start'] = start_date
        item['earning_period_end'] = end_date
        item['estimated_revenue'] = estimated
        item['estimated_gap'] = None if estimated is None else round(float(estimated) - paid, 4)
        synced_at = item.get('synced_at') or item.get('mdd')
        if hasattr(synced_at, 'strftime'):
            item['synced_at'] = synced_at.strftime('%Y-%m-%d %H:%M:%S')
        payment_date = item.get('payment_date')
        if hasattr(payment_date, 'strftime'):
            item['payment_date'] = payment_date.strftime('%Y-%m-%d')
        enriched.append(item)
    return enriched


def serialize_txn_rows(rows):
    out = []
    for row in rows or []:
        item = dict(row)
        for key in ('txn_date', 'imported_at'):
            value = item.get(key)
            if hasattr(value, 'strftime'):
                item[key] = value.strftime('%Y-%m-%d %H:%M:%S' if key == 'imported_at' else '%Y-%m-%d')
        _row_to_idr(item, ('amount_value', 'debit_value', 'credit_value', 'balance_value'))
        out.append(item)
    return out


def _csv_totals(transactions):
    unpaid = 0.0
    paid = 0.0
    earning = 0.0
    deduction = 0.0
    for row in transactions or []:
        category = str(row.get('category') or '')
        desc = str(row.get('description') or '').lower()
        amount = float(row.get('amount_value') or 0)
        if category == 'payment' or desc.startswith('automatic payment') or desc.startswith('payment'):
            paid += abs(amount)
        elif 'ending balance' in desc:
            unpaid += amount
        elif category == 'earning' or desc.startswith('earnings'):
            earning += abs(amount)
        if category in ('invalid_activity', 'policy', 'adjustment') or 'invalid' in desc:
            deduction += amount if amount < 0 else -abs(amount)
    return {
        'unpaid': round(unpaid, 4),
        'paid': round(paid, 4),
        'earning': round(earning, 4),
        'deduction': round(deduction, 4),
    }


def _compare_row(label, api_value, csv_value, meaning, csv_only=False):
    has_api = api_value is not None
    has_csv = csv_value is not None
    gap = None
    match = 'csv_only' if csv_only else 'missing'
    if has_api and has_csv:
        gap = round(float(csv_value) - float(api_value), 4)
        match = 'ok' if abs(gap) < 1 else 'gap'
    elif has_api:
        match = 'api_only'
    elif has_csv:
        match = 'csv_only' if csv_only else 'csv_only'
    return {
        'label': label,
        'api_value': api_value,
        'csv_value': csv_value,
        'gap': gap,
        'match': match,
        'meaning': meaning,
    }


def build_payment_summary(payments, transactions):
    api_unpaid = 0.0
    api_paid = 0.0
    currencies = set()
    for row in payments or []:
        amount = float(row.get('amount_value') or 0)
        if str(row.get('status') or '') == 'unpaid':
            api_unpaid += amount
        else:
            api_paid += amount
        if row.get('currency'):
            currencies.add(str(row.get('currency')))
    csv = _csv_totals(transactions)
    for row in transactions or []:
        if row.get('currency'):
            currencies.add(str(row.get('currency')))
    has_api = bool(payments)
    has_csv = bool(transactions)
    unpaid = api_unpaid if has_api else csv['unpaid']
    paid = api_paid if has_api else csv['paid']
    last_sync = ''
    for row in payments or []:
        value = str(row.get('synced_at') or '')
        if value > last_sync:
            last_sync = value
    for row in transactions or []:
        value = str(row.get('imported_at') or '')
        if value > last_sync:
            last_sync = value
    compare = [
        _compare_row(
            'Unpaid / saldo berjalan',
            round(api_unpaid, 4) if has_api else None,
            csv['unpaid'] if has_csv else None,
            'API memakai payments/unpaid. CSV memakai Ending balance periode yang sama.',
        ),
        _compare_row(
            'Sudah dibayar',
            round(api_paid, 4) if has_api else None,
            csv['paid'] if has_csv else None,
            'API memakai payments/yyyy-MM-dd. CSV memakai Automatic payment.',
        ),
        _compare_row(
            'Earning periode',
            None,
            csv['earning'] if has_csv else None,
            'Hanya ada di CSV Transactions. API AdSense tidak merinci earning harian.',
            csv_only=True,
        ),
        _compare_row(
            'Potongan Invalid Traffic',
            None,
            csv['deduction'] if has_csv else None,
            'Alasan CSV masih berguna: API hanya total bayar, tidak merinci IVT/policy.',
            csv_only=True,
        ),
    ]
    return {
        'unpaid_total': round(unpaid, 4),
        'paid_total': round(paid, 4),
        'deduction_total': csv['deduction'],
        'payment_count': len(payments or []),
        'transaction_count': len(transactions or []),
        'currency': 'IDR',
        'fx_note': 'Semua nominal ditampilkan dalam IDR. Akun USD/mata uang lain dikonversi otomatis.',
        'last_sync': last_sync,
        'has_api': has_api,
        'has_csv': has_csv,
        'api_unpaid': round(api_unpaid, 4),
        'api_paid': round(api_paid, 4),
        'csv_unpaid': csv['unpaid'],
        'csv_paid': csv['paid'],
        'csv_earning': csv['earning'],
        'compare': compare,
    }
