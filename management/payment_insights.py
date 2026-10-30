"""Invalid-activity explanation + Meta Account Ads breakdown for payment pages."""
from __future__ import annotations

from datetime import datetime

from management.database import data_mysql, normalize_roi_domain_merge_key


PRODUCT_RULES = (
    (
        ('ad exchange', 'adx', 'ad manager'),
        'adx',
        'Ad Exchange for Content',
        'Google memotong earning Ad Exchange karena sebagian traffic dianggap invalid (GIVT/SIVT) pada inventory AdX.',
    ),
    (
        ('adsense',),
        'adsense',
        'AdSense for Content',
        'Google memotong earning AdSense karena sebagian traffic dianggap invalid pada inventory AdSense.',
    ),
    (
        ('youtube',),
        'youtube',
        'YouTube',
        'Potongan invalid/policy pada channel YouTube. Domain situs tidak relevan di sini.',
    ),
)


def _as_text(value):
    return str(value or '').strip()


def _as_float(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _parse_date(value):
    text = _as_text(value)[:10]
    try:
        return datetime.strptime(text, '%Y-%m-%d').date()
    except ValueError:
        return None


def _activity_months(start_date, end_date):
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    if not start and not end:
        return []
    if start and end and end < start:
        start, end = end, start
    start = start or end
    end = end or start
    months = []
    cursor = start.replace(day=1)
    last = end.replace(day=1)
    while cursor <= last:
        months.append((f'{cursor.year:04d}', f'{cursor.month:02d}'))
        if cursor.month == 12:
            cursor = cursor.replace(year=cursor.year + 1, month=1)
        else:
            cursor = cursor.replace(month=cursor.month + 1)
    return months


def classify_invalid_line(description, txn_type=''):
    blob = f'{_as_text(txn_type)} {_as_text(description)}'.lower()
    product = 'other'
    product_label = 'Tidak diketahui'
    meaning = (
        'Potongan invalid/policy dari Google. CSV Payments tidak menulis domain, '
        'tanggal, atau campaign penyebabnya.'
    )
    for hints, key, label, text in PRODUCT_RULES:
        if any(hint in blob for hint in hints):
            product = key
            product_label = label
            meaning = text
            break
    if 'invalid' in blob or 'ivt' in blob:
        reason_label = 'Invalid Traffic (IVT)'
        reason_detail = (
            'Google menolak sebagian impression/klik karena traffic tidak memenuhi '
            'standar valid. Penyebab resmi (bot, incentivized, mismatch geo, dsb.) '
            'tidak dibuka di file ini.'
        )
    elif 'policy' in blob:
        reason_label = 'Policy / compliance'
        reason_detail = 'Potongan terkait pelanggaran kebijakan iklan atau situs.'
    elif 'adjust' in blob:
        reason_label = 'Adjustment'
        reason_detail = 'Koreksi saldo dari Google, bisa terkait IVT periode sebelumnya.'
    else:
        reason_label = 'Potongan lain'
        reason_detail = meaning
    return {
        'product': product,
        'product_label': product_label,
        'reason_label': reason_label,
        'reason_detail': reason_detail,
        'meaning': meaning,
    }


def _domain_keys(raw):
    raw = _as_text(raw).lower()
    if not raw:
        return []
    keys = {raw}
    parts = [part for part in raw.replace('www.', '').split('.') if part]
    if len(parts) >= 2:
        keys.add(f'{parts[0]}.{parts[1]}')
    merged = normalize_roi_domain_merge_key(raw)
    if merged:
        keys.add(merged)
    return [key for key in keys if key]


def _lookup_mapped(mapping, raw):
    if not mapping:
        return None
    for key in _domain_keys(raw):
        if key in mapping:
            return mapping[key]
    return None


def _account_clause(alias, account_ids):
    ids = [_as_text(item) for item in (account_ids or []) if _as_text(item)]
    if not ids:
        return '', []
    placeholders = ','.join(['%s'] * len(ids))
    return f' AND {alias}.account_id IN ({placeholders})', ids


def _fetch_rows(db, sql, params):
    if not db.execute_query(sql, tuple(params)):
        return []
    return db.fetch_all() or []


def _publisher_source_rows(db, source, account_ids, start_date, end_date, kind='domain'):
    if source == 'adsense':
        if kind == 'country':
            table, date_col, domain_col, rev_col = (
                'data_adsense_country', 'data_adsense_country_tanggal',
                'data_adsense_country_domain', 'data_adsense_country_revenue',
            )
        else:
            table, date_col, domain_col, rev_col = (
                'data_adsense_domain', 'data_adsense_tanggal',
                'data_adsense_domain', 'data_adsense_revenue',
            )
    else:
        if kind == 'country':
            table, date_col, domain_col, rev_col = (
                'data_adx_country', 'data_adx_country_tanggal',
                'data_adx_country_domain', 'data_adx_country_revenue',
            )
        else:
            table, date_col, domain_col, rev_col = (
                'data_adx_domain', 'data_adx_domain_tanggal',
                'data_adx_domain', 'data_adx_domain_revenue',
            )
    extra, extra_params = _account_clause('d', account_ids)
    sql = f"""
        SELECT
            d.{domain_col} AS domain,
            COALESCE(SUM(CAST(d.{rev_col} AS DECIMAL(18,4))), 0) AS revenue
        FROM {table} d
        WHERE DATE(d.{date_col}) BETWEEN %s AND %s
          AND TRIM(COALESCE(d.{domain_col}, '')) <> ''
          {extra}
        GROUP BY d.{domain_col}
    """
    return _fetch_rows(db, sql, [start_date, end_date] + extra_params)


def _publisher_domain_rows(db, source, account_ids, start_date, end_date):
    country_rows = _publisher_source_rows(db, source, account_ids, start_date, end_date, 'country')
    if country_rows:
        return country_rows
    return _publisher_source_rows(db, source, account_ids, start_date, end_date, 'domain')


def _rekap_domain_rows(db, source, account_ids, start_date, end_date):
    months = _activity_months(start_date, end_date)
    if not months:
        return {}
    if source == 'adsense':
        table = 'data_adsense_rekap'
        year_col = 'data_adsense_rekap_tahun'
        month_col = 'data_adsense_rekap_bulan'
        tarik_col = 'data_adsense_rekap_tanggal'
        domain_col = 'data_adsense_rekap_domain'
        rev_col = 'data_adsense_rekap_revenue'
    else:
        table = 'data_adx_rekap'
        year_col = 'data_adx_rekap_tahun'
        month_col = 'data_adx_rekap_bulan'
        tarik_col = 'data_adx_rekap_tanggal'
        domain_col = 'data_adx_rekap_domain'
        rev_col = 'data_adx_rekap_revenue'
    extra, extra_params = _account_clause('r', account_ids)
    totals = {}
    for year, month in months:
        tarik_sql = f"""
            SELECT DISTINCT {tarik_col} AS tanggal_tarik
            FROM {table} r
            WHERE r.{year_col} = %s AND r.{month_col} = %s
            {extra}
            ORDER BY {tarik_col} DESC
        """
        tarik_rows = _fetch_rows(db, tarik_sql, [year, month] + extra_params)
        if not tarik_rows:
            continue
        resolved = tarik_rows[0].get('tanggal_tarik')
        if hasattr(resolved, 'isoformat'):
            resolved = resolved.isoformat()
        resolved = _as_text(resolved)
        if not resolved:
            continue
        sql = f"""
            SELECT
                r.{domain_col} AS domain,
                COALESCE(SUM(CAST(r.{rev_col} AS DECIMAL(18,4))), 0) AS revenue
            FROM {table} r
            WHERE r.{year_col} = %s
              AND r.{month_col} = %s
              AND r.{tarik_col} = %s
              AND TRIM(COALESCE(r.{domain_col}, '')) <> ''
              {extra}
            GROUP BY r.{domain_col}
        """
        for row in _fetch_rows(db, sql, [year, month, resolved] + extra_params):
            domain = _as_text(row.get('domain'))
            if not domain:
                continue
            totals[domain] = totals.get(domain, 0.0) + _as_float(row.get('revenue'))
    return totals


def _campaign_rows(db, start_date, end_date, domain_needles):
    if not domain_needles:
        return []
    like_parts = []
    params = [start_date, end_date]
    for needle in domain_needles:
        like_parts.append('LOWER(c.data_ads_domain) LIKE %s')
        params.append(f'%{needle.lower()}%')
    sql = f"""
        SELECT
            c.data_ads_domain AS domain,
            c.account_ads_id,
            a.account_name,
            COALESCE(SUM(CAST(c.data_ads_spend AS DECIMAL(18,4))), 0) AS spend,
            MAX(c.data_ads_tanggal) AS last_spend_date,
            COUNT(DISTINCT c.data_ads_campaign_id) AS campaign_count
        FROM data_ads_campaign c
        LEFT JOIN master_account_ads a ON a.account_id = c.account_ads_id
        WHERE DATE(c.data_ads_tanggal) BETWEEN %s AND %s
          AND TRIM(COALESCE(c.data_ads_domain, '')) <> ''
          AND ({' OR '.join(like_parts)})
        GROUP BY c.data_ads_domain, c.account_ads_id, a.account_name
    """
    return _fetch_rows(db, sql, params)


def _ivt_rate_label(rate):
    if rate >= 40:
        return 'tinggi'
    if rate >= 15:
        return 'sedang'
    return 'rendah'


def _risk_copy(spend, gap, revenue):
    if spend > 0 and gap > 0:
        return (
            'Ada iklan Meta yang jalan dan revenue harian lebih besar dari rekap resmi. '
            'Ini indikasi terkuat kontributor Invalid Traffic, meski Google tidak menunjuk domain ini secara resmi.'
        )
    if spend > 0:
        return (
            'Ada Account Ads Meta yang jalan di subdomain ini. Traffic berbayar lebih sering kena IVT '
            'ketimbang trafik organik, jadi akun ini perlu dicek kualitas kampanyenya.'
        )
    if gap > 0:
        return (
            'Tidak ada spend Meta, tapi ada selisih revenue harian vs rekap. '
            'Potongan bisa berasal dari trafik organik/direct/referral yang dianggap invalid.'
        )
    if revenue > 0:
        return (
            'Subdomain ini menghasilkan earning tanpa iklan Meta. '
            'Bisa kena GIVT, tapi bukan sinyal utama traffic berbayar.'
        )
    return 'Tidak ada sinyal lokal yang cukup untuk memperkirakan kontribusi IVT.'


def build_payment_insights(source, transactions, account_ids=None, start_date=None, end_date=None):
    db = data_mysql()
    txns = list(transactions or [])
    currency = ''
    invalid_rows = []
    earning_by_product = {}
    for row in txns:
        category = _as_text(row.get('category')).lower()
        desc = _as_text(row.get('description'))
        amount = _as_float(row.get('amount_value'))
        currency = currency or _as_text(row.get('currency'))
        classified = classify_invalid_line(desc, row.get('txn_type'))
        product = classified['product']
        if category == 'earning' or desc.lower().startswith('earnings'):
            earning_by_product[product] = earning_by_product.get(product, 0.0) + abs(amount)
        if category not in ('invalid_activity', 'policy', 'adjustment'):
            if 'invalid' not in desc.lower() and 'policy' not in desc.lower():
                continue
        item = dict(row)
        item.update(classified)
        item['amount_value'] = amount if amount < 0 else -abs(amount)
        invalid_rows.append(item)

    groups = {}
    for row in invalid_rows:
        key = row['product']
        bucket = groups.setdefault(key, {
            'product': key,
            'product_label': row['product_label'],
            'meaning': row['meaning'],
            'reason_label': row['reason_label'],
            'reason_detail': row['reason_detail'],
            'invalid_amount': 0.0,
            'earning_amount': earning_by_product.get(key, 0.0),
            'line_count': 0,
            'lines': [],
        })
        bucket['invalid_amount'] += abs(_as_float(row.get('amount_value')))
        bucket['line_count'] += 1
        bucket['lines'].append({
            'txn_date': row.get('txn_date'),
            'description': row.get('description'),
            'txn_type': row.get('txn_type'),
            'amount_value': row.get('amount_value'),
            'reason_label': row.get('reason_label'),
            'product_label': row.get('product_label'),
        })

    group_list = []
    total_invalid = 0.0
    total_earning = sum(earning_by_product.values())
    for bucket in groups.values():
        invalid_amt = bucket['invalid_amount']
        earning_amt = bucket['earning_amount'] or earning_by_product.get(bucket['product'], 0.0)
        rate = (invalid_amt / earning_amt * 100.0) if earning_amt > 0 else 0.0
        bucket['earning_amount'] = round(earning_amt, 4)
        bucket['invalid_amount'] = round(invalid_amt, 4)
        bucket['ivt_rate'] = round(rate, 2)
        bucket['ivt_rate_label'] = _ivt_rate_label(rate)
        total_invalid += invalid_amt
        group_list.append(bucket)
    group_list.sort(key=lambda item: item['invalid_amount'], reverse=True)

    domain_rows = []
    account_rows = []
    if start_date and end_date:
        daily_rows = _publisher_domain_rows(db, source, account_ids, start_date, end_date)
        rekap_map = _rekap_domain_rows(db, source, account_ids, start_date, end_date)
        daily_by_key = {}
        for row in daily_rows:
            domain = _as_text(row.get('domain'))
            key = normalize_roi_domain_merge_key(domain) or db._normalize_subdomain_key(domain)
            if not key:
                continue
            bucket = daily_by_key.setdefault(key, {
                'domain': key,
                'domain_raw': domain,
                'revenue': 0.0,
                'rekap_revenue': 0.0,
            })
            bucket['revenue'] += _as_float(row.get('revenue'))
            if len(domain) < len(bucket.get('domain_raw') or domain):
                bucket['domain_raw'] = domain
        for raw_domain, revenue in rekap_map.items():
            key = normalize_roi_domain_merge_key(raw_domain) or db._normalize_subdomain_key(raw_domain)
            if not key:
                continue
            bucket = daily_by_key.setdefault(key, {
                'domain': key,
                'domain_raw': raw_domain,
                'revenue': 0.0,
                'rekap_revenue': 0.0,
            })
            bucket['rekap_revenue'] += _as_float(revenue)

        needles = []
        seen_needles = set()
        for key, item in daily_by_key.items():
            for candidate in (key, item.get('domain_raw'), str(item.get('domain_raw') or '').split('.')[0]):
                token = _as_text(candidate).lower()
                if token and token not in seen_needles:
                    seen_needles.add(token)
                    needles.append(token)

        ads_map = {}
        for row in _campaign_rows(db, start_date, end_date, needles):
            key = normalize_roi_domain_merge_key(row.get('domain')) or db._normalize_subdomain_key(row.get('domain'))
            if not key:
                continue
            ads = ads_map.setdefault(key, {
                'accounts': {},
                'spend': 0.0,
                'campaign_count': 0,
                'last_spend_date': '',
            })
            name = _as_text(row.get('account_name')) or _as_text(row.get('account_ads_id')) or '-'
            spend = _as_float(row.get('spend'))
            ads['accounts'][name] = ads['accounts'].get(name, 0.0) + spend
            ads['spend'] += spend
            ads['campaign_count'] += int(row.get('campaign_count') or 0)
            last_spend = row.get('last_spend_date')
            last_spend_s = last_spend.strftime('%Y-%m-%d') if hasattr(last_spend, 'strftime') else _as_text(last_spend)[:10]
            if last_spend_s > ads['last_spend_date']:
                ads['last_spend_date'] = last_spend_s

        status_map = {}
        try:
            if daily_by_key:
                status_map = db.get_ads_last_campaign_status_map(list(daily_by_key.keys()), lookback_days=180) or {}
        except Exception:
            status_map = {}

        weights = []
        for key, item in daily_by_key.items():
            revenue = _as_float(item.get('revenue'))
            rekap_revenue = _as_float(item.get('rekap_revenue'))
            gap = max(0.0, revenue - rekap_revenue) if rekap_revenue > 0 else 0.0
            ads = ads_map.get(key) or {}
            spend = _as_float(ads.get('spend'))
            # Share imported IVT by daily earning. Gap is only a local signal, not Google's official split.
            weight = revenue if revenue > 0 else spend
            weights.append((key, item, ads, spend, revenue, rekap_revenue, gap, weight))

        weight_total = sum(item[-1] for item in weights) or 0.0
        account_bucket = {}
        for key, item, ads, spend, revenue, rekap_revenue, gap, weight in weights:
            share = (weight / weight_total) if weight_total > 0 else 0.0
            estimated_ivt = total_invalid * share
            accounts = ads.get('accounts') or {}
            account_name = ''
            if accounts:
                account_name = sorted(accounts.items(), key=lambda pair: (-pair[1], pair[0].lower()))[0][0]
            status = status_map.get(key) or {}
            if not account_name:
                account_name = _as_text(status.get('account_ads'))
            net = revenue - spend
            roi = ((revenue - spend) / spend * 100.0) if spend > 0 else 0.0
            row = {
                'domain': item.get('domain') or key,
                'account_ads': account_name or '-',
                'account_ads_running': bool(account_name),
                'spend': round(spend, 4),
                'revenue': round(revenue, 4),
                'rekap_revenue': round(rekap_revenue, 4),
                'gap': round(gap, 4),
                'net': round(net, 4),
                'roi': round(roi, 2),
                'estimated_ivt': round(estimated_ivt, 4),
                'ivt_share': round(share * 100.0, 2),
                'campaign_count': int(ads.get('campaign_count') or 0),
                'campaign_status': _as_text(status.get('campaign_status')) or ('ACTIVE' if spend > 0 else '-'),
                'campaign_name': _as_text(status.get('campaign_name')),
                'last_spend_date': ads.get('last_spend_date') or _as_text(status.get('campaign_last_spend_date')),
                'likely_reason': _risk_copy(spend, gap, revenue),
            }
            domain_rows.append(row)
            acc_key = row['account_ads'] if row['account_ads_running'] else 'Tanpa Account Ads'
            acc = account_bucket.setdefault(acc_key, {
                'account_ads': acc_key,
                'running': row['account_ads_running'],
                'domain_count': 0,
                'spend': 0.0,
                'revenue': 0.0,
                'estimated_ivt': 0.0,
                'campaign_count': 0,
                'domains': [],
            })
            acc['domain_count'] += 1
            acc['spend'] += spend
            acc['revenue'] += revenue
            acc['estimated_ivt'] += estimated_ivt
            acc['campaign_count'] += int(row.get('campaign_count') or 0)
            acc['domains'].append(row['domain'])

        domain_rows.sort(key=lambda item: (item['estimated_ivt'], item['spend'], item['revenue']), reverse=True)
        for acc in account_bucket.values():
            spend = acc['spend']
            revenue = acc['revenue']
            acc['spend'] = round(spend, 4)
            acc['revenue'] = round(revenue, 4)
            acc['net'] = round(revenue - spend, 4)
            acc['roi'] = round(((revenue - spend) / spend * 100.0) if spend > 0 else 0.0, 2)
            acc['estimated_ivt'] = round(acc['estimated_ivt'], 4)
            acc['domains'] = sorted(set(acc['domains']))
            account_rows.append(acc)
        account_rows.sort(key=lambda item: (not item['running'], -item['spend'], -item['revenue']))

    top_domains = list(domain_rows)
    return {
        'invalid': {
            'note': (
                'Google tidak menulis domain atau campaign penyebab Invalid Traffic di CSV Payments. '
                'Tab ini membaca rincian Invalid Activity, lalu memperkirakan kontributor dari '
                'selisih revenue harian vs rekap resmi dan Account Ads Meta yang jalan di akun terfilter.'
            ),
            'total_invalid': round(total_invalid, 4),
            'total_earning': round(total_earning, 4),
            'ivt_rate': round((total_invalid / total_earning * 100.0) if total_earning > 0 else 0.0, 2),
            'currency': 'IDR',
            'groups': group_list,
            'lines': [
                {
                    'txn_date': row.get('txn_date'),
                    'account_name': row.get('account_name'),
                    'description': row.get('description'),
                    'product_label': row.get('product_label'),
                    'reason_label': row.get('reason_label'),
                    'reason_detail': row.get('reason_detail'),
                    'amount_value': row.get('amount_value'),
                    'currency': row.get('currency') or currency,
                }
                for row in invalid_rows
            ],
            'suspect_domains': top_domains,
        },
        'account_ads': {
            'note': (
                'Rincian subdomain pada Filter Account, dipetakan ke Account Ads Meta yang menjalankan iklan '
                'pada rentang tanggal yang sama — pola yang sama dengan kolom Account Ads di Monitoring Per Domain.'
            ),
            'currency': 'IDR',
            'accounts': account_rows,
            'domains': domain_rows,
        },
    }
