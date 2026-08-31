"""Database helpers for Report Account AdSense (app_credentials based)."""

# AdX revenue: gunakan data_adx_country (sumber sama dengan menu monitoring_domain).
# data_adx_domain sering tidak lengkap per tanggal meskipun data country sudah ada.
_ADX_REV_TABLE = 'data_adx_country'
_ADX_REV_DATE_COL = 'data_adx_country_tanggal'
_ADX_REV_AMOUNT_COL = 'data_adx_country_revenue'
_ADX_REV_DOMAIN_COL = 'data_adx_country_domain'


def _report_cred_adsense_fetch_accounts(db, account_q=None, user_id=None, is_super=True):
    params = []
    lines = [
        "SELECT ac.account_id, ac.account_name, ac.user_mail, ac.network_code",
        "FROM app_credentials ac",
    ]
    if not is_super and user_id:
        lines.append("INNER JOIN app_credentials_assign ca ON ca.account_id = ac.account_id")
    lines.append("WHERE COALESCE(ac.is_active, '1') = '1'")
    if not is_super and user_id:
        lines.append("AND ca.user_id = %s")
        params.append(user_id)
    q = str(account_q or '').strip()
    if q:
        lines.append(
            "AND (ac.account_name LIKE %s OR ac.user_mail LIKE %s OR CAST(ac.account_id AS CHAR) LIKE %s)"
        )
        like = f"%{q}%"
        params.extend([like, like, like])
    lines.append("ORDER BY ac.account_name ASC")
    if not db.execute_query("\n".join(lines), tuple(params) if params else None):
        return []
    rows = db.cur_hris.fetchall() or []
    accounts = []
    for row in rows:
        cid = str(row.get('account_id') or '').strip()
        if not cid:
            continue
        accounts.append({
            'account_id': cid,
            'account_key': cid,
            'account_name': str(row.get('account_name') or '').strip() or cid,
            'user_mail': str(row.get('user_mail') or '').strip(),
            'network_code': str(row.get('network_code') or '').strip(),
        })
    return accounts


def search_report_account_adsense_suggest(db, q, limit=20, user_id=None, is_super=True):
    q = str(q or '').strip()
    if len(q) < 3:
        return {'status': True, 'data': []}
    try:
        limit = max(1, min(int(limit or 20), 50))
        accounts = _report_cred_adsense_fetch_accounts(db, q, user_id, is_super)[:limit]
        data = []
        seen = set()
        for acct in accounts:
            name = acct.get('account_name') or ''
            if not name or name.lower() in seen:
                continue
            seen.add(name.lower())
            email = acct.get('user_mail') or ''
            text = name + (f' ({email})' if email else '')
            data.append({
                'account_id': acct.get('account_id'),
                'account_name': name,
                'user_mail': email,
                'text': text,
            })
        return {'status': True, 'data': data}
    except Exception as e:
        return {'status': False, 'data': str(e)}


def _report_cred_adsense_sum_cred_map(by_cred_map, cred_id):
    bucket = (by_cred_map or {}).get(str(cred_id)) or {}
    return round(sum(float(v or 0) for v in bucket.values()), 2)


def _report_cred_adsense_fetch_revenue_by_account(
    db, table, date_col, revenue_col, start_date, end_date, account_ids=None,
):
    """Sum revenue per account_id — tanpa double-count dari normalisasi domain ganda."""
    params = [start_date, end_date]
    where_extra = ''
    if account_ids:
        ids = [str(i).strip() for i in account_ids if str(i).strip()]
        if ids:
            placeholders = ','.join(['%s'] * len(ids))
            where_extra = f' AND account_id IN ({placeholders})'
            params.extend(ids)
    sql = f"""
        SELECT account_id,
               COALESCE(SUM(CAST({revenue_col} AS DECIMAL(18,4))), 0) AS revenue
        FROM {table}
        WHERE DATE({date_col}) BETWEEN %s AND %s{where_extra}
        GROUP BY account_id
    """
    if not db.execute_query(sql, tuple(params)):
        return {}
    out = {}
    for row in (db.cur_hris.fetchall() or []):
        cid = str(row.get('account_id') or '').strip()
        if cid:
            out[cid] = round(float(row.get('revenue') or 0), 2)
    return out


def _report_cred_adsense_fetch_daily_revenue_by_account(
    db, table, date_col, revenue_col, start_date, end_date, account_ids=None,
):
    """Daily revenue per account_id — tanpa double-count dari normalisasi domain ganda."""
    params = [start_date, end_date]
    where_extra = ''
    if account_ids:
        ids = [str(i).strip() for i in account_ids if str(i).strip()]
        if ids:
            placeholders = ','.join(['%s'] * len(ids))
            where_extra = f' AND account_id IN ({placeholders})'
            params.extend(ids)
    sql = f"""
        SELECT account_id, DATE({date_col}) AS d,
               COALESCE(SUM(CAST({revenue_col} AS DECIMAL(18,4))), 0) AS revenue
        FROM {table}
        WHERE DATE({date_col}) BETWEEN %s AND %s{where_extra}
        GROUP BY account_id, DATE({date_col})
        ORDER BY d ASC
    """
    if not db.execute_query(sql, tuple(params)):
        return {}
    out = {}
    for row in (db.cur_hris.fetchall() or []):
        cid = str(row.get('account_id') or '').strip()
        d = row.get('d')
        if hasattr(d, 'isoformat'):
            d = d.isoformat()
        else:
            d = str(d or '')[:10]
        if not cid or not d:
            continue
        out.setdefault(cid, {})
        out[cid][d] = round(float(row.get('revenue') or 0), 2)
    return out


def _report_cred_adsense_fetch_spend_by_domain(db, start_date, end_date):
    dom_expr = db._report_account_domain_key_sql('b.data_ads_domain')
    sql = f"""
        SELECT {dom_expr} AS domain_key,
               COALESCE(SUM(CAST(b.data_ads_spend AS DECIMAL(18,4))), 0) AS spend
        FROM data_ads_campaign b
        WHERE DATE(b.data_ads_tanggal) BETWEEN %s AND %s
          AND TRIM(COALESCE(b.data_ads_domain, '')) <> ''
        GROUP BY domain_key
    """
    if not db.execute_query(sql, (start_date, end_date)):
        return {}
    out = {}
    for row in (db.cur_hris.fetchall() or []):
        k = str(row.get('domain_key') or '').strip()
        if k:
            out[k] = float(row.get('spend') or 0)
    return out


def _report_cred_adsense_fetch_spend_daily_by_domain(db, start_date, end_date):
    dom_expr = db._report_account_domain_key_sql('b.data_ads_domain')
    sql = f"""
        SELECT DATE(b.data_ads_tanggal) AS d, {dom_expr} AS domain_key,
               COALESCE(SUM(CAST(b.data_ads_spend AS DECIMAL(18,4))), 0) AS spend
        FROM data_ads_campaign b
        WHERE DATE(b.data_ads_tanggal) BETWEEN %s AND %s
          AND TRIM(COALESCE(b.data_ads_domain, '')) <> ''
        GROUP BY d, domain_key
    """
    if not db.execute_query(sql, (start_date, end_date)):
        return {}
    out = {}
    for row in (db.cur_hris.fetchall() or []):
        d = row.get('d')
        if hasattr(d, 'isoformat'):
            d = d.isoformat()
        else:
            d = str(d or '')[:10]
        if not d:
            continue
        k = str(row.get('domain_key') or '').strip()
        if not k:
            continue
        out.setdefault(d, {})
        out[d][k] = float(row.get('spend') or 0)
    return out


def _report_cred_adsense_cred_domain_keys(db, cred_id, adx_by_cred, adsense_by_cred, subdomain_summary):
    cid = str(cred_id or '').strip()
    domains = set()
    for item in (subdomain_summary.get(cid) or []):
        dk = db._normalize_domain_match_key(item.get('subdomain'))
        if dk:
            domains.add(dk)
    for dk in ((adx_by_cred or {}).get(cid) or {}).keys():
        domains.add(str(dk))
    for dk in ((adsense_by_cred or {}).get(cid) or {}).keys():
        domains.add(str(dk))
    return domains


def _report_cred_adsense_fetch_revenue_by_account_subdomain(
    db, table, date_col, revenue_col, domain_col, start_date, end_date, account_ids=None,
):
    """Revenue per account + subdomain key (tanpa double-count normalisasi ganda)."""
    params = [start_date, end_date]
    where_extra = ''
    if account_ids:
        ids = [str(i).strip() for i in account_ids if str(i).strip()]
        if ids:
            placeholders = ','.join(['%s'] * len(ids))
            where_extra = f' AND account_id IN ({placeholders})'
            params.extend(ids)
    sql = f"""
        SELECT account_id, {domain_col} AS raw_domain,
               COALESCE(SUM(CAST({revenue_col} AS DECIMAL(18,4))), 0) AS revenue
        FROM {table}
        WHERE DATE({date_col}) BETWEEN %s AND %s{where_extra}
        GROUP BY account_id, raw_domain
    """
    if not db.execute_query(sql, tuple(params)):
        return {}
    out = {}
    for row in (db.cur_hris.fetchall() or []):
        cid = str(row.get('account_id') or '').strip()
        key = db._normalize_subdomain_key(row.get('raw_domain'))
        if not cid or not key:
            continue
        out.setdefault(cid, {})
        out[cid][key] = round(out[cid].get(key, 0.0) + float(row.get('revenue') or 0), 2)
    return out


def _report_cred_adsense_lookup_domain_spend(db, subdomain_key, spend_by_domain):
    total = 0.0
    used = set()
    candidates = {
        str(subdomain_key or '').strip(),
        db._normalize_domain_match_key(subdomain_key),
        db._report_account_normalize_campaign_domain(subdomain_key),
    }
    for key in candidates:
        if not key or key in used:
            continue
        if key in (spend_by_domain or {}):
            total += float(spend_by_domain.get(key) or 0)
            used.add(key)
    return round(total, 2)


def _report_cred_adsense_campaign_platform(raw_domain):
    token = str(raw_domain or '').strip().lower().split('_')[0].split()[0]
    if not token:
        return 'adsense'
    for suffix in ('.adx',):
        if token.endswith(suffix):
            return 'adx'
    for suffix in ('.disp', '.display'):
        if token.endswith(suffix):
            return 'adsense'
    parts = [p for p in token.split('.') if p]
    if len(parts) >= 3:
        label = parts[2].upper()
        if label == 'ADX':
            return 'adx'
        if label in ('DISP', 'DISPLAY'):
            return 'adsense'
    return 'adsense'


def _report_cred_adsense_fetch_running_campaign_counts_by_subdomain(db, start_date, end_date):
    """Campaign per subdomain dari data_ads_campaign (spend > 0 di periode), dipisah AdX vs AdSense."""
    dom_expr = db._report_account_domain_key_sql('b.data_ads_domain')
    sql = f"""
        SELECT {dom_expr} AS domain_key,
               MAX(LOWER(TRIM(b.data_ads_domain))) AS raw_domain,
               b.data_ads_campaign_id,
               COALESCE(SUM(CAST(b.data_ads_spend AS DECIMAL(18,4))), 0) AS spend
        FROM data_ads_campaign b
        WHERE DATE(b.data_ads_tanggal) BETWEEN %s AND %s
          AND TRIM(COALESCE(b.data_ads_domain, '')) <> ''
          AND TRIM(COALESCE(b.data_ads_campaign_id, '')) <> ''
        GROUP BY domain_key, b.data_ads_campaign_id
        HAVING spend > 0
    """
    if not db.execute_query(sql, (start_date, end_date)):
        return {}
    buckets = {}
    for row in (db.cur_hris.fetchall() or []):
        cid = str(row.get('data_ads_campaign_id') or '').strip()
        if not cid:
            continue
        raw = str(row.get('raw_domain') or '').strip()
        platform = _report_cred_adsense_campaign_platform(raw)
        domain_keys = set()
        primary = str(row.get('domain_key') or '').strip()
        if primary:
            domain_keys.add(primary)
        normalized = db._normalize_subdomain_key(db._report_account_normalize_campaign_domain(raw or primary))
        if normalized:
            domain_keys.add(normalized)
        match_key = db._normalize_domain_match_key(raw or primary)
        if match_key:
            domain_keys.add(match_key)
        for key in domain_keys:
            if not key:
                continue
            slot = buckets.setdefault(key, {'adx': set(), 'adsense': set()})
            slot[platform].add(cid)
    return {
        key: {
            'adx_campaign_count': len(vals.get('adx') or set()),
            'adsense_campaign_count': len(vals.get('adsense') or set()),
        }
        for key, vals in buckets.items()
    }


def _report_cred_adsense_lookup_campaign_counts(db, subdomain_key, campaign_counts):
    candidates = [
        str(subdomain_key or '').strip(),
        db._normalize_domain_match_key(subdomain_key),
        db._normalize_subdomain_key(subdomain_key),
        db._report_account_normalize_campaign_domain(subdomain_key),
    ]
    adx_ids = set()
    adsense_ids = set()
    seen_keys = set()
    for key in candidates:
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        bucket = (campaign_counts or {}).get(key)
        if not bucket:
            continue
        # Angka sudah unique per key; ambil max antar alias key agar tidak double-count
        adx_ids.add(int(bucket.get('adx_campaign_count') or 0))
        adsense_ids.add(int(bucket.get('adsense_campaign_count') or 0))
    if not adx_ids and not adsense_ids:
        return {'adx_campaign_count': 0, 'adsense_campaign_count': 0}
    return {
        'adx_campaign_count': max(adx_ids) if adx_ids else 0,
        'adsense_campaign_count': max(adsense_ids) if adsense_ids else 0,
    }


def _report_cred_adsense_build_subdomain_rows(
    db, cred_id, subdomain_summary, adx_by_sub, adsense_by_sub, spend_by_domain,
    campaign_counts=None,
):
    cid = str(cred_id or '').strip()
    keys = set()
    for item in (subdomain_summary.get(cid) or []):
        k = str((item or {}).get('subdomain') or '').strip()
        if k:
            keys.add(k)
    for k in ((adx_by_sub or {}).get(cid) or {}).keys():
        keys.add(str(k))
    for k in ((adsense_by_sub or {}).get(cid) or {}).keys():
        keys.add(str(k))

    rows = []
    for key in sorted(keys):
        adx_rev = float((adx_by_sub.get(cid) or {}).get(key) or 0)
        adsense_rev = float((adsense_by_sub.get(cid) or {}).get(key) or 0)
        spend = _report_cred_adsense_lookup_domain_spend(db, key, spend_by_domain)
        revenue = adx_rev + adsense_rev
        metrics = db._report_account_build_row_metrics(spend, revenue, 0)
        counts = _report_cred_adsense_lookup_campaign_counts(db, key, campaign_counts)
        rows.append({
            'subdomain': key,
            'adx_revenue': adx_rev,
            'adsense_revenue': adsense_rev,
            'adx_campaign_count': counts['adx_campaign_count'],
            'adsense_campaign_count': counts['adsense_campaign_count'],
            'spend': metrics['spend'],
            'revenue': metrics['revenue'],
            'profit': metrics['profit'],
            'roi': metrics['roi'],
        })
    rows.sort(key=lambda r: float(r.get('revenue') or 0), reverse=True)
    return rows


def _report_cred_adsense_sum_spend_for_domains(db, domain_keys, spend_by_domain):
    total = 0.0
    used = set()
    for dk in (domain_keys or []):
        candidates = {
            str(dk or '').strip(),
            db._normalize_domain_match_key(dk),
            db._report_account_normalize_campaign_domain(dk),
        }
        for key in candidates:
            if not key or key in used:
                continue
            if key in (spend_by_domain or {}):
                total += float(spend_by_domain.get(key) or 0)
                used.add(key)
    return round(total, 2)


def _report_cred_adsense_detect_data_gaps(db, start_date, end_date, spend_daily_by_domain):
    """Flag dates where FB spend exists but AdX revenue is completely missing."""
    from datetime import datetime as dt, timedelta

    gaps = []
    try:
        sql = f"""
            SELECT DATE({_ADX_REV_DATE_COL}) AS d,
                   COALESCE(SUM(CAST({_ADX_REV_AMOUNT_COL} AS DECIMAL(18,4))), 0) AS revenue
            FROM {_ADX_REV_TABLE}
            WHERE DATE({_ADX_REV_DATE_COL}) BETWEEN %s AND %s
            GROUP BY DATE({_ADX_REV_DATE_COL})
        """
        if not db.execute_query(sql, (start_date, end_date)):
            return gaps
        adx_by_day = {}
        for row in (db.cur_hris.fetchall() or []):
            d = row.get('d')
            if hasattr(d, 'isoformat'):
                d = d.isoformat()
            else:
                d = str(d or '')[:10]
            if d:
                adx_by_day[d] = float(row.get('revenue') or 0)

        d0 = dt.strptime(start_date, '%Y-%m-%d').date()
        d1 = dt.strptime(end_date, '%Y-%m-%d').date()
        cur = d0
        while cur <= d1:
            ds = cur.isoformat()
            day_spend = round(sum(float(v or 0) for v in (spend_daily_by_domain.get(ds) or {}).values()), 2)
            day_adx = round(float(adx_by_day.get(ds) or 0), 2)
            if day_spend > 0 and day_adx <= 0:
                gaps.append({
                    'date': ds,
                    'spend': day_spend,
                    'adx_revenue': day_adx,
                })
            cur += timedelta(days=1)
    except Exception:
        return gaps
    return gaps


def list_report_account_adsense_summary(
    db,
    start_date,
    end_date,
    account_q=None,
    user_id=None,
    is_super=True,
):
    from datetime import datetime as dt, timedelta

    try:
        start_date = str(start_date or '').strip()[:10]
        end_date = str(end_date or '').strip()[:10]
        if not start_date or not end_date:
            return {'status': False, 'data': 'Rentang tanggal wajib diisi'}

        accounts = _report_cred_adsense_fetch_accounts(db, account_q, user_id, is_super)
        empty_summary = {'spend': 0.0, 'revenue': 0.0, 'profit': 0.0, 'roi': 0.0}
        if not accounts:
            return {
                'status': True,
                'data': {
                    'period': {'start': start_date, 'end': end_date},
                    'summary': {'daily': empty_summary},
                    'rows': [],
                    'chart': [],
                },
            }

        account_ids = [a['account_id'] for a in accounts]
        subdomain_summary_resp = db.get_subdomain_platform_summary_for_accounts(account_ids)
        subdomain_summary = (subdomain_summary_resp or {}).get('data') or {}

        adx_rev_map, adx_by_cred = db._report_account_build_revenue_maps(
            _ADX_REV_TABLE, _ADX_REV_DATE_COL, _ADX_REV_AMOUNT_COL, _ADX_REV_DOMAIN_COL,
            start_date, end_date,
        )
        adsense_rev_map, adsense_by_cred = db._report_account_build_revenue_maps(
            'data_adsense_domain', 'data_adsense_tanggal', 'data_adsense_revenue', 'data_adsense_domain',
            start_date, end_date,
        )
        adx_rev_by_account = _report_cred_adsense_fetch_revenue_by_account(
            db, _ADX_REV_TABLE, _ADX_REV_DATE_COL, _ADX_REV_AMOUNT_COL,
            start_date, end_date, account_ids,
        )
        adsense_rev_by_account = _report_cred_adsense_fetch_revenue_by_account(
            db, 'data_adsense_domain', 'data_adsense_tanggal', 'data_adsense_revenue',
            start_date, end_date, account_ids,
        )
        adx_daily_by_account = _report_cred_adsense_fetch_daily_revenue_by_account(
            db, _ADX_REV_TABLE, _ADX_REV_DATE_COL, _ADX_REV_AMOUNT_COL,
            start_date, end_date, account_ids,
        )
        adsense_daily_by_account = _report_cred_adsense_fetch_daily_revenue_by_account(
            db, 'data_adsense_domain', 'data_adsense_tanggal', 'data_adsense_revenue',
            start_date, end_date, account_ids,
        )
        adx_by_sub = _report_cred_adsense_fetch_revenue_by_account_subdomain(
            db, _ADX_REV_TABLE, _ADX_REV_DATE_COL, _ADX_REV_AMOUNT_COL, _ADX_REV_DOMAIN_COL,
            start_date, end_date, account_ids,
        )
        adsense_by_sub = _report_cred_adsense_fetch_revenue_by_account_subdomain(
            db, 'data_adsense_domain', 'data_adsense_tanggal', 'data_adsense_revenue', 'data_adsense_domain',
            start_date, end_date, account_ids,
        )
        spend_by_domain = _report_cred_adsense_fetch_spend_by_domain(db, start_date, end_date)
        spend_daily_by_domain = _report_cred_adsense_fetch_spend_daily_by_domain(db, start_date, end_date)
        campaign_counts = _report_cred_adsense_fetch_running_campaign_counts_by_subdomain(
            db, start_date, end_date,
        )

        rows_out = []
        totals = {'spend': 0.0, 'revenue': 0.0, 'profit': 0.0}

        for acct in accounts:
            cid = str(acct['account_id'])
            domains = _report_cred_adsense_cred_domain_keys(
                db, cid, adx_by_cred, adsense_by_cred, subdomain_summary,
            )
            adx_rev = float(adx_rev_by_account.get(cid) or 0)
            adsense_rev = float(adsense_rev_by_account.get(cid) or 0)
            spend = _report_cred_adsense_sum_spend_for_domains(db, domains, spend_by_domain)
            revenue = adx_rev + adsense_rev
            metrics = db._report_account_build_row_metrics(spend, revenue, len(domains))
            subdomains = _report_cred_adsense_build_subdomain_rows(
                db, cid, subdomain_summary, adx_by_sub, adsense_by_sub, spend_by_domain,
                campaign_counts,
            )
            rows_out.append({
                'account_id': cid,
                'account_key': cid,
                'account_name': acct.get('account_name') or cid,
                'user_mail': acct.get('user_mail') or '',
                'network_code': acct.get('network_code') or '',
                'adx_revenue': adx_rev,
                'adsense_revenue': adsense_rev,
                'spend': metrics['spend'],
                'revenue': metrics['revenue'],
                'profit': metrics['profit'],
                'roi': metrics['roi'],
                'subdomain_count': metrics['subdomain_count'],
                'subdomains': subdomains,
            })
            totals['spend'] += metrics['spend']
            totals['revenue'] += metrics['revenue']
            totals['profit'] += metrics['profit']

        rows_out.sort(key=lambda r: float(r.get('revenue') or 0), reverse=True)

        chart = []
        try:
            d0 = dt.strptime(start_date, '%Y-%m-%d').date()
            d1 = dt.strptime(end_date, '%Y-%m-%d').date()
            cur = d0
            while cur <= d1:
                ds = cur.isoformat()
                day_spend = 0.0
                day_adx = 0.0
                day_adsense = 0.0
                for acct in accounts:
                    cid = str(acct['account_id'])
                    domains = _report_cred_adsense_cred_domain_keys(
                        db, cid, adx_by_cred, adsense_by_cred, subdomain_summary,
                    )
                    day_dom_spend = spend_daily_by_domain.get(ds) or {}
                    day_spend += _report_cred_adsense_sum_spend_for_domains(db, domains, day_dom_spend)
                    day_adx += float((adx_daily_by_account.get(cid) or {}).get(ds) or 0)
                    day_adsense += float((adsense_daily_by_account.get(cid) or {}).get(ds) or 0)
                day_revenue = day_adx + day_adsense
                chart.append({
                    'date': ds,
                    'spend': round(day_spend, 2),
                    'adx_revenue': round(day_adx, 2),
                    'adsense_revenue': round(day_adsense, 2),
                    'revenue': round(day_revenue, 2),
                    'profit': round(day_revenue - day_spend, 2),
                })
                cur += timedelta(days=1)
        except Exception:
            chart = []

        summary_metrics = db._report_account_build_row_metrics(totals['spend'], totals['revenue'], 0)
        data_gaps = _report_cred_adsense_detect_data_gaps(
            db, start_date, end_date, spend_daily_by_domain,
        )
        data_warnings = []
        if data_gaps:
            gap_dates = [g['date'] for g in data_gaps]
            gap_spend = round(sum(float(g.get('spend') or 0) for g in data_gaps), 2)
            data_warnings.append({
                'code': 'missing_adx_revenue',
                'title': 'Data revenue AdX tidak lengkap',
                'message': (
                    'Revenue AdX nol di database pada tanggal '
                    + ', '.join(gap_dates)
                    + ' meskipun masih ada spend Facebook Ads. Profit pada periode ini '
                    'cenderung minus karena revenue belum tersinkron — bukan karena rumus profit salah. '
                    'Jalankan ulang sync/cron AdX domain untuk tanggal tersebut.'
                ),
                'gap_dates': gap_dates,
                'gap_spend': gap_spend,
            })
        return {
            'status': True,
            'data': {
                'period': {'start': start_date, 'end': end_date},
                'summary': {
                    'daily': {
                        'spend': summary_metrics['spend'],
                        'revenue': summary_metrics['revenue'],
                        'profit': summary_metrics['profit'],
                        'roi': summary_metrics['roi'],
                    },
                },
                'rows': rows_out,
                'chart': chart,
                'data_warnings': data_warnings,
            },
        }
    except Exception as e:
        return {'status': False, 'data': str(e)}
