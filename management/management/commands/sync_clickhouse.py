from django.core.management.base import BaseCommand
from datetime import date, datetime, timedelta
import json
import os

import pymysql

try:
    import requests
except Exception:
    requests = None


class Command(BaseCommand):
    help = "Sync tabel reporting dari MySQL ke ClickHouse (insert JSONEachRow), khusus SELECT range tanggal."

    def add_arguments(self, parser):
        parser.add_argument("--days", type=int, default=None)
        parser.add_argument("--since", type=str, default=None)
        parser.add_argument("--tables", type=str, default=None)
        parser.add_argument("--batch-size", type=int, default=int(os.getenv("CH_SYNC_BATCH_SIZE", "5000")))
        parser.add_argument("--no-delete", action="store_true", default=False)
        parser.add_argument("--dry-run", action="store_true", default=False)

    def _default_tables(self):
        return [
            "data_adsense_country",
            "data_adsense_domain",
            "data_adx_country",
            "data_adx_domain",
            "data_ads_campaign",
            "data_ads_country",
            "log_ads_country",
            "log_adsense_country",
            "log_adx_country",
        ]

    def _resolve_tables(self, tables_arg):
        if tables_arg:
            return [t.strip().lower() for t in tables_arg.split(",") if t.strip()]
        raw = str(os.getenv("REPORT_DB_TABLES", "") or os.getenv("DB_REPORT_TABLES", "") or "").strip()
        if raw:
            tables = [t.strip().lower() for t in raw.split(",") if t.strip()]
            if tables:
                return tables
        return self._default_tables()

    def _mysql_conn(self):
        host = os.getenv("DB_HOST") or os.getenv("HRIS_DB_HOST", "127.0.0.1")
        raw_port = (os.getenv("DB_PORT") or os.getenv("HRIS_DB_PORT") or "3306").strip()
        try:
            port = int(raw_port)
        except (TypeError, ValueError):
            port = 3306
        user = os.getenv("DB_USER") or os.getenv("HRIS_DB_USER", "root")
        password = os.getenv("DB_PASSWORD") or os.getenv("HRIS_DB_PASSWORD", "hris123456")
        database = os.getenv("DB_NAME") or os.getenv("HRIS_DB_NAME", "hris_trendHorizone")
        connect_timeout = int(os.getenv("MYSQL_CONNECT_TIMEOUT", "10"))
        read_timeout = int(os.getenv("MYSQL_READ_TIMEOUT", "300"))
        write_timeout = int(os.getenv("MYSQL_WRITE_TIMEOUT", "300"))
        return pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.SSDictCursor,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            write_timeout=write_timeout,
        )

    def _ch_config(self):
        host = os.getenv("CH_HOST") or os.getenv("REPORT_DB_HOST") or os.getenv("DB_REPORT_HOST") or "127.0.0.1"
        raw_port = (os.getenv("CH_PORT") or os.getenv("REPORT_DB_PORT") or os.getenv("DB_REPORT_PORT") or "8123").strip()
        try:
            port = int(raw_port)
        except (TypeError, ValueError):
            port = 8123
        user = os.getenv("CH_USER") or os.getenv("REPORT_DB_USER") or os.getenv("DB_REPORT_USER") or "default"
        password = os.getenv("CH_PASSWORD") or os.getenv("REPORT_DB_PASSWORD") or os.getenv("DB_REPORT_PASSWORD") or "hris123456"
        database = os.getenv("CH_DB") or os.getenv("REPORT_DB_NAME") or os.getenv("DB_REPORT_NAME") or ""
        timeout = int(os.getenv("CH_HTTP_TIMEOUT", "60"))
        return host, port, user, password, database, timeout

    def _ch_post(self, sql_text):
        if requests is None:
            raise RuntimeError("requests library tidak tersedia (dibutuhkan untuk koneksi ClickHouse HTTP).")
        host, port, user, password, database, timeout = self._ch_config()
        base = f"http://{host}:{port}/"
        params = {}
        if database:
            params["database"] = database
        if user:
            params["user"] = user
        if password:
            params["password"] = password
        resp = requests.post(base, params=params, data=sql_text.encode("utf-8"), timeout=timeout)
        if resp.status_code >= 400:
            body = (resp.text or '').strip()
            if len(body) > 2000:
                body = body[:2000] + "..."
            raise RuntimeError(f"ClickHouse HTTP error status={resp.status_code} body={body}")
        return resp.text

    def _pick_date_col(self, table, columns):
        preferred = f"{table}_tanggal"
        cols = set([c.lower() for c in columns])
        if preferred.lower() in cols:
            return preferred
        for c in columns:
            cl = c.lower()
            if cl.endswith("_tanggal"):
                return c
        if "mdd" in cols:
            return "mdd"
        return None

    def _json_row(self, row):
        out = {}
        for k, v in (row or {}).items():
            if isinstance(v, (datetime,)):
                out[k] = v.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(v, (date,)):
                out[k] = v.strftime("%Y-%m-%d")
            elif isinstance(v, (bytes, bytearray)):
                try:
                    out[k] = v.decode("utf-8", errors="replace")
                except Exception:
                    out[k] = str(v)
            else:
                out[k] = v
        return out

    def handle(self, *args, **options):
        engine = str(os.getenv("REPORT_DB_ENGINE", "") or os.getenv("DB_REPORT_ENGINE", "") or "").strip().lower()
        if engine not in ("clickhouse", "ch"):
            self.stdout.write(self.style.WARNING("REPORT_DB_ENGINE belum diset clickhouse/ch; sync tetap bisa jalan tapi pastikan env ClickHouse sudah benar."))
        
        days_opt = options.get("days")
        since_opt = options.get("since")
        if since_opt:
            start_date = since_opt.strip()
        else:
            days = days_opt if days_opt is not None else int(os.getenv("CH_SYNC_DAYS", "1"))
            days = max(1, int(days))
            start_date = (date.today() - timedelta(days=days - 1)).strftime("%Y-%m-%d")

        tables = self._resolve_tables(options.get("tables"))
        batch_size = int(options.get("batch_size") or 5000)
        dry_run = bool(options.get("dry_run"))
        no_delete = bool(options.get("no_delete"))

        self.stdout.write(self.style.WARNING(f"Sync ClickHouse start_date={start_date} tables={','.join(tables)} batch_size={batch_size} dry_run={dry_run} no_delete={no_delete}"))

        mysql = self._mysql_conn()
        try:
            for table in tables:
                self.stdout.write(self.style.SUCCESS(f"== {table} =="))
                attempt = 0
                while True:
                    attempt += 1
                    try:
                        if getattr(mysql, "open", False) is False:
                            try:
                                mysql.close()
                            except Exception:
                                pass
                            mysql = self._mysql_conn()
                        with mysql.cursor() as cur:
                            cur.execute(f"SHOW COLUMNS FROM `{table}`")
                            cols_rows = cur.fetchall() or []
                            columns = [r.get("Field") for r in cols_rows if r.get("Field")]
                            if not columns:
                                self.stdout.write(self.style.WARNING(f"{table}: tidak ada kolom / tabel tidak ditemukan di MySQL."))
                                break

                            date_col = self._pick_date_col(table, columns)
                            where_sql = ""
                            where_params = ()
                            if date_col:
                                where_sql = f" WHERE DATE(`{date_col}`) >= %s"
                                where_params = (start_date,)

                            if date_col and (not no_delete) and (not dry_run):
                                self._ch_post(
                                    f"ALTER TABLE {table} DELETE WHERE toDate({date_col}) >= toDate('{start_date}') SETTINGS mutations_sync=1"
                                )

                            if dry_run:
                                cur.execute(f"SELECT COUNT(*) AS c FROM `{table}`{where_sql}", where_params)
                                cnt = cur.fetchone() or {}
                                self.stdout.write(self.style.WARNING(f"{table}: dry-run count={cnt.get('c', 0)} where={where_sql or '(no filter)'}"))
                                break

                            cur.execute(f"SELECT * FROM `{table}`{where_sql}", where_params)

                            total = 0
                            while True:
                                rows = cur.fetchmany(batch_size)
                                if not rows:
                                    break
                                lines = []
                                for r in rows:
                                    lines.append(json.dumps(self._json_row(r), ensure_ascii=False, default=str))
                                payload = f"INSERT INTO {table} FORMAT JSONEachRow\n" + "\n".join(lines) + "\n"
                                self._ch_post(payload)
                                total += len(rows)
                            self.stdout.write(self.style.SUCCESS(f"{table}: synced rows={total}"))
                            break
                    except pymysql.err.OperationalError as e:
                        code = None
                        try:
                            code = int(e.args[0])
                        except Exception:
                            code = None
                        if code in (2006, 2013) and attempt < 2:
                            self.stdout.write(self.style.WARNING(f"{table}: koneksi MySQL terputus (code={code}); retry reconnect..."))
                            try:
                                mysql.close()
                            except Exception:
                                pass
                            mysql = self._mysql_conn()
                            continue
                        self.stdout.write(self.style.ERROR(f"{table}: error {e}"))
                        break
                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f"{table}: error {e}"))
                        break
        finally:
            try:
                mysql.close()
            except Exception:
                pass