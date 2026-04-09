from __future__ import annotations

from functools import lru_cache
from typing import Any

import clickhouse_connect
import pandas as pd
from django.conf import settings


@lru_cache(maxsize=1)
def get_client():
    cfg = settings.CLICKHOUSE
    return clickhouse_connect.get_client(
        host=cfg["host"],
        port=cfg["port"],
        username=cfg["username"],
        password=cfg["password"],
        database=cfg["database"],
        secure=cfg.get("secure", False),
        connect_timeout=cfg.get("connect_timeout", 10),
        send_receive_timeout=cfg.get("send_receive_timeout", 120),
    )


def command(sql: str) -> Any:
    if settings.ANALYTICS.get("debug_sql"):
        print(sql)
    return get_client().command(sql)


def query_rows(sql: str):
    if settings.ANALYTICS.get("debug_sql"):
        print(sql)
    result = get_client().query(sql)
    rows = getattr(result, "result_rows", None)
    if rows is None:
        rows = getattr(result, "result_set", [])
    cols = getattr(result, "column_names", [])
    return cols, rows


def query_df(sql: str) -> pd.DataFrame:
    cols, rows = query_rows(sql)
    return pd.DataFrame(rows, columns=cols)


def insert_df(table: str, df: pd.DataFrame):
    if df.empty:
        return
    get_client().insert_df(table, df)
