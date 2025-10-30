from django.db import connections


def disable_returning_on_mysql():
    """
    Disable SQL RETURNING usage for MySQL/MariaDB backends.
    Some MariaDB versions (pre-10.5) don't support RETURNING.
    This patch forces Django to avoid RETURNING-based inserts/updates.
    """
    try:
        for conn in connections.all():
            if getattr(conn, "vendor", None) == "mysql":
                features = getattr(conn, "features", None)
                if not features:
                    continue
                for attr in (
                    "can_return_rows_from_insert",
                    "can_return_id_from_insert",
                    "supports_returning_rows_from_update",
                    "supports_update_returning",
                    "has_native_returning",
                    "can_return_columns_from_insert",
                    "can_return_columns_from_update",
                ):
                    if hasattr(features, attr):
                        setattr(features, attr, False)
        print("[PATCH] Disabled SQL RETURNING for MySQL/MariaDB (compatibility mode)")
    except Exception as e:
        print(f"[PATCH] Failed to disable SQL RETURNING: {e}")


def monkey_patch_mysql_insert_drop_returning():
    """
    Monkey-patch MySQL SQLInsertCompiler to strip any trailing RETURNING clause.
    This is a defensive workaround for environments where Django mistakenly
    emits INSERT ... RETURNING against MariaDB/MySQL that don't support it.
    Ensures return type is always a list of (sql, params) tuples.
    """
    try:
        from django.db.backends.mysql.compiler import SQLInsertCompiler
    except Exception:
        return

    original_as_sql = getattr(SQLInsertCompiler, "as_sql", None)
    if not original_as_sql or getattr(SQLInsertCompiler, "_patched_drop_returning", False):
        return

    def _strip_returning(sql: str) -> str:
        # Case-insensitive strip of trailing " RETURNING ..."
        upper = sql.upper()
        pos = upper.find(" RETURNING ")
        if pos != -1:
            return sql[:pos]
        return sql

    def patched_as_sql(self, *args, **kwargs):  # noqa: ANN001
        res = original_as_sql(self, *args, **kwargs)
        if res is None:
            return []

        def _handle(item):
            if isinstance(item, tuple) and item:
                sql = item[0]
                return (_strip_returning(sql),) + item[1:]
            return item

        if isinstance(res, list):
            return [_handle(i) for i in res]
        elif isinstance(res, tuple):
            # Normalize to list of one tuple
            return [_handle(res)]
        else:
            # Unknown type; try to normalize to empty list to avoid TypeError
            try:
                return list(res)  # may be generator
            except Exception:
                return []

    SQLInsertCompiler.as_sql = patched_as_sql
    SQLInsertCompiler._patched_drop_returning = True


def monkey_patch_insert_execute_sql_safe():
    """Ensure SQLInsertCompiler.execute_sql never returns [None] when no rows are returned.
    This guards Django's Model._save_table from TypeError when it expects an iterable.
    """
    try:
        from django.db.models.sql.compiler import SQLInsertCompiler as BaseSQLInsertCompiler
    except Exception:
        return

    original_execute_sql = getattr(BaseSQLInsertCompiler, "execute_sql", None)
    if not original_execute_sql or getattr(BaseSQLInsertCompiler, "_patched_exec_safe", False):
        return

    def patched_execute_sql(self, *args, **kwargs):  # noqa: ANN001
        res = original_execute_sql(self, *args, **kwargs)
        try:
            # Only adjust for MySQL/MariaDB
            vendor = getattr(self.connection, "vendor", None)
            if vendor == "mysql" and isinstance(res, list) and res and res[0] is None:
                return [()]  # empty row to keep zip() happy without assigning
        except Exception:
            # If any issue, return original result
            return res
        return res

    BaseSQLInsertCompiler.execute_sql = patched_execute_sql
    BaseSQLInsertCompiler._patched_exec_safe = True


def force_disable_mysql_returning_class():
    """Force-disable RETURNING-related features at MySQL DatabaseFeatures class level.
    This ensures future connections won't enable RETURNING even if detection misfires.
    """
    try:
        from django.db.backends.mysql.features import DatabaseFeatures as MySQLFeatures
    except Exception:
        return

    for attr in (
        "can_return_rows_from_insert",
        "can_return_id_from_insert",
        "supports_returning_rows_from_update",
        "supports_update_returning",
        "has_native_returning",
        "can_return_columns_from_insert",
        "can_return_columns_from_update",
    ):
        try:
            setattr(MySQLFeatures, attr, False)
        except Exception:
            # Ignore if attribute is missing in this Django version
            pass
    try:
        # Some Django versions use DatabaseOperations flags
        from django.db.backends.mysql.operations import DatabaseOperations as MySQLOps
        for op_attr in (
            "return_id_after_insert",
            "can_return_columns_from_insert",
            "can_return_columns_from_update",
        ):
            try:
                setattr(MySQLOps, op_attr, False)
            except Exception:
                pass
    except Exception:
        pass

    print("[PATCH] Forced MySQL DatabaseFeatures class to disable RETURNING")