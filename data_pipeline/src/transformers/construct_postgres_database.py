"""
Copy every SQLite file in DATABASE_PATH into a PostgreSQL database whose name
matches the file stem (e.g. countries_stats.db -> database "countries_stats").

Reads connection settings from .env (same keys as docker/server):
  DB_HOST, DB_USER, DB_PASSWORD, DB_PORT
and path layout from the data pipeline:
  PROJECT_PATH (optional; used to locate .env), DATABASE_PATH (folder of *.db files)
"""

from __future__ import annotations

import os
import re
import sqlite3
from collections import deque
from pathlib import Path

from dotenv import load_dotenv

try:
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extras import execute_batch
except ImportError as exc:  # pragma: no cover - import guard
    raise ImportError(
        "psycopg2 is required. Install with: pip install psycopg2-binary"
    ) from exc


_IDENT_SAFE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _load_env() -> None:
    project = os.getenv("PROJECT_PATH")
    if project:
        load_dotenv(Path(project) / ".env")
    load_dotenv()


def _sqlite_quote_ident(name: str) -> str:
    """Double-quote a SQLite identifier (table/column name)."""
    return '"' + name.replace('"', '""') + '"'


def _escape_percent_for_mogrify(sql_text: str) -> str:
    """
    Psycopg2's mogrify() applies Python %-formatting to the query string.
    A literal `%` inside a double-quoted identifier (e.g. "Yearly % Change")
    must be written as `%%` or the formatter mis-counts placeholders and can
    raise IndexError when binding parameters.
    """
    out: list[str] = []
    i = 0
    n = len(sql_text)
    while i < n:
        if sql_text[i] == "%" and i + 1 < n and sql_text[i + 1] == "s":
            out.append("%s")
            i += 2
        elif sql_text[i] == "%":
            out.append("%%")
            i += 1
        else:
            out.append(sql_text[i])
            i += 1
    return "".join(out)


def _pg_type(sqlite_declared: str | None) -> str:
    if not sqlite_declared:
        return "TEXT"
    t = sqlite_declared.strip().upper()
    if "INT" in t:
        return "INTEGER"
    if any(x in t for x in ("REAL", "FLOA", "DOUB", "DEC", "NUM")):
        return "DOUBLE PRECISION"
    if "BLOB" in t:
        return "BYTEA"
    if any(x in t for x in ("CHAR", "CLOB", "TEXT")):
        return "TEXT"
    return "TEXT"


def _quote_ident_pg(name: str) -> sql.Identifier:
    return sql.Identifier(name)


def _sqlite_user_tables(conn: sqlite3.Connection) -> list[str]:
    cur = conn.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type = 'table' AND name NOT LIKE 'sqlite_%' "
        "ORDER BY name"
    )
    return [row[0] for row in cur.fetchall()]


def _table_create_order(conn: sqlite3.Connection, tables: list[str]) -> list[str]:
    """Parents before children so FOREIGN KEY constraints can be created."""
    deps: dict[str, set[str]] = {t: set() for t in tables}
    table_set = set(tables)
    for child in tables:
        for row in conn.execute(f"PRAGMA foreign_key_list({child!r})"):
            parent = row[2]
            if parent in table_set and parent != child:
                deps[child].add(parent)

    in_degree = {t: len(deps[t]) for t in tables}
    queue = deque(t for t in tables if in_degree[t] == 0)
    ordered: list[str] = []
    while queue:
        t = queue.popleft()
        ordered.append(t)
        for other in tables:
            if t in deps.get(other, set()):
                in_degree[other] -= 1
                if in_degree[other] == 0:
                    queue.append(other)

    if len(ordered) != len(tables):
        return sorted(tables)
    return ordered


def _build_create_table_sqlite(
    conn: sqlite3.Connection, table: str
) -> tuple[sql.Composed, list[str]]:
    info = conn.execute(f"PRAGMA table_info({table!r})").fetchall()
    if not info:
        raise ValueError(f"No columns for table {table!r}")

    pk_cols = [row[1] for row in sorted(info, key=lambda r: r[5]) if row[5]]
    col_defs: list[sql.Composed] = []
    col_names: list[str] = []

    for _cid, name, decl, notnull, _dflt, pk in info:
        col_names.append(name)
        pg_t = _pg_type(decl)
        parts: list[sql.SQL | sql.Composed] = [_quote_ident_pg(name), sql.SQL(pg_t)]
        if notnull and not pk:
            parts.append(sql.SQL("NOT NULL"))
        col_defs.append(sql.SQL(" ").join(parts))

    if pk_cols:
        pk_sql = sql.SQL(", ").join(_quote_ident_pg(c) for c in pk_cols)
        col_defs.append(sql.SQL("PRIMARY KEY (") + pk_sql + sql.SQL(")"))

    stmt = (
        sql.SQL("CREATE TABLE ")
        + _quote_ident_pg(table)
        + sql.SQL(" (")
        + sql.SQL(", ").join(col_defs)
        + sql.SQL(")")
    )
    return stmt, col_names


def _ensure_database(
    admin_kwargs: dict, maintenance_db: str, db_name: str
) -> None:
    conn = psycopg2.connect(dbname=maintenance_db, **admin_kwargs)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s", (db_name,)
            )
            if cur.fetchone() is None:
                cur.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name))
                )
    finally:
        conn.close()


def _drop_public_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
            """
        )
        names = [r[0] for r in cur.fetchall()]

    try:
        with conn.cursor() as cur:
            for name in names:
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
                        sql.Identifier(name)
                    )
                )
    finally:
        pass


def _copy_table(
    sqlite_conn: sqlite3.Connection,
    pg_conn,
    table: str,
    col_names: list[str],
) -> None:
    if not col_names:
        return

    # Explicit column list so row order/width always matches PRAGMA table_info.
    # (SELECT * is fragile if the table definition and PRAGMA ever diverge.)
    sqlite_cols = ", ".join(_sqlite_quote_ident(c) for c in col_names)
    select_sql = f"SELECT {sqlite_cols} FROM {_sqlite_quote_ident(table)}"
    cur_s = sqlite_conn.execute(select_sql)
    rows = cur_s.fetchall()
    if not rows:
        return

    cols_sql = sql.SQL(", ").join(_quote_ident_pg(c) for c in col_names)
    placeholders = sql.SQL(", ").join(
        [sql.Placeholder() for _ in col_names]
    )
    insert = (
        sql.SQL("INSERT INTO ")
        + _quote_ident_pg(table)
        + sql.SQL(" (")
        + cols_sql
        + sql.SQL(") VALUES (")
        + placeholders
        + sql.SQL(")")
    )

    tuples: list[tuple] = []
    for row in rows:
        t = tuple(row)
        if len(t) != len(col_names):
            raise ValueError(
                f"Row length {len(t)} != column count {len(col_names)} "
                f"for table {table!r}"
            )
        tuples.append(t)

    with pg_conn.cursor() as cur:
        insert_sql = _escape_percent_for_mogrify(insert.as_string(cur))
        execute_batch(cur, insert_sql, tuples, page_size=100)
    pg_conn.commit()


def _copy_sqlite_file_to_postgres(
    sqlite_path: Path,
    admin_kwargs: dict,
    maintenance_db: str,
) -> None:
    stem = sqlite_path.stem
    if not _IDENT_SAFE.match(stem):
        raise ValueError(
            f"Database name derived from {sqlite_path.name!r} is not a safe "
            f"PostgreSQL identifier: {stem!r}"
        )

    _ensure_database(admin_kwargs, maintenance_db, stem)

    target_kwargs = {**admin_kwargs, "dbname": stem}
    pg_conn = psycopg2.connect(**target_kwargs)
    try:
        _drop_public_tables(pg_conn)

        sqlite_conn = sqlite3.connect(str(sqlite_path))
        try:
            tables = _sqlite_user_tables(sqlite_conn)
            if not tables:
                return
            ordered = _table_create_order(sqlite_conn, tables)

            with pg_conn.cursor() as cur:
                for table in ordered:
                    create_stmt, col_names = _build_create_table_sqlite(
                        sqlite_conn, table
                    )
                    cur.execute(create_stmt)
            pg_conn.commit()

            for table in ordered:
                _, col_names = _build_create_table_sqlite(sqlite_conn, table)
                _copy_table(sqlite_conn, pg_conn, table, col_names)
        finally:
            sqlite_conn.close()
    finally:
        pg_conn.close()


def _admin_connect_kwargs() -> dict:
    host = os.getenv("DB_HOST")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    port = os.getenv("DB_PORT", "5432")
    if not all([host, user, password is not None]):
        raise RuntimeError(
            "Missing PostgreSQL settings in .env: need DB_HOST, DB_USER, DB_PASSWORD "
            "(and optionally DB_PORT)."
        )
    kwargs: dict = {
        "host": host,
        "user": user,
        "password": password,
        "port": port,
    }
    sslmode = os.getenv("PG_SSLMODE")
    if sslmode:
        kwargs["sslmode"] = sslmode
    return kwargs


def main() -> None:
    _load_env()

    db_dir = os.getenv("DATABASE_PATH")
    if not db_dir:
        raise RuntimeError("DATABASE_PATH is not set in .env")

    db_path = Path(db_dir).expanduser().resolve()
    if not db_path.is_dir():
        raise RuntimeError(f"DATABASE_PATH is not a directory: {db_path}")

    admin_kwargs = _admin_connect_kwargs()
    maintenance_db = os.getenv("PG_MAINTENANCE_DB", "postgres")

    sqlite_files = sorted(db_path.glob("*.db"))
    if not sqlite_files:
        print(f"No .db files found under {db_path}")
        return

    for sqlite_file in sqlite_files:
        try:
            _copy_sqlite_file_to_postgres(
                sqlite_file, admin_kwargs, maintenance_db
            )
            print(f"Copied {sqlite_file.name} -> PostgreSQL database {sqlite_file.stem!r}")
        except Exception as e:
            print(f"Failed to import {sqlite_file.name}: {e}")
            raise


if __name__ == "__main__":
    main()
