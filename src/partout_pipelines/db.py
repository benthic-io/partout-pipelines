from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any

from .config import Config


class DatabaseError(RuntimeError):
    pass


@contextmanager
def connect(
    cfg: Config,
    dbname: str | None = None,
    *,
    autocommit: bool = False,
) -> Iterator[Any]:
    try:
        import psycopg2
    except ImportError as exc:
        raise DatabaseError("psycopg2 is required for database operations") from exc
    connection = psycopg2.connect(**cfg.database.connect_kwargs(dbname))
    connection.autocommit = autocommit
    try:
        yield connection
        if not autocommit:
            connection.commit()
    except Exception:
        if not autocommit:
            connection.rollback()
        raise
    finally:
        connection.close()


def database_exists(cfg: Config, dbname: str) -> bool:
    with connect(cfg, cfg.database.maintenance_db) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
            return cursor.fetchone() is not None


def create_database(cfg: Config, dbname: str) -> None:
    serving = {cfg.database.name, *cfg.database.targets.values()}
    if dbname in serving:
        raise DatabaseError(
            f"refusing to create serving database {dbname!r}; "
            "select a validation database explicitly"
        )
    if database_exists(cfg, dbname):
        return
    try:
        from psycopg2 import sql
    except ImportError as exc:
        raise DatabaseError("psycopg2 is required for database operations") from exc
    with connect(cfg, cfg.database.maintenance_db, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))


def analyze(cfg: Config, dbname: str) -> None:
    with connect(cfg, dbname, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute("ANALYZE")


def grant_read_access(
    cfg: Config,
    dbname: str,
    *,
    roles: Sequence[str],
    schemas: Sequence[str],
) -> list[str]:
    if not roles or not schemas:
        return []
    try:
        from psycopg2 import sql
    except ImportError as exc:
        raise DatabaseError("psycopg2 is required for database operations") from exc
    with connect(cfg, dbname) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)", (list(roles),))
            existing_roles = [row[0] for row in cursor.fetchall()]
            if not existing_roles:
                return []
            role_list = [sql.Identifier(role) for role in existing_roles]
            exposed: list[str] = []
            for schema in schemas:
                cursor.execute("SELECT to_regnamespace(%s)", (schema,))
                if cursor.fetchone()[0] is None:
                    continue
                schema_identifier = sql.Identifier(schema)
                grants = sql.SQL("GRANT USAGE ON SCHEMA {} TO ").format(schema_identifier)
                grants = grants + sql.SQL(", ").join(role_list) + sql.SQL(";")
                cursor.execute(grants)
                grants = sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO ").format(
                    schema_identifier
                )
                grants = grants + sql.SQL(", ").join(role_list) + sql.SQL(";")
                cursor.execute(grants)
                exposed.append(schema)
            return exposed


def quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
