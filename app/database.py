from os import getenv
from typing import Any

from psycopg2 import pool


_db_pool: pool.SimpleConnectionPool | None = None


def _dsn() -> str:
    host = getenv("POSTGRES_HOST", "postgres")
    port = getenv("POSTGRES_PORT", "5432")
    user = getenv("POSTGRES_USER", "postgres")
    password = getenv("POSTGRES_PASSWORD", "postgres")
    dbname = getenv("POSTGRES_DB", "roundrobin")
    return f"host={host} port={port} user={user} password={password} dbname={dbname}"


def init_db_pool() -> None:
    global _db_pool
    if _db_pool is not None:
        return

    _db_pool = pool.SimpleConnectionPool(minconn=1, maxconn=10, dsn=_dsn())
    _ensure_tables()


def close_db_pool() -> None:
    global _db_pool
    if _db_pool is not None:
        _db_pool.closeall()
        _db_pool = None


def _ensure_tables() -> None:
    if _db_pool is None:
        return

    conn = _db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS request_logs (
                    id BIGSERIAL PRIMARY KEY,
                    worker_name VARCHAR(100) NOT NULL,
                    path VARCHAR(255) NOT NULL,
                    client_ip VARCHAR(64),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
        conn.commit()
    finally:
        _db_pool.putconn(conn)


def write_request_log(worker_name: str, path: str, client_ip: str | None) -> None:
    if _db_pool is None:
        return

    conn = _db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO request_logs (worker_name, path, client_ip)
                VALUES (%s, %s, %s);
                """,
                (worker_name, path, client_ip),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _db_pool.putconn(conn)


def fetch_last_logs(limit: int = 20) -> list[dict[str, Any]]:
    if _db_pool is None:
        return []

    conn = _db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, worker_name, path, client_ip, created_at
                FROM request_logs
                ORDER BY id DESC
                LIMIT %s;
                """,
                (limit,),
            )
            rows = cur.fetchall()
        return [
            {
                "id": row[0],
                "worker": row[1],
                "path": row[2],
                "client_ip": row[3],
                "created_at": row[4].isoformat(),
            }
            for row in rows
        ]
    finally:
        _db_pool.putconn(conn)
