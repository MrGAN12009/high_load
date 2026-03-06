from os import getenv
from typing import Any

from psycopg2 import pool


_db_pool: pool.SimpleConnectionPool | None = None
_MIGRATION_LOCK_KEY = 424242001
_MARKETPLACE_CATEGORIES = (
    "Electronics",
    "Home",
    "Fashion",
    "Beauty",
    "Sports",
    "Books",
    "Toys",
    "Automotive",
    "Food",
    "Pets",
)


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
            # Serialize migrations between workers on shared Postgres.
            cur.execute("SELECT pg_advisory_xact_lock(%s);", (_MIGRATION_LOCK_KEY,))
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
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS categories (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL UNIQUE
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS products (
                    id BIGSERIAL PRIMARY KEY,
                    category_id INT NOT NULL REFERENCES categories(id) ON DELETE RESTRICT,
                    name VARCHAR(255) NOT NULL,
                    description TEXT NOT NULL,
                    price NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
                    stock INT NOT NULL CHECK (stock >= 0),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_products_category_id ON products (category_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_products_name_lower ON products (LOWER(name));")
            cur.executemany(
                """
                INSERT INTO categories (name)
                VALUES (%s)
                ON CONFLICT (name) DO NOTHING;
                """,
                [(category,) for category in _MARKETPLACE_CATEGORIES],
            )
            cur.execute(
                """
                WITH needed_rows AS (
                    SELECT GREATEST(0, 1000 - COUNT(*)) AS cnt
                    FROM products
                ),
                series AS (
                    SELECT generate_series(1, (SELECT cnt FROM needed_rows)) AS n
                ),
                generated AS (
                    SELECT
                        n,
                        (ARRAY[
                            'Smart', 'Ultra', 'Eco', 'Classic', 'Pro', 'Compact', 'Premium', 'Daily', 'Urban', 'Flex'
                        ])[1 + FLOOR(RANDOM() * 10)::INT] AS adjective,
                        (ARRAY[
                            'Portable', 'Modern', 'Advanced', 'Comfort', 'Essential', 'Travel', 'Performance', 'Studio', 'Family', 'Lite'
                        ])[1 + FLOOR(RANDOM() * 10)::INT] AS style,
                        (ARRAY[
                            'Bundle', 'Set', 'Kit', 'Pack', 'Edition', 'Model', 'Series', 'Selection', 'Version', 'Collection'
                        ])[1 + FLOOR(RANDOM() * 10)::INT] AS noun,
                        (ARRAY[
                            'Electronics', 'Home', 'Fashion', 'Beauty', 'Sports', 'Books', 'Toys', 'Automotive', 'Food', 'Pets'
                        ])[1 + FLOOR(RANDOM() * 10)::INT] AS category_name
                    FROM series
                )
                INSERT INTO products (category_id, name, description, price, stock)
                SELECT
                    c.id,
                    CONCAT(g.adjective, ' ', g.style, ' ', g.noun, ' #', g.n),
                    CONCAT('Marketplace product #', g.n, ' in category ', g.category_name, '.'),
                    ROUND((5 + RANDOM() * 1995)::NUMERIC, 2),
                    (10 + FLOOR(RANDOM() * 190))::INT
                FROM generated g
                JOIN categories c ON c.name = g.category_name;
                """
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
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


def fetch_categories() -> list[str]:
    if _db_pool is None:
        return []

    conn = _db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT name
                FROM categories
                ORDER BY name;
                """
            )
            rows = cur.fetchall()
        return [row[0] for row in rows]
    finally:
        _db_pool.putconn(conn)


def search_products(query: str = "", category: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
    if _db_pool is None:
        return []

    normalized_query = query.strip()
    query_pattern = f"%{normalized_query}%"
    normalized_category = category.strip() if category else None
    if normalized_category == "":
        normalized_category = None

    conn = _db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.id, p.name, p.description, p.price, p.stock, c.name
                FROM products p
                JOIN categories c ON c.id = p.category_id
                WHERE
                    (%s = '' OR p.name ILIKE %s OR p.description ILIKE %s)
                    AND (%s IS NULL OR c.name = %s)
                ORDER BY p.id DESC
                LIMIT %s;
                """,
                (
                    normalized_query,
                    query_pattern,
                    query_pattern,
                    normalized_category,
                    normalized_category,
                    limit,
                ),
            )
            rows = cur.fetchall()

        return [
            {
                "id": row[0],
                "name": row[1],
                "description": row[2],
                "price": float(row[3]),
                "stock": row[4],
                "category": row[5],
            }
            for row in rows
        ]
    finally:
        _db_pool.putconn(conn)
