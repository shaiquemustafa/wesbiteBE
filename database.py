import os
import json
from contextlib import contextmanager
from dotenv import load_dotenv
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import register_default_jsonb

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in .env file. Please set it.")

register_default_jsonb(loads=json.loads, globally=True)

_pool: SimpleConnectionPool | None = None


def connect_to_db():
    """Initializes the Postgres connection pool and ensures tables exist."""
    global _pool
    if _pool is None:
        _pool = SimpleConnectionPool(1, 8, dsn=DATABASE_URL, sslmode="require")
        with get_conn() as conn:
            _ensure_tables(conn)


def close_db_connection():
    """Closes the Postgres connection pool."""
    global _pool
    if _pool:
        _pool.closeall()
        _pool = None


@contextmanager
def get_conn():
    """Yields a pooled connection with commit/rollback handling."""
    if _pool is None:
        raise ConnectionError("Database connection is not established.")
    conn = _pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)


def _ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_bse_announcements (
                newsid TEXT PRIMARY KEY,
                news_submission_dt TIMESTAMPTZ,
                data JSONB NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS predictions (
                id BIGSERIAL PRIMARY KEY,
                scrip_cd TEXT NOT NULL,
                pdf_link TEXT NOT NULL,
                impact TEXT,
                news_submission_dt TIMESTAMPTZ,
                data JSONB NOT NULL,
                UNIQUE (scrip_cd, pdf_link)
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ui_data (
                id BIGSERIAL PRIMARY KEY,
                news_time TIMESTAMPTZ,
                category TEXT,
                data JSONB NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_predictions_news_submission_dt
                ON predictions (news_submission_dt);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_predictions_impact
                ON predictions (impact);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_ui_data_news_time
                ON ui_data (news_time);
            """
        )