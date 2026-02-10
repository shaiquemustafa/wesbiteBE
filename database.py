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
        _pool = SimpleConnectionPool(
            1,
            8,
            dsn=DATABASE_URL,
            sslmode="require",
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )
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
    """
    Yields a live pooled connection with commit/rollback handling.
    Automatically detects and replaces stale/dead connections
    (e.g. after Neon drops idle SSL connections).
    """
    if _pool is None:
        raise ConnectionError("Database connection is not established.")

    conn = _pool.getconn()

    # --- Liveness check: ping the connection before handing it out ---
    alive = False
    if not conn.closed:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            conn.rollback()          # clear the implicit txn from the ping
            alive = True
        except Exception:
            pass                      # connection is dead

    if not alive:
        # Discard the dead connection and get a fresh one from the pool
        try:
            _pool.putconn(conn, close=True)
        except Exception:
            pass
        conn = _pool.getconn()        # pool will create a brand-new connection

    try:
        yield conn
        if not conn.closed:
            conn.commit()
    except Exception:
        # Attempt rollback, but don't fail if the connection is already gone
        if not conn.closed:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        try:
            _pool.putconn(conn, close=(conn.closed != 0))
        except Exception:
            pass


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