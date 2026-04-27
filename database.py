import os
import json
from contextlib import contextmanager
from dotenv import load_dotenv
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import register_default_jsonb

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in .env file. Please set it.")

register_default_jsonb(loads=json.loads, globally=True)

# Pool sizing — must comfortably exceed WHATSAPP_PARALLELISM since each
# broadcast worker may grab a connection to record delivery status.
DB_POOL_MIN = int(os.getenv("DB_POOL_MIN", "2"))
DB_POOL_MAX = int(os.getenv("DB_POOL_MAX", "30"))

_pool: ThreadedConnectionPool | None = None


def connect_to_db():
    """Initializes the Postgres connection pool and ensures tables exist."""
    global _pool
    if _pool is None:
        # ThreadedConnectionPool is thread-safe (unlike SimpleConnectionPool).
        # We need this because broadcasts now fan out across many worker
        # threads, and each may call get_conn() concurrently.
        _pool = ThreadedConnectionPool(
            DB_POOL_MIN,
            DB_POOL_MAX,
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


def get_pool_status() -> dict:
    """Returns basic pool sizing info for diagnostics."""
    return {
        "min": DB_POOL_MIN,
        "max": DB_POOL_MAX,
        "initialized": _pool is not None,
    }


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
                data JSONB NOT NULL,
                analyzed BOOLEAN NOT NULL DEFAULT FALSE
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
            CREATE TABLE IF NOT EXISTS company_master (
                bse_scrip_code INTEGER PRIMARY KEY,
                isin TEXT,
                company_name TEXT NOT NULL,
                nse_symbol TEXT,
                mkt_cap_full DOUBLE PRECISION,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )
        # Add 'analyzed' column if it doesn't exist (safe for existing tables)
        cur.execute(
            """
            ALTER TABLE raw_bse_announcements
                ADD COLUMN IF NOT EXISTS analyzed BOOLEAN NOT NULL DEFAULT FALSE;
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_raw_bse_analyzed
                ON raw_bse_announcements (analyzed)
                WHERE analyzed = FALSE;
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
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_company_master_nse_symbol
                ON company_master (nse_symbol)
                WHERE nse_symbol IS NOT NULL;
            """
        )
        # Industry classification columns (24-label canonical taxonomy).
        # Backfilled by POST /api/admin/backfill-industries; populated for
        # new companies inside CompanyService.upsert_company().
        cur.execute(
            """
            ALTER TABLE company_master
                ADD COLUMN IF NOT EXISTS industry TEXT,
                ADD COLUMN IF NOT EXISTS industry_source TEXT,
                ADD COLUMN IF NOT EXISTS industry_updated_at TIMESTAMPTZ;
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_company_master_industry
                ON company_master (industry)
                WHERE industry IS NOT NULL;
            """
        )

        # ── Auth tables ──────────────────────────────────────────────
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id BIGSERIAL PRIMARY KEY,
                phone VARCHAR(15) UNIQUE NOT NULL,
                name VARCHAR(100),
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                last_login_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )
        # Add inbound message tracking columns (idempotent)
        cur.execute(
            """
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS last_incoming_message_text TEXT,
            ADD COLUMN IF NOT EXISTS last_incoming_message_at TIMESTAMPTZ;
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS otp_requests (
                id BIGSERIAL PRIMARY KEY,
                phone VARCHAR(15) NOT NULL,
                otp_code VARCHAR(6) NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL,
                is_verified BOOLEAN DEFAULT FALSE,
                attempts INTEGER DEFAULT 0,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_users_phone
                ON users (phone);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_otp_phone_expires
                ON otp_requests (phone, expires_at);
            """
        )

        # ── User watchlist ────────────────────────────────────────────
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_watchlist (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                bse_scrip_code INTEGER NOT NULL,
                added_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (user_id, bse_scrip_code)
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_watchlist_user
                ON user_watchlist (user_id);
            """
        )
        # Fast lookup: "which users watch this stock?" — used when news arrives
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_watchlist_scrip
                ON user_watchlist (bse_scrip_code);
            """
        )
        
        # ── Denormalized table for users who want all updates ───────────────────
        # This table provides instant lookup without scanning the entire users table
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users_receive_all_updates (
                phone VARCHAR(20) PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_users_receive_all_phone
                ON users_receive_all_updates (phone);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_users_receive_all_user_id
                ON users_receive_all_updates (user_id);
            """
        )
        
        # Populate the table with existing users who have receive_all_updates = TRUE
        cur.execute(
            """
            INSERT INTO users_receive_all_updates (phone, user_id)
            SELECT phone, id FROM users 
            WHERE is_active = TRUE AND receive_all_updates = TRUE
            ON CONFLICT (phone) DO NOTHING;
            """
        )
        # Add receive_all_updates column to users (default TRUE for new users)
        cur.execute(
            """
            ALTER TABLE users
                ADD COLUMN IF NOT EXISTS receive_all_updates BOOLEAN DEFAULT TRUE;
            """
        )
        # Ensure the default is TRUE (may have been created with FALSE previously)
        cur.execute(
            """
            ALTER TABLE users
                ALTER COLUMN receive_all_updates SET DEFAULT TRUE;
            """
        )
        # Track whether user has completed stock selection onboarding
        cur.execute(
            """
            ALTER TABLE users
                ADD COLUMN IF NOT EXISTS onboarding_complete BOOLEAN DEFAULT FALSE;
            """
        )

        # Lightweight table for low-impact watchlist notifications
        # (N/A, NEUTRAL, MATCHED — no Indian API enrichment, just OpenAI summary)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS watchlist_notifications (
                id BIGSERIAL PRIMARY KEY,
                scrip_cd VARCHAR(20),
                company_name VARCHAR(255),
                impact VARCHAR(50),
                category VARCHAR(100),
                summary TEXT,
                pdf_link TEXT,
                news_time TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_watchlist_notif_created
                ON watchlist_notifications (created_at);
            """
        )

        # User events table (track website visits, etc.)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_events (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
                event_type VARCHAR(50) NOT NULL DEFAULT 'page_visit',
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_user_events_user_id
                ON user_events (user_id);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_user_events_created
                ON user_events (created_at);
            """
        )

        # Scheduled jobs table — tracks last_run_at for periodic background jobs
        # (e.g. the 'user_training_broadcast' that goes out every 15 days)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS scheduled_jobs (
                job_name VARCHAR(100) PRIMARY KEY,
                last_run_at TIMESTAMPTZ,
                last_status VARCHAR(50),
                last_meta JSONB,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )

        # WhatsApp broadcast table (filtered bulk messages for all users)
        # Stores only: STRONGLY POSITIVE (all >2,500 Cr), STRONGLY NEGATIVE (>10K Cr), FINANCIAL RESULTS. Regular NEGATIVE is excluded.
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS whatsapp_broadcast (
                id BIGSERIAL PRIMARY KEY,
                scrip_cd VARCHAR(20),
                company_name VARCHAR(255),
                impact VARCHAR(50),
                category VARCHAR(100),
                summary TEXT,
                pdf_link TEXT,
                news_time TIMESTAMPTZ,
                mkt_cap_cr NUMERIC,
                data JSONB,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                sent_at TIMESTAMPTZ
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_whatsapp_broadcast_created
                ON whatsapp_broadcast (created_at);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_whatsapp_broadcast_scrip
                ON whatsapp_broadcast (scrip_cd);
            """
        )
        # Dedupe lookups: same company + same category (expression matches api.py filter)
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_whatsapp_broadcast_scrip_cat
                ON whatsapp_broadcast (scrip_cd, (UPPER(TRIM(COALESCE(category, '')))));
            """
        )
        # Add sent_at column if it doesn't exist (migration for existing tables)
        # MUST be done before creating index on sent_at
        cur.execute("""
            DO $$ 
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'whatsapp_broadcast' 
                    AND column_name = 'sent_at'
                ) THEN
                    ALTER TABLE whatsapp_broadcast ADD COLUMN sent_at TIMESTAMPTZ;
                END IF;
            END $$;
        """)
        # Create index on sent_at ONLY if the column exists
        cur.execute("""
            DO $$ 
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'whatsapp_broadcast' 
                    AND column_name = 'sent_at'
                ) THEN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_indexes 
                        WHERE tablename = 'whatsapp_broadcast' 
                        AND indexname = 'idx_whatsapp_broadcast_sent'
                    ) THEN
                        CREATE INDEX idx_whatsapp_broadcast_sent
                            ON whatsapp_broadcast (sent_at) WHERE sent_at IS NULL;
                    END IF;
                END IF;
            END $$;
        """)

        # Message delivery status table (tracks WhatsApp message delivery via Gupshup webhooks)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS message_delivery_status (
                id BIGSERIAL PRIMARY KEY,
                message_id VARCHAR(255) UNIQUE,  -- Gupshup message ID
                phone VARCHAR(20) NOT NULL,  -- Recipient phone number
                user_name VARCHAR(100),  -- User name from users table
                message_title VARCHAR(255),  -- "OTP" or company name for broadcasts
                status VARCHAR(50) NOT NULL,  -- sent, delivered, read, failed, enqueued
                error_code VARCHAR(100),  -- Error code if failed
                error_message TEXT,  -- Error message if failed
                timestamp TIMESTAMPTZ NOT NULL,  -- When status was updated
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                raw_payload JSONB  -- Store full webhook payload for debugging
            );
            """
        )
        # Add columns if they don't exist (migration for existing tables)
        cur.execute("""
            DO $$ 
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'message_delivery_status' 
                    AND column_name = 'user_name'
                ) THEN
                    ALTER TABLE message_delivery_status ADD COLUMN user_name VARCHAR(100);
                END IF;
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = 'message_delivery_status' 
                    AND column_name = 'message_title'
                ) THEN
                    ALTER TABLE message_delivery_status ADD COLUMN message_title VARCHAR(255);
                END IF;
            END $$;
        """)
        
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_message_delivery_phone
                ON message_delivery_status (phone);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_message_delivery_status
                ON message_delivery_status (status);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_message_delivery_timestamp
                ON message_delivery_status (timestamp);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_message_delivery_message_id
                ON message_delivery_status (message_id);
            """
        )

        # Daily RSS briefing (news_scratch pipeline) — stored for internal review;
        # no UI/comms wiring yet. Times are naive IST wall clock in *_ist columns.
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS news_briefing_runs (
                id BIGSERIAL PRIMARY KEY,
                cycle VARCHAR(20) NOT NULL
                    CHECK (cycle IN ('pre_market', 'during_market', 'post_market')),
                briefing_date_ist DATE NOT NULL,
                run_at_ist TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                window_start_ist TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                window_end_ist TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                status VARCHAR(30) NOT NULL DEFAULT 'completed',
                error_message TEXT,
                stock_news_rows INT NOT NULL DEFAULT 0,
                industry_insight_rows INT NOT NULL DEFAULT 0,
                all_items_rows INT NOT NULL DEFAULT 0,
                cross_cycle_skipped INT NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_news_briefing_runs_date
                ON news_briefing_runs (briefing_date_ist DESC);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_news_briefing_runs_cycle_date
                ON news_briefing_runs (cycle, briefing_date_ist);
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS news_briefing_industry_insights (
                id BIGSERIAL PRIMARY KEY,
                run_id BIGINT NOT NULL REFERENCES news_briefing_runs(id) ON DELETE CASCADE,
                insight_tier VARCHAR(10) NOT NULL DEFAULT 'full',
                sort_order INT NOT NULL DEFAULT 0,
                industry TEXT,
                n_items INT,
                direction TEXT,
                headline TEXT,
                bullets TEXT,
                key_numbers TEXT,
                key_stocks TEXT,
                key_themes TEXT
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_news_briefing_ins_run
                ON news_briefing_industry_insights (run_id);
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS news_briefing_stock_news (
                id BIGSERIAL PRIMARY KEY,
                run_id BIGINT NOT NULL REFERENCES news_briefing_runs(id) ON DELETE CASCADE,
                sort_order INT NOT NULL,
                affected_stocks TEXT,
                bse_scrip_code TEXT,
                nse_symbol TEXT,
                isin TEXT,
                market_cap_cr TEXT,
                score DOUBLE PRECISION,
                direction TEXT,
                industry_primary TEXT,
                affected_industries TEXT,
                ai_summary TEXT,
                implication TEXT,
                key_numbers TEXT,
                category TEXT,
                source TEXT,
                published_at_ist TIMESTAMP WITHOUT TIME ZONE,
                title TEXT,
                link TEXT,
                story_fingerprint CHAR(64) NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_news_briefing_sn_run
                ON news_briefing_stock_news (run_id);
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS news_briefing_all_items (
                id BIGSERIAL PRIMARY KEY,
                run_id BIGINT NOT NULL REFERENCES news_briefing_runs(id) ON DELETE CASCADE,
                sort_order INT NOT NULL,
                industry_primary TEXT,
                affected_stocks TEXT,
                bse_scrip_code TEXT,
                nse_symbol TEXT,
                isin TEXT,
                market_cap_cr TEXT,
                score DOUBLE PRECISION,
                direction TEXT,
                category TEXT,
                ai_summary TEXT,
                implication TEXT,
                key_numbers TEXT,
                source TEXT,
                published_at_ist TIMESTAMP WITHOUT TIME ZONE,
                title TEXT,
                link TEXT,
                story_fingerprint CHAR(64) NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_news_briefing_ai_run
                ON news_briefing_all_items (run_id);
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS news_briefing_seen_stories (
                briefing_date_ist DATE NOT NULL,
                story_fingerprint CHAR(64) NOT NULL,
                first_seen_run_id BIGINT REFERENCES news_briefing_runs(id) ON DELETE SET NULL,
                first_seen_cycle VARCHAR(20),
                created_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (briefing_date_ist, story_fingerprint)
            );
            """
        )

        # Table metadata/documentation (definitions, rules, cutoffs, functions)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS table_metadata (
                id BIGSERIAL PRIMARY KEY,
                table_name VARCHAR(100) UNIQUE NOT NULL,
                description TEXT NOT NULL,
                cutoff_rule TEXT,
                filtering_rules TEXT,
                purpose TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )

        # Insert initial metadata for all tables
        metadata_records = [
            (
                "ui_data",
                "Stores all impactful market news for display on the website. Contains full enrichment data (prices, analyst consensus, quarterly results). Only companies >2,500 Cr market cap are processed.",
                "48 hours - entries older than 48 hours are automatically deleted.",
                "Includes: POSITIVE, STRONGLY POSITIVE, NEGATIVE, STRONGLY NEGATIVE, BEAT, MISSED (for companies >2,500 Cr). Excludes: NEUTRAL, MATCHED, N/A, and companies <2,500 Cr.",
                "Website display - shows all impactful news to users browsing the dashboard.",
            ),
            (
                "watchlist_notifications",
                "Lightweight table for low-impact announcements sent only to users who follow specific stocks.",
                "48 hours - entries older than 48 hours are automatically deleted.",
                "Includes: NEUTRAL, MATCHED, N/A, LIKELY NEUTRAL, and other low-impact variations. No Indian API enrichment.",
                "WhatsApp notifications for watchlist users - ensures users get ALL announcements for stocks they follow, even if immaterial.",
            ),
            (
                "whatsapp_broadcast",
                "Filtered bulk messages table for WhatsApp notifications to all users. Stricter filtering than ui_data.",
                "48 hours - entries older than 48 hours are automatically deleted.",
                "Includes: (1) STRONGLY POSITIVE or BEAT for all companies >2,500 Cr, (2) STRONGLY NEGATIVE only for non-financial categories and market cap >10,000 Cr, (3) Financial Results category only when impact is STRONGLY POSITIVE or BEAT (not plain POSITIVE/NEUTRAL/NEGATIVE). Excludes: regular NEGATIVE, MISSED, etc. At most one row per scrip_cd + category while data exists (dedupes repeat filings).",
                "WhatsApp bulk notifications - sends high-priority news to all relevant users via WhatsApp.",
            ),
            (
                "user_events",
                "Tracks user activity events like page visits, interactions, etc.",
                "No automatic cleanup - kept for analytics and long-term tracking.",
                "No filtering - all events are recorded.",
                "Analytics and user behavior tracking - records when users open the website, interact with features, etc.",
            ),
            (
                "users",
                "User accounts with authentication, preferences, and profile information.",
                "No automatic cleanup - permanent user data.",
                "No filtering - all users are stored.",
                "User management - stores phone numbers, names, notification preferences (receive_all_updates), onboarding status. last_incoming_message_text holds a JSON array string of the last 4 inbound WhatsApp messages.",
            ),
            (
                "scheduled_jobs",
                "Tracks last_run_at for periodic background jobs (e.g. user_training_broadcast every 15 days).",
                "No automatic cleanup - one row per job_name.",
                "No filtering - one row per job_name.",
                "Background scheduler bookkeeping - guarantees periodic broadcasts run on cadence and are not duplicated on restart.",
            ),
            (
                "user_watchlist",
                "Stocks selected by users for personalized notifications.",
                "No automatic cleanup - permanent user preferences.",
                "No filtering - all watchlist selections are stored.",
                "User preferences - tracks which stocks each user follows (min 3, max 15 stocks per user).",
            ),
            (
                "otp_requests",
                "Temporary OTP codes for WhatsApp login authentication.",
                "1 hour - expired OTPs older than 1 hour are automatically deleted.",
                "No filtering - all OTP requests are stored temporarily.",
                "Authentication - stores OTP codes with 5-minute expiry for secure login via WhatsApp.",
            ),
            (
                "predictions",
                "Stored predictions/analysis results from BSE announcements (legacy table). Only companies >2,500 Cr market cap are processed.",
                "48 hours - entries older than 48 hours are automatically deleted.",
                "Excludes: NEUTRAL, MATCHED, N/A, and companies <2,500 Cr. Only impactful predictions are stored.",
                "Legacy storage - historical predictions data (may be phased out in favor of ui_data).",
            ),
            (
                "raw_bse_announcements",
                "Raw announcement data fetched from BSE website before processing.",
                "48 hours - entries older than 48 hours are automatically deleted.",
                "No filtering - all fetched announcements are stored temporarily.",
                "Data pipeline - stores raw BSE data before PDF download and analysis.",
            ),
            (
                "news_briefing_runs",
                "One row per RSS briefing job (pre_market / during_market / post_market). "
                "Stores IST window boundaries and row counts; children hold sheet payloads.",
                "Older same-cycle rows for the previous IST calendar day are deleted when a new run succeeds.",
                "Cron-triggered news_scratch pipeline output for internal review (no UI yet).",
                "Internal daily digest storage.",
            ),
            (
                "news_briefing_industry_insights",
                "Industry-level LLM bullets for a briefing run (full + small tiers).",
                "Deleted when parent news_briefing_runs row is deleted (ON DELETE CASCADE).",
                "Mirrors industry_insights + industry_small Excel sheets.",
                "Sector narrative archive per run.",
            ),
            (
                "news_briefing_stock_news",
                "Stock-focused briefing rows with BSE/NSE/ISIN/market cap columns.",
                "Deleted when parent run is deleted.",
                "Cross-cycle dedupe uses news_briefing_seen_stories + story_fingerprint.",
                "Stock-level RSS digest archive.",
            ),
            (
                "news_briefing_all_items",
                "All items by primary industry for a briefing run.",
                "Deleted when parent run is deleted.",
                "Cross-cycle dedupe by story_fingerprint per briefing_date_ist.",
                "Full funnel survivor list per run.",
            ),
            (
                "news_briefing_seen_stories",
                "Per trading-day IST fingerprints of stories already stored (stock_news + all_items).",
                "Optional cleanup of rows older than ~14 days can be done by the job.",
                "Prevents duplicate link/title across the three daily cycles.",
                "Cross-cycle dedupe index.",
            ),
        ]

        for record in metadata_records:
            cur.execute(
                """
                INSERT INTO table_metadata (table_name, description, cutoff_rule, filtering_rules, purpose)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (table_name) DO UPDATE SET
                    description = EXCLUDED.description,
                    cutoff_rule = EXCLUDED.cutoff_rule,
                    filtering_rules = EXCLUDED.filtering_rules,
                    purpose = EXCLUDED.purpose,
                    updated_at = NOW()
                """,
                record,
            )