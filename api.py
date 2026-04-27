import os
import pandas as pd
from datetime import datetime, timedelta, timezone
import hashlib
import requests
from fastapi import FastAPI, Query, Path, HTTPException, BackgroundTasks, Header, Request, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from contextlib import asynccontextmanager
import asyncio
import json
import logging

from announcements import fetch_and_filter_announcements
from results import _process_single_announcement, _impact_map
from stock_enrichment import enrich_prediction
from database import connect_to_db, close_db_connection, get_conn
from psycopg2.extras import Json
from service.announcement_service import AnnouncementService
from service.ui_data_service import UIDataService
from service.company_service import CompanyService
from service.auth_service import AuthService
from service.notification_service import NotificationService
from service.watchlist_service import WatchlistService
from service.whatsapp_service import WhatsAppService
from entity.ui_data import UIDataItem
from typing import Dict, List, Optional, Tuple

# Fixed IST offset (no tzdata dependency)
IST = timezone(timedelta(hours=5, minutes=30))

# Scheduler config (env-overridable)
SCHEDULER_INTERVAL_MIN = int(os.getenv("SCHEDULER_INTERVAL_MIN", "2"))
SCHEDULER_ENABLED = os.getenv("SCHEDULER_ENABLED", "true").lower() == "true"
# How often the 'user_training' utility template (feature explainer) is broadcast
USER_TRAINING_INTERVAL_DAYS = int(os.getenv("USER_TRAINING_INTERVAL_DAYS", "15"))
USER_TRAINING_JOB_NAME = "user_training_broadcast"
META_PIXEL_ID = os.getenv("META_PIXEL_ID", "1245580987757334")
META_ACCESS_TOKEN = os.getenv(
    "META_ACCESS_TOKEN",
    "EAAVKbgAbnP0BQ7R5gQyzD1EcWRRTlhyWF0uUe5vJtFqsqOA9snAQ77JZBeo5ctV2uVpZAFnlzoQdWrFXRMgOnxoGm74MI859Bnv8ZC4WFGV5omm1EciZA0eZCrtRn4GKmxnXoSOh56CoSZCsFCKS8OdxT5l5JRJLzyk6XJtQD5HIadVgMWFSHEPUreISSfqo1qdQZDZD",
)
META_TEST_EVENT_CODE = os.getenv("META_TEST_EVENT_CODE", "").strip()


def _now_ist_naive() -> datetime:
    """Current time in IST as a naive datetime."""
    return datetime.now(IST).replace(tzinfo=None)

def _to_ist(dt) -> datetime:
    """
    Converts a datetime to IST timezone.
    Handles naive datetimes, UTC datetimes, and already IST datetimes.
    Returns timezone-aware IST datetime.
    """
    if dt is None:
        return None
    
    # If already timezone-aware
    if dt.tzinfo is not None:
        # Convert to IST
        return dt.astimezone(IST)
    else:
        # If naive, assume it's already in IST and add IST timezone
        return dt.replace(tzinfo=IST)


analysis_lock = asyncio.Lock()
logger = logging.getLogger("uvicorn.error")
_scheduler_task: asyncio.Task | None = None


async def _send_pending_broadcasts():
    """
    Async wrapper: runs the (synchronous, network-heavy) pending-broadcast
    sender on a worker thread so the asyncio event loop stays free. OTPs,
    signups, and other API calls keep being served while sends are in
    flight.
    """
    await asyncio.to_thread(_send_pending_broadcasts_sync)


def _send_pending_broadcasts_sync():
    """
    Automatically sends notifications for entries in whatsapp_broadcast
    that haven't been sent yet (sent_at IS NULL). Each entry is dispatched
    via WhatsAppService.send_market_update_broadcast which fans out to
    Gupshup in parallel using the dedicated WhatsApp thread pool.
    """
    try:
        # Get all unsent entries from the last hour (to avoid sending very old entries)
        unsent_entries = []
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, scrip_cd, company_name, impact, category, summary, pdf_link, 
                           news_time, mkt_cap_cr, data
                    FROM whatsapp_broadcast
                    WHERE sent_at IS NULL
                      AND created_at > NOW() - INTERVAL '1 hour'
                    ORDER BY created_at DESC
                    LIMIT 50
                    """
                )
                rows = cur.fetchall()
                
                for row in rows:
                    entry = {
                        "id": row[0],
                        "scrip_cd": str(row[1]) if row[1] else None,
                        "company_name": row[2],
                        "impact": row[3],
                        "category": row[4],
                        "summary": row[5],
                        "pdf_link": row[6],
                        "news_time": row[7],
                        "mkt_cap_cr": float(row[8]) if row[8] else None,
                    }
                    
                    # Merge data from JSONB if available
                    if row[9]:
                        data_dict = row[9] if isinstance(row[9], dict) else {}
                        entry.update(data_dict)
                    
                    unsent_entries.append(entry)
        
        if not unsent_entries:
            return
        
        logger.info("  🔄 Found %d unsent entries in whatsapp_broadcast, sending notifications...", len(unsent_entries))
        
        # Send notifications
        notif_service = NotificationService()
        sent_count = 0
        
        for entry in unsent_entries:
            try:
                result = notif_service.notify_all_users(entry)
                if result["sent"] > 0:
                    sent_count += 1
                    # Mark as sent
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            # Set sent_at in IST
                            sent_at_ist = _now_ist_naive().replace(tzinfo=IST)
                            cur.execute(
                                """
                                UPDATE whatsapp_broadcast
                                SET sent_at = %s
                                WHERE id = %s
                                """,
                                (sent_at_ist, entry["id"]),
                            )
                    logger.info("  ✅ Sent notification for %s (entry ID: %s)", entry.get("company_name"), entry["id"])
            except Exception as e:
                logger.warning("  ⚠️ Failed to send notification for entry %s: %s", entry.get("id"), e)
        
        if sent_count > 0:
            logger.info("  ✅ Sent %d pending notifications", sent_count)
    except Exception as e:
        logger.error("  ❌ Error in _send_pending_broadcasts: %s", e)


# =========================================================================
#  USER TRAINING BROADCAST
#  Sends the 'user_training' utility template to every active user every
#  USER_TRAINING_INTERVAL_DAYS days. Tracks last_run_at in scheduled_jobs
#  so the cadence survives restarts and is never duplicated.
# =========================================================================
def _format_high_impact_label(receive_all_updates: bool) -> str:
    """'turned on' if user opted into high-impact news, else 'turned off'."""
    return "turned on" if receive_all_updates else "turned off"


def _format_watchlist_csv(company_names: list[str]) -> str:
    """
    Joins the user's watchlist company names with commas. Falls back to a
    polite placeholder when the user has not added any stocks yet (Gupshup
    template variables cannot be empty).
    """
    cleaned = [str(n).strip() for n in (company_names or []) if n and str(n).strip()]
    if not cleaned:
        return "no stocks added yet"
    return ", ".join(cleaned)


def _fetch_user_training_recipients() -> list[dict]:
    """
    Returns the list of users who should receive the 'user_training' broadcast.

    Each row: {phone, receive_all_updates, watchlist_companies (list[str])}.
    Only active users with a phone number are included.
    """
    recipients: list[dict] = []
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT u.id,
                           u.phone,
                           COALESCE(u.receive_all_updates, FALSE) AS receive_all_updates,
                           COALESCE(
                               ARRAY_AGG(c.company_name ORDER BY c.company_name)
                                   FILTER (WHERE c.company_name IS NOT NULL),
                               ARRAY[]::TEXT[]
                           ) AS watchlist_companies
                    FROM users u
                    LEFT JOIN user_watchlist w ON w.user_id = u.id
                    LEFT JOIN company_master c ON c.bse_scrip_code = w.bse_scrip_code
                    WHERE u.is_active = TRUE
                      AND u.phone IS NOT NULL
                      AND TRIM(u.phone) <> ''
                    GROUP BY u.id, u.phone, u.receive_all_updates
                    """
                )
                for row in cur.fetchall():
                    recipients.append(
                        {
                            "user_id": row[0],
                            "phone": row[1],
                            "receive_all_updates": bool(row[2]),
                            "watchlist_companies": list(row[3] or []),
                        }
                    )
    except Exception as e:
        logger.error("  ❌ Failed to load user_training recipients: %s", e)
    return recipients


def _run_user_training_broadcast() -> dict:
    """
    Synchronously sends the 'user_training' utility template to every
    eligible user. Designed to be invoked from a worker thread (via
    asyncio.to_thread) so the asyncio event loop is never blocked.

    The actual fan-out happens in WhatsAppService's dedicated thread
    pool (WHATSAPP_PARALLELISM concurrent Gupshup calls), so a 5,000-user
    blast typically completes in well under a minute instead of hours.
    """
    recipients = _fetch_user_training_recipients()
    if not recipients:
        logger.info("📚 user_training broadcast: no eligible recipients")
        return {"sent": 0, "failed": 0, "total": 0}

    svc = WhatsAppService()
    payload = [
        {
            "phone": r["phone"],
            "name_label": "user",
            "watchlist_csv": _format_watchlist_csv(r["watchlist_companies"]),
            "high_impact_label": _format_high_impact_label(r["receive_all_updates"]),
        }
        for r in recipients
    ]

    logger.info("📚 user_training broadcast: sending to %d users (parallel)", len(payload))
    result = svc.send_user_training_messages(payload)
    logger.info(
        "📚 user_training broadcast complete: %d sent, %d failed (total %d)",
        result.get("sent", 0), result.get("failed", 0), result.get("total", 0),
    )
    return result


def _claim_scheduled_job(job_name: str, interval_days: int) -> bool:
    """
    Atomically claims a periodic job slot. Returns True iff the caller now
    owns the run (i.e. enough days have elapsed since last_run_at). The
    timestamp is updated *before* the job runs so concurrent scheduler
    iterations never double-fire.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO scheduled_jobs (job_name, last_run_at, last_status, updated_at)
                    VALUES (%s, NOW(), 'running', NOW())
                    ON CONFLICT (job_name) DO UPDATE
                        SET last_run_at = NOW(),
                            last_status = 'running',
                            updated_at = NOW()
                        WHERE scheduled_jobs.last_run_at IS NULL
                           OR scheduled_jobs.last_run_at <= NOW() - (%s || ' days')::INTERVAL
                    RETURNING last_run_at
                    """,
                    (job_name, str(interval_days)),
                )
                row = cur.fetchone()
                return row is not None
    except Exception as e:
        logger.error("  ❌ _claim_scheduled_job(%s) failed: %s", job_name, e)
        return False


def _record_scheduled_job_result(job_name: str, status: str, meta: dict | None = None) -> None:
    """Records the outcome (success/failure + counts) of a periodic job run."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE scheduled_jobs
                    SET last_status = %s,
                        last_meta = %s,
                        updated_at = NOW()
                    WHERE job_name = %s
                    """,
                    (status, Json(meta or {}), job_name),
                )
    except Exception as e:
        logger.warning("  ⚠️ Failed to record result for job %s: %s", job_name, e)


async def _maybe_run_user_training_broadcast() -> None:
    """
    Checks if the periodic user_training broadcast is due and, if so,
    runs it in a worker thread (so we don't block the asyncio loop).
    """
    if not _claim_scheduled_job(USER_TRAINING_JOB_NAME, USER_TRAINING_INTERVAL_DAYS):
        return
    logger.info(
        "📚 user_training broadcast is due (every %d days) — starting…",
        USER_TRAINING_INTERVAL_DAYS,
    )
    try:
        result = await asyncio.to_thread(_run_user_training_broadcast)
        _record_scheduled_job_result(USER_TRAINING_JOB_NAME, "success", result)
    except Exception as e:
        logger.error("❌ user_training broadcast failed: %s", e)
        _record_scheduled_job_result(USER_TRAINING_JOB_NAME, "failed", {"error": str(e)})


def _make_json_serializable(obj):
    """
    Recursively convert pandas Timestamp and datetime objects to ISO format strings
    for JSON serialization.
    """
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: _make_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_make_json_serializable(item) for item in obj]
    else:
        return obj


def _is_financial_results_category(category: str) -> bool:
    """True for BSE category labels like 'Financial Results/Announcement'."""
    c = (category or "").upper()
    return "FINANCIAL" in c


def _is_strongly_positive_broadcast_impact(impact: str) -> bool:
    """WhatsApp broadcast: strongly positive only (not plain POSITIVE / NEUTRAL)."""
    imp = (impact or "").strip().upper()
    return "STRONGLY POSITIVE" in imp or imp == "BEAT"


def _should_include_in_whatsapp_broadcast(enriched_item: dict, company_service: CompanyService) -> Tuple[bool, Optional[float]]:
    """
    Determines if an item should be included in whatsapp_broadcast table.

    Financial Results category (anything with 'Financial' in the label):
        Only STRONGLY POSITIVE or BEAT. Excludes POSITIVE, NEUTRAL, NEGATIVE, STRONGLY NEGATIVE,
        MATCHED, MISSED, N/A, etc.

    All other categories:
        STRONGLY POSITIVE or BEAT → include.
        STRONGLY NEGATIVE → include only if market cap > 10,000 Cr.
        Regular NEGATIVE (not STRONGLY), MISSED → excluded.

    Note: Pipeline already filters to companies >2,500 Cr.
    """
    impact_raw = enriched_item.get("impact") or ""
    impact = impact_raw.strip().upper()
    category = enriched_item.get("category") or ""
    scrip_cd = enriched_item.get("scrip_cd")

    mkt_cap = enriched_item.get("mkt_cap_cr")
    if mkt_cap is None and scrip_cd:
        try:
            scrip_int = int(scrip_cd)
            caps = company_service.get_market_caps([scrip_int])
            mkt_cap = caps.get(scrip_int)
        except (ValueError, TypeError):
            pass

    # Financial results: WhatsApp broadcast only for strongly positive / beat — never other impacts
    # (even if category were mis-tagged, applying this first avoids plain POSITIVE slipping through
    # via any future rule change; mis-tagged financials also must not use the STRONGLY NEGATIVE path.)
    if _is_financial_results_category(category):
        return (_is_strongly_positive_broadcast_impact(impact_raw), mkt_cap)

    if _is_strongly_positive_broadcast_impact(impact_raw):
        return (True, mkt_cap)

    if "STRONGLY NEGATIVE" in impact:
        if mkt_cap and mkt_cap > 10000:
            return (True, mkt_cap)
        return (False, None)

    if "NEGATIVE" in impact and "STRONGLY NEGATIVE" not in impact:
        return (False, None)

    if impact == "MISSED":
        return (False, None)

    return (False, None)


def _normalize_category_broadcast_key(category: Optional[str]) -> str:
    """Stable key for dedupe: trim + uppercase."""
    return (category or "").strip().upper()


def _whatsapp_broadcast_has_scrip_and_category(scrip_cd, category: Optional[str]) -> bool:
    """
    True if a whatsapp_broadcast row already exists for this scrip and category.
    Old rows are removed by cleanup (~48h), so a new filing can broadcast again after that.
    """
    if scrip_cd is None:
        return False
    scrip = str(scrip_cd).strip()
    if not scrip:
        return False
    norm_cat = _normalize_category_broadcast_key(category)
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1 FROM whatsapp_broadcast
                    WHERE scrip_cd = %s
                      AND UPPER(TRIM(COALESCE(category, ''))) = %s
                    LIMIT 1
                    """,
                    (scrip, norm_cat),
                )
                return cur.fetchone() is not None
    except Exception as e:
        logger.warning("whatsapp_broadcast scrip+category duplicate check failed: %s", e)
        return False


def _cleanup_old_records():
    """Delete records older than 48 hours from all tables to keep the DB lean."""
    cutoff = _now_ist_naive() - timedelta(hours=48)
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM raw_bse_announcements WHERE news_submission_dt < %s", (cutoff,))
                raw_del = cur.rowcount
                cur.execute("DELETE FROM predictions WHERE news_submission_dt < %s", (cutoff,))
                pred_del = cur.rowcount
                cur.execute("DELETE FROM ui_data WHERE news_time < %s", (cutoff,))
                ui_del = cur.rowcount
                cur.execute("DELETE FROM watchlist_notifications WHERE created_at < %s", (cutoff,))
                wl_del = cur.rowcount
                cur.execute("DELETE FROM whatsapp_broadcast WHERE created_at < %s", (cutoff,))
                wb_del = cur.rowcount
                cur.execute("DELETE FROM otp_requests WHERE expires_at < NOW() - INTERVAL '1 hour'")
                otp_del = cur.rowcount
        if raw_del or pred_del or ui_del or wl_del or wb_del or otp_del:
            logger.info(
                "Cleanup: deleted %s raw, %s predictions, %s ui_data, %s watchlist_notifs, %s whatsapp_broadcast, %s expired OTPs.",
                raw_del, pred_del, ui_del, wl_del, wb_del, otp_del,
            )
    except Exception as e:
        logger.warning("Cleanup failed: %s", e)


# =========================================================================
# Background scheduler — runs the pipeline every N minutes
# =========================================================================
_run_counter = 0

async def _scheduled_analysis_loop():
    """
    Runs the full analysis pipeline at a fixed interval.
    - Fetches ALL of today's announcements from BSE
    - Only NEW ones (not already in DB) get processed (force=False)
    - No time filter — any new announcement for today gets analyzed
    - Skips if a manual run is already in progress
    """
    global _run_counter
    interval = SCHEDULER_INTERVAL_MIN * 60
    logger.info("Scheduler started: will run every %s minutes.", SCHEDULER_INTERVAL_MIN)

    # Wait one interval before the first run (let the app warm up)
    await asyncio.sleep(interval)

    while True:
        _run_counter += 1
        now_ist = _now_ist_naive()
        logger.info("=" * 60)
        logger.info("🔄 SCHEDULED RUN #%s STARTED  —  %s IST",
                     _run_counter, now_ist.strftime("%Y-%m-%d %H:%M:%S"))
        logger.info("=" * 60)

        if analysis_lock.locked():
            logger.info("⏭️  SKIPPED — previous run still in progress.")
        else:
            try:
                result = await run_analysis_in_background(
                    target_date=None,       # today
                    market_cap_start=2500,
                    market_cap_end=999999,
                    hours=0,                # full day (no time filter)
                    force=False,            # only NEW announcements get processed
                )
                # ---- Clean summary box ----
                logger.info("-" * 60)
                logger.info("✅ RUN #%s COMPLETED — Summary:", _run_counter)
                logger.info("   BSE announcements today:  %s", result.get("bse_total", "N/A"))
                logger.info("   New announcements stored:  %s", result.get("new_stored", 0))
                logger.info("   Sent for analysis:         %s", result.get("filtered", 0))
                logger.info("   Predictions created:       %s", result.get("inserted_predictions", 0))
                logger.info("   UI data records stored:    %s", result.get("ui_data_stored", 0))
                logger.info("   Status: %s", result.get("message", ""))
                logger.info("-" * 60)
            except Exception as e:
                logger.error("❌ RUN #%s FAILED: %s", _run_counter, e)
        
        # Send any pending notifications that were missed
        try:
            await _send_pending_broadcasts()
        except Exception as e:
            logger.error("❌ Failed to send pending broadcasts: %s", e)

        # Periodic 'user_training' utility broadcast (every USER_TRAINING_INTERVAL_DAYS days)
        try:
            await _maybe_run_user_training_broadcast()
        except Exception as e:
            logger.error("❌ user_training periodic broadcast errored: %s", e)

        await asyncio.sleep(interval)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler_task
    connect_to_db()

    # Start the background scheduler
    if SCHEDULER_ENABLED:
        _scheduler_task = asyncio.create_task(_scheduled_analysis_loop())
    else:
        logger.info("Scheduler is DISABLED (SCHEDULER_ENABLED=false).")

    # Kick the periodic 'user_training' broadcast off immediately on boot so
    # a fresh deploy doesn't have to wait for the next scheduler tick. The
    # 15-day claim in scheduled_jobs still guarantees it never double-fires
    # across restarts.
    async def _startup_user_training_kick():
        # Tiny delay so the DB pool / app is fully ready before we fan out.
        await asyncio.sleep(5)
        try:
            await _maybe_run_user_training_broadcast()
        except Exception as e:
            logger.error("❌ Startup user_training kick failed: %s", e)

    asyncio.create_task(_startup_user_training_kick())

    yield

    # Shutdown: stop accepting new WhatsApp work, then cancel scheduler + close DB
    try:
        from service.whatsapp_service import shutdown_whatsapp_executor
        shutdown_whatsapp_executor(wait=False)
    except Exception as e:
        logger.warning("WhatsApp executor shutdown failed: %s", e)

    if _scheduler_task:
        _scheduler_task.cancel()
        try:
            await _scheduler_task
        except asyncio.CancelledError:
            pass
    close_db_connection()

app = FastAPI(
    title="BSE Announcements Analyzer API",
    description="Triggers a pipeline to fetch, filter, download, and analyze BSE announcements.",
    lifespan=lifespan,
)

# Allow frontend (Netlify) to call the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],            # Allow all origins (or set your Netlify URL)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================================================================
# POST /analyze-announcements/
# =========================================================================
@app.post("/analyze-announcements/", summary="Run the full analysis pipeline")
async def run_analysis_pipeline(
    background_tasks: BackgroundTasks,
    date: str | None = Query(
        None,
        description="Target date YYYY-MM-DD.",
        pattern=r"^\d{4}-\d{2}-\d{2}$",
    ),
    start_time: str | None = Query(
        None,
        description="Start of time window HH:MM (IST). Use with 'date'. E.g. 15:00",
        pattern=r"^\d{2}:\d{2}(:\d{2})?$",
    ),
    end_time: str | None = Query(
        None,
        description="End of time window HH:MM (IST). Use with 'date'. E.g. 17:00",
        pattern=r"^\d{2}:\d{2}(:\d{2})?$",
    ),
    market_cap_st: int = Query(2500, description="Start of market cap range (Crores)."),
    market_cap_end: int = Query(999999, description="End of market cap range (Crores)."),
    hours: int = Query(0, description="Lookback window in hours from now. 0 = full day."),
    force: bool = Query(True, description="Reprocess announcements already in DB."),
    run_now: bool = Query(False, description="Run synchronously and return counts."),
):
    target_date = None
    if date:
        try:
            target_date = datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
    

    # Parse optional start_time / end_time into time objects
    parsed_start_time = None
    parsed_end_time = None
    if start_time:
        try:
            fmt = "%H:%M:%S" if start_time.count(":") == 2 else "%H:%M"
            parsed_start_time = datetime.strptime(start_time, fmt).time()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid start_time. Use HH:MM or HH:MM:SS.")
    if end_time:
        try:
            fmt = "%H:%M:%S" if end_time.count(":") == 2 else "%H:%M"
            parsed_end_time = datetime.strptime(end_time, fmt).time()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid end_time. Use HH:MM or HH:MM:SS.")

    if analysis_lock.locked():
        raise HTTPException(status_code=409, detail="Analysis already running. Try again later.")

    if run_now:
        result = await run_analysis_in_background(
            target_date, market_cap_st, market_cap_end, hours, force,
            parsed_start_time, parsed_end_time,
        )
        return result

    background_tasks.add_task(
        run_analysis_in_background,
        target_date, market_cap_st, market_cap_end, hours, force,
        parsed_start_time, parsed_end_time,
    )
    return {"message": "Analysis pipeline started in the background."}


async def run_analysis_in_background(
    target_date: datetime | None,
    market_cap_start: int,
    market_cap_end: int,
    hours: int,
    force: bool,
    start_time_obj=None,   # datetime.time or None
    end_time_obj=None,     # datetime.time or None
):
    """
    Async wrapper: acquires the lock, then runs the HEAVY synchronous
    pipeline in a separate thread so the event loop stays responsive
    (health checks, scheduler, other requests keep working).
    """
    async with analysis_lock:
        result = await asyncio.to_thread(
            _pipeline_sync,
            target_date, market_cap_start, market_cap_end,
            hours, force, start_time_obj, end_time_obj,
        )
        return result


def _pipeline_sync(
    target_date: datetime | None,
    market_cap_start: int,
    market_cap_end: int,
    hours: int,
    force: bool,
    start_time_obj=None,
    end_time_obj=None,
):
    """
    The SYNCHRONOUS pipeline that does all heavy I/O (BSE fetch, PDF
    download, OpenAI calls, DB writes).  Runs in a thread via
    asyncio.to_thread() so it never blocks the event loop.
    """
    now_ist = _now_ist_naive()

    # ----- Determine mode -----
    if target_date and start_time_obj and end_time_obj:
        mode = f"time-range ({start_time_obj.strftime('%H:%M')}–{end_time_obj.strftime('%H:%M')})"
        the_date = target_date
        start_dt = datetime.combine(target_date.date(), start_time_obj)
        end_dt = datetime.combine(target_date.date(), end_time_obj)
    elif target_date and hours > 0:
        mode = f"date+window ({hours}h)"
        the_date = target_date
        end_dt = target_date.replace(hour=23, minute=59, second=59)
        start_dt = end_dt - timedelta(hours=hours)
    elif target_date:
        mode = "full-day"
        the_date = target_date
        start_dt = None
        end_dt = None
    elif hours > 0:
        mode = f"incremental ({hours}h)"
        the_date = now_ist
        end_dt = now_ist
        start_dt = end_dt - timedelta(hours=hours)
    else:
        mode = "full-day (today)"
        the_date = now_ist
        start_dt = None
        end_dt = None

    logger.info("  Pipeline: mode=%s | date=%s | force=%s",
                 mode, the_date.strftime("%Y-%m-%d"), force)
    if start_dt and end_dt:
        logger.info("  Time window: %s → %s IST", start_dt, end_dt)

    summary = {
        "mode": mode,
        "date": the_date.strftime("%Y-%m-%d"),
        "bse_total": 0,
        "new_stored": 0,
        "filtered": 0,
        "analyzed": 0,
        "inserted_predictions": 0,
        "ui_data_stored": 0,
        "message": "",
    }

    announcement_service = AnnouncementService()
    ui_service = UIDataService()
    company_service = CompanyService()

    # --- Step 1: Fetch & filter announcements ---
    filtered_df, fetch_stats = fetch_and_filter_announcements(
        target_date=the_date,
            market_cap_start=market_cap_start,
            market_cap_end=market_cap_end,
        start_datetime=start_dt,
        end_datetime=end_dt,
        force_reprocess=force,
        )
    summary["bse_total"] = fetch_stats.get("bse_total", 0)
    summary["new_stored"] = fetch_stats.get("new_stored", 0)
    summary["filtered"] = len(filtered_df)
    if filtered_df.empty:
        summary["message"] = "No new announcements to process."
        return summary

    # --- Step 2: Process each PDF ONE BY ONE ---
    # After each PDF: mark as analyzed + save prediction + save ui_data
    # This way if the service crashes, completed PDFs are NOT retried.
    total = len(filtered_df)
    total_predictions = 0
    total_ui = 0
    items_to_notify = []           # WhatsApp: STRONGLY POSITIVE/BEAT (all); non-financial STRONGLY NEGATIVE >10K Cr; financial = strongly positive only
    watchlist_only_items = []      # Low-impact items → notify only watchlist users

    logger.info("  [Analyse] Processing %s PDFs one-by-one ...", total)

    for i, (_, row) in enumerate(filtered_df.iterrows(), 1):
        row_dict = row.to_dict()
        newsid = str(row_dict.get("NEWSID", ""))

        # 2a) Process this single PDF (download → extract → OpenAI)
        result = _process_single_announcement(row_dict, i, total)

        # 2b) IMMEDIATELY mark as analyzed (even if N/A or no prediction)
        if newsid:
            try:
                announcement_service.mark_as_analyzed([newsid])
            except Exception as e:
                logger.warning("  Failed to mark %s as analyzed: %s", newsid, e)

        # If no directional prediction, move on
        if not result:
            continue

        # 2b-ii) Update NSE symbol in company_master if extracted from PDF
        nse_sym = result.get("NSE_Symbol")
        if nse_sym:
            try:
                scrip_int = int(row_dict.get("SCRIP_CD", 0))
                if scrip_int:
                    company_service.update_nse_symbol(scrip_int, nse_sym)
            except (ValueError, TypeError):
                pass

        # 2c) Store this single prediction in DB
        pred_df = pd.DataFrame([result])
        # Add derived metrics
        pred_df["Impact_Score"] = pred_df["Impact"].apply(
            lambda t: _impact_map.get(t.upper(), 0)
        )

        _impact_upper = str(pred_df.iloc[0]["Impact"]).strip().upper()
        is_low_impact = (
            _impact_upper in ("NEUTRAL", "MATCHED", "N/A")
            or "NEUTRAL" in _impact_upper   # catches "LIKELY NEUTRAL", "MOSTLY NEUTRAL", etc.
            or "MATCHED" in _impact_upper
            or "N/A" in _impact_upper
            or "IMMATERIAL" in _impact_upper
        )

        if not is_low_impact:
            # === IMPACTFUL: full enrichment via Indian API + store in ui_data ===
            pred_df["Mid_%"] = 0.0
            pred_df["Rank"] = 0
            pred_df["SCRIP_CD"] = pred_df["SCRIP_CD"].astype(str)

            try:
                inserted = announcement_service.create_predictions(
                    pred_df, "predictions", force=force
                )
                if inserted:
                    total_predictions += len(inserted)
                    logger.info("  [%s/%s] ✅ Prediction saved for SCRIP %s.", i, total, row_dict.get("SCRIP_CD"))
            except Exception as e:
                logger.warning("  [%s/%s] Failed to save prediction: %s", i, total, e)

            # Enrich with Indian API data & store in ui_data (shown on website)
            # All impacts (including STRONGLY NEGATIVE) go to ui_data
            try:
                enriched = enrich_prediction(result)
                ui_count = ui_service.bulk_store_enriched([enriched])
                if ui_count:
                    total_ui += ui_count
                    logger.info("  [%s/%s] ✅ UI data saved for SCRIP %s.", i, total, row_dict.get("SCRIP_CD"))
                
                # Check if this should go to whatsapp_broadcast (stricter filtering)
                should_broadcast, mkt_cap_cr = _should_include_in_whatsapp_broadcast(enriched, company_service)
                if should_broadcast:
                    if _whatsapp_broadcast_has_scrip_and_category(
                        enriched.get("scrip_cd"), enriched.get("category")
                    ):
                        logger.info(
                            "  [%s/%s] ⏭️ WhatsApp broadcast skipped (already have scrip+category in window): SCRIP %s | %s",
                            i,
                            total,
                            row_dict.get("SCRIP_CD"),
                            enriched.get("category") or "",
                        )
                    else:
                        try:
                            with get_conn() as conn:
                                with conn.cursor() as cur:
                                    # Convert news_time to IST before storing
                                    news_time_ist = _to_ist(enriched.get("news_time")) if enriched.get("news_time") else None
                                    current_time_ist = _now_ist_naive().replace(tzinfo=IST)  # Current time in IST

                                    cur.execute(
                                        """
                                        INSERT INTO whatsapp_broadcast
                                            (scrip_cd, company_name, impact, category, summary, pdf_link, news_time, mkt_cap_cr, data, created_at)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                        """,
                                        (
                                            enriched.get("scrip_cd"),
                                            enriched.get("company_name", "Unknown"),
                                            enriched.get("impact"),
                                            enriched.get("category"),
                                            enriched.get("summary"),
                                            enriched.get("pdf_link"),
                                            news_time_ist,
                                            mkt_cap_cr,
                                            Json(_make_json_serializable(enriched)),  # Store full enriched data as JSONB (convert Timestamps to strings)
                                            current_time_ist,  # Explicitly set created_at in IST
                                        ),
                                    )
                            items_to_notify.append(enriched)  # Add to notification queue
                            logger.info("  [%s/%s] ✅ WhatsApp broadcast entry saved for SCRIP %s (mkt_cap=%.0f Cr).",
                                        i, total, row_dict.get("SCRIP_CD"), mkt_cap_cr or 0)
                        except Exception as e:
                            logger.warning("  [%s/%s] Failed to save whatsapp_broadcast entry: %s", i, total, e)
                else:
                    logger.info("  [%s/%s] ⏭️ SCRIP %s excluded from WhatsApp broadcast (doesn't meet criteria).",
                                i, total, row_dict.get("SCRIP_CD"))
            except Exception as e:
                logger.warning("  [%s/%s] Enrichment/UI save failed: %s", i, total, e)
        else:
            # === LOW-IMPACT: NO Indian API, store lightweight, notify watchlist only ===
            logger.info("  [%s/%s] %s: low-impact (%s) — storing for watchlist notification only.",
                         i, total, newsid, pred_df.iloc[0]["Impact"])

            lightweight_item = {
                "scrip_cd": str(result.get("SCRIP_CD", "")),
                "company_name": result.get("Company", "Unknown"),
                "impact": result.get("Impact", "N/A"),
                "category": result.get("Category", "General"),
                "summary": result.get("Summary", ""),
                "pdf_link": result.get("PDF_Link", ""),
                "news_time": result.get("News_submission_dt"),
            }

            # Store in watchlist_notifications table (cleaned up after 48h)
            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO watchlist_notifications
                                (scrip_cd, company_name, impact, category, summary, pdf_link, news_time)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                lightweight_item["scrip_cd"],
                                lightweight_item["company_name"],
                                lightweight_item["impact"],
                                lightweight_item["category"],
                                lightweight_item["summary"],
                                lightweight_item["pdf_link"],
                                lightweight_item["news_time"],
                            ),
                        )
                logger.info("  [%s/%s] ✅ Watchlist notification stored for SCRIP %s.",
                            i, total, row_dict.get("SCRIP_CD"))
            except Exception as e:
                logger.warning("  [%s/%s] Failed to store watchlist notification: %s", i, total, e)

            watchlist_only_items.append(lightweight_item)

    # --- Step 3: Send WhatsApp notifications for new items ---
    total_notified = 0
    notif_service = NotificationService()

    # 3a) WhatsApp broadcast items → notify watchlist users + receive_all_updates users
    # items_to_notify: passed whatsapp_broadcast rules (see _should_include_in_whatsapp_broadcast)
    if items_to_notify:
        try:
            notif_result = notif_service.notify_all_users_bulk(items_to_notify)
            total_notified += notif_result["total_sent"]
            logger.info(
                "  📢 Notifications (impactful): %d sent, %d failed for %d items.",
                notif_result["total_sent"],
                notif_result["total_failed"],
                notif_result["items_processed"],
            )
            
            # Mark entries as sent in whatsapp_broadcast table
            if notif_result["total_sent"] > 0:
                try:
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            for item in items_to_notify:
                                scrip_cd = item.get("scrip_cd")
                                news_time = item.get("news_time")
                                if scrip_cd and news_time:
                                    # Convert news_time to IST for comparison (in case it's not already)
                                    news_time_ist = _to_ist(news_time) if news_time else None
                                    # Set sent_at in IST
                                    sent_at_ist = _now_ist_naive().replace(tzinfo=IST)
                                    cur.execute(
                                        """
                                        UPDATE whatsapp_broadcast
                                        SET sent_at = %s
                                        WHERE scrip_cd = %s AND news_time = %s AND sent_at IS NULL
                                        """,
                                        (sent_at_ist, scrip_cd, news_time_ist),
                                    )
                    logger.info("  ✅ Marked %d entries as sent in whatsapp_broadcast", len(items_to_notify))
                except Exception as e:
                    logger.warning("  ⚠️ Failed to mark entries as sent: %s", e)
        except Exception as e:
            logger.warning("  ⚠️ Notification sending failed (impactful): %s", e)

    # 3b) Low-impact items → notify ONLY users who have the stock in their watchlist
    if watchlist_only_items:
        try:
            watchlist_result = notif_service.notify_watchlist_only_bulk(watchlist_only_items)
            total_notified += watchlist_result["total_sent"]
            logger.info(
                "  📢 Notifications (watchlist-only): %d sent, %d failed for %d items.",
                watchlist_result["total_sent"],
                watchlist_result["total_failed"],
                watchlist_result["items_processed"],
            )
        except Exception as e:
            logger.warning("  ⚠️ Notification sending failed (watchlist-only): %s", e)

    # --- Step 4: Summary ---
    summary["analyzed"] = total
    summary["inserted_predictions"] = total_predictions
    summary["ui_data_stored"] = total_ui
    summary["notifications_sent"] = total_notified

    # --- Step 5: Cleanup records older than 48 hours ---
    _cleanup_old_records()

    summary["message"] = "Done."

    return summary


# =========================================================================
# GET /predictions/{date}
# =========================================================================
@app.get("/predictions/{date}", summary="Fetch predictions by date")
def get_predictions(
    date: str = Path(..., description="Target date in YYYY-MM-DD format.", pattern=r"^\d{4}-\d{2}-\d{2}$"),
):
    try:
        target_date = datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

    service = AnnouncementService()
    try:
        predictions_df = service.get_predictions_by_date(target_date)
    except ConnectionError as e:
        raise HTTPException(status_code=500, detail=str(e))

    if predictions_df.empty:
        return {"message": f"No predictions found for {date}."}

    predictions_df = predictions_df.where(pd.notna(predictions_df), None)
    return predictions_df.to_dict("records")


# =========================================================================
# UI data endpoints
# =========================================================================
@app.post("/ui-data/", summary="Store UI Data Document")
def store_ui_data(data_item: UIDataItem):
    ui_service = UIDataService()
    data_item_as_dict_list = [data_item.model_dump()]
    try:
        results = ui_service.create_ui_data_document(data_item_as_dict_list)
    except ConnectionError as e:
        raise HTTPException(status_code=500, detail=str(e))

    if not results or results[0].get("errors"):
        error_details = results[0]["errors"] if results else "Unknown error."
        raise HTTPException(status_code=422, detail=error_details)

    return {"message": "UI data stored successfully", "inserted_id": results[0]["inserted_id"]}


@app.get("/ui-data/today", summary="Fetch latest UI data for the current date")
def get_todays_ui_data():
    target_date = _now_ist_naive()
    ui_service = UIDataService()
    try:
        latest_data = ui_service.get_latest_ui_data(target_date=target_date)
    except ConnectionError as e:
        raise HTTPException(status_code=500, detail=str(e))

    if not latest_data:
        raise HTTPException(status_code=404, detail=f"No UI data found for today ({target_date.strftime('%Y-%m-%d')}).")
    return latest_data


# =========================================================================
# Misc endpoints
# =========================================================================
@app.get("/announcements/latest", summary="Fetch the latest raw announcement")
def get_latest_announcement():
    service = AnnouncementService()
    try:
        latest = service.get_latest_announcements()
    except ConnectionError as e:
        raise HTTPException(status_code=500, detail=str(e))
    if not latest:
        return {"message": "No announcements found."}
    return latest


@app.get("/ping", summary="Health check")
def ping():
    return {"message": "pong"}


# =========================================================================
# Company master management
# =========================================================================
@app.post("/load-company-master", summary="Load company master data from Excel file")
def load_company_master(
    file_path: str = Query(
        "./assets/LIST_OF_comapnies_BSE_NSE_with_mcap.xlsx",
        description="Path to Excel file with company data.",
    ),
):
    """
    One-time (or periodic) endpoint to bulk-load / refresh the company_master
    table from an Excel file.  The file must have columns:
    ISIN, Company Name, BSE_Scrip_Code, NSE_Symbol, MktCapFull
    """
    try:
        df = pd.read_excel(file_path)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File not found: {file_path}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read Excel: {e}")

    required = {"BSE_Scrip_Code", "Company Name", "MktCapFull"}
    if not required.issubset(set(df.columns)):
        raise HTTPException(
            status_code=400,
            detail=f"Excel must have columns: {required}. Found: {list(df.columns)}"
        )

    company_service = CompanyService()
    count = company_service.bulk_load_from_dataframe(df)
    total = company_service.get_count()
    return {
        "message": f"Loaded {count} companies. Total in DB: {total}.",
        "loaded": count,
        "total_in_db": total,
    }


@app.get("/company-master/count", summary="Get company master count")
def company_master_count():
    company_service = CompanyService()
    return {"total": company_service.get_count()}


@app.get(
    "/api/admin/industry-stats",
    summary="[ADMIN] Industry distribution in company_master",
)
def admin_industry_stats():
    """Returns total rows, uncategorized count, and a per-industry breakdown."""
    company_service = CompanyService()
    total = company_service.get_count()
    uncategorized = company_service.count_uncategorized()
    breakdown = company_service.count_by_industry()
    return {
        "total": total,
        "categorized": total - uncategorized,
        "uncategorized": uncategorized,
        "by_industry": breakdown,
    }


@app.get(
    "/api/admin/uncategorized",
    summary="[ADMIN] List all rows in company_master with industry IS NULL",
)
def admin_list_uncategorized(
    limit: int = Query(500, ge=1, le=10000),
):
    """Return the bse_scrip_code, nse_symbol, and company_name for every
    uncategorized row, capped at `limit`. Used to manually classify the
    long-tail residue the LLM cascade can't resolve."""
    company_service = CompanyService()
    rows = company_service.fetch_uncategorized_batch(limit)
    return {
        "count": len(rows),
        "rows": [
            {
                "bse_scrip_code": int(r["bse_scrip_code"]),
                "nse_symbol": r.get("nse_symbol"),
                "company_name": r.get("company_name"),
            }
            for r in rows
        ],
    }


@app.post(
    "/api/admin/set-industries",
    summary="[ADMIN] Bulk-apply manual industry classifications",
)
def admin_set_industries(
    payload: Dict[str, str] = Body(
        ...,
        description=(
            "Mapping of {bse_scrip_code: industry_label}. Each label must be "
            "one of the 24 canonical names. Source is recorded as 'manual'."
        ),
        example={"500001": "Energy", "500002": "Industrial Manufacturing & Engineering"},
    ),
):
    """Idempotent: overwrites whatever was there. Returns per-row status."""
    from service.industry_classifier import CANON_NAMES

    company_service = CompanyService()
    canon_set = set(CANON_NAMES)
    applied: list[dict] = []
    rejected: list[dict] = []
    for scrip_str, label in payload.items():
        try:
            scrip = int(scrip_str)
        except (TypeError, ValueError):
            rejected.append({"bse_scrip_code": scrip_str, "reason": "non-integer scrip"})
            continue
        if label not in canon_set:
            rejected.append({"bse_scrip_code": scrip, "reason": f"invalid label: {label!r}"})
            continue
        try:
            company_service.update_industry(scrip, label, "manual")
            applied.append({"bse_scrip_code": scrip, "industry": label})
        except Exception as e:  # noqa: BLE001
            rejected.append({"bse_scrip_code": scrip, "reason": f"{type(e).__name__}: {e}"})

    return {
        "applied_count": len(applied),
        "rejected_count": len(rejected),
        "applied": applied,
        "rejected": rejected,
        "remaining_uncategorized": company_service.count_uncategorized(),
    }


@app.post(
    "/api/admin/backfill-industries",
    summary="[ADMIN] Classify all uncategorized rows in company_master",
)
def admin_backfill_industries(
    limit: int = Query(
        500,
        ge=1,
        le=5000,
        description="Max rows to process in this call (run again to continue).",
    ),
    use_llm: bool = Query(
        False,
        description="Use gpt-4.1-nano fallback for rows the deterministic cascade can't classify.",
    ),
    fetch_api_industry: bool = Query(
        True,
        description=(
            "Call Indian API /stock?name=<nse_symbol or company_name> for each row "
            "to enrich with the api_industry field. Costs API quota."
        ),
    ),
    parallelism: int = Query(
        16,
        ge=1,
        le=32,
        description="Concurrent Indian API workers (only used if fetch_api_industry=true).",
    ),
    debug: bool = Query(
        False,
        description="Include per-row diagnostics (query sent + raw Indian API result) in response.",
    ),
):
    """
    Backfill the `industry` column in `company_master` for rows where it is
    NULL. Idempotent: safe to call repeatedly. Already-classified rows are
    never touched.

    Cascade per row:
      1. Indian API /stock → companyProfile.industry → 24-label map
      2. Keyword cascade on company_name
      3. (optional) gpt-4.1-nano LLM fallback
    """
    from concurrent.futures import ThreadPoolExecutor
    from service.industry_classifier import (
        CANON_NAMES,
        classify as classify_industry,
        classify_with_llm,
    )
    # Lazy local import to avoid a hard dependency on stock_enrichment when
    # this endpoint isn't called.
    from stock_enrichment import _fetch_stock_info, _clean_stock_name

    company_service = CompanyService()
    rows = company_service.fetch_uncategorized_batch(limit)
    if not rows:
        return {
            "message": "Nothing to do — every row already has an industry.",
            "processed": 0,
            "classified_indian_api": 0,
            "classified_keyword": 0,
            "classified_llm": 0,
            "still_unknown": 0,
            "remaining_uncategorized": 0,
        }

    n_api = 0
    n_keyword = 0
    n_unknown = 0
    unresolved: list[dict] = []
    api_calls = 0

    # ----- Parallel Indian API fan-out --------------------------------
    # Calling /stock 200x sequentially burns 5+ minutes; with 16 workers
    # the same batch finishes in ~15s. Indian API tolerates the burst.
    api_industry_by_scrip: dict[int, Optional[str]] = {}
    debug_rows: list[dict] = []
    if fetch_api_industry:
        def _query_one(row: dict) -> Tuple[int, Optional[str], str, str]:
            scrip = int(row["bse_scrip_code"])
            name = (row.get("company_name") or "").strip()
            nse = (row.get("nse_symbol") or "").strip()
            # Indian API quirks observed against company_master:
            #   - "LARSEN & TOUBRO LTD." (uppercase + ampersand) → 15s timeout
            #     "Larsen & Toubro" (title-case) → fast 200 with industry.
            #   - "Zomato Limited" → not found (renamed to Eternal).
            #   - "MOTHERSON SUMI SYSTEMS LTD." → not found (renamed).
            # So: prefer NSE symbol, otherwise title-case + strip suffixes.
            if nse:
                query = nse
            else:
                query = _clean_stock_name(name).title()
            if not query:
                return scrip, None, query, "empty-query"
            try:
                info = _fetch_stock_info(query)
                if info is None:
                    return scrip, None, query, "fetch-returned-none"
                if info.get("api_industry"):
                    return scrip, info["api_industry"], query, "ok"
                return scrip, None, query, "no-industry-field"
            except Exception as e:  # noqa: BLE001
                logger.debug("Indian API failed for SCRIP %s ('%s'): %s", scrip, query, e)
                return scrip, None, query, f"exc:{type(e).__name__}"

        with ThreadPoolExecutor(max_workers=parallelism) as pool:
            for scrip, ind, q, status in pool.map(_query_one, rows):
                api_industry_by_scrip[scrip] = ind
                api_calls += 1
                if debug:
                    debug_rows.append({
                        "bse_scrip_code": scrip,
                        "query_sent": q,
                        "api_industry": ind,
                        "status": status,
                    })

    # ----- Sequential classify + DB write -----------------------------
    for row in rows:
        scrip = int(row["bse_scrip_code"])
        name = (row.get("company_name") or "").strip()
        api_industry = api_industry_by_scrip.get(scrip)

        label, source = classify_industry(name, api_industry)
        if label:
            company_service.update_industry(scrip, label, source)
            if source == "indian_api":
                n_api += 1
            else:
                n_keyword += 1
        else:
            unresolved.append({
                "id": int(scrip),
                "company_name": name,
                "api_industry": api_industry,
            })

    n_llm = 0
    if use_llm and unresolved:
        llm_hits = classify_with_llm(unresolved)
        for scrip_id, label in llm_hits.items():
            if label in CANON_NAMES:
                company_service.update_industry(int(scrip_id), label, "llm")
                n_llm += 1
        # Anything still unresolved after LLM
        n_unknown = len(unresolved) - n_llm
    else:
        n_unknown = len(unresolved)

    remaining = company_service.count_uncategorized()
    response = {
        "processed": len(rows),
        "classified_indian_api": n_api,
        "classified_keyword": n_keyword,
        "classified_llm": n_llm,
        "still_unknown": n_unknown,
        "indian_api_calls": api_calls,
        "remaining_uncategorized": remaining,
        "unresolved_sample": [
            {"bse_scrip_code": u["id"], "company_name": u["company_name"]}
            for u in unresolved[:10]
        ],
    }
    if debug:
        # Per-row diagnostics: query string sent + status from Indian API.
        response["debug"] = debug_rows[:30]
    return response


# =========================================================================
# Auth – WhatsApp OTP Login
# =========================================================================

class SendOTPRequest(BaseModel):
    phone: str = Field(..., description="Phone number (with or without country code)", examples=["9474841416", "919474841416"])

class VerifyOTPRequest(BaseModel):
    phone: str = Field(..., description="Phone number used to request the OTP")
    otp: str = Field(..., min_length=4, max_length=4, description="4-digit OTP received on WhatsApp")
    name: Optional[str] = Field(None, max_length=100, description="User's display name (collected at login)")

class UpdateNameRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, description="User's display name")

class MetaEventRequest(BaseModel):
    event_name: str = Field(..., min_length=1, max_length=100, description="Meta event name")
    event_id: Optional[str] = Field(None, max_length=200, description="Event ID for deduplication")
    phone: Optional[str] = Field(None, max_length=20, description="Phone number for event matching")
    external_id: Optional[str] = Field(None, max_length=200, description="Stable internal user identifier")
    fbc: Optional[str] = Field(None, max_length=200, description="Facebook click ID token (_fbc)")
    fbp: Optional[str] = Field(None, max_length=200, description="Facebook browser ID token (_fbp)")
    event_source_url: Optional[str] = Field(None, max_length=500, description="Page URL where event happened")
    action_source: str = Field("website", description="Meta action source")


def _get_current_user(authorization: Optional[str] = Header(None)) -> dict:
    """
    Helper that extracts and validates the JWT from the Authorization header.
    Returns the decoded token payload or raises 401.
    """
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header missing.")

    token = authorization.replace("Bearer ", "").strip()
    if not token:
        raise HTTPException(status_code=401, detail="Token missing.")

    auth_service = AuthService()
    decoded = auth_service.decode_token(token)

    if not decoded["valid"]:
        raise HTTPException(status_code=401, detail=decoded["message"])

    return decoded


@app.post("/api/auth/send-otp", summary="Send OTP to WhatsApp number")
def send_otp(body: SendOTPRequest):
    """
    Generates a 6-digit OTP, saves it, and sends it to the
    user's WhatsApp number via WATI.
    """
    auth_service = AuthService()
    result = auth_service.send_otp(body.phone)

    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])

    return {"message": result["message"]}


@app.post("/api/auth/verify-otp", summary="Verify OTP and get JWT token")
def verify_otp(body: VerifyOTPRequest):
    """
    Verifies the OTP. On success returns a JWT token (valid 30 days)
    and the user object.  Automatically creates the user on first login.
    """
    auth_service = AuthService()
    result = auth_service.verify_otp(body.phone, body.otp, name=body.name)

    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])

    return {
        "token": result["token"],
        "is_new_user": result["is_new_user"],
        "user": result["user"],
    }


@app.post("/api/events/visit", summary="Record a page visit event")
def record_visit(authorization: Optional[str] = Header(None)):
    """
    Records a page visit event for the logged-in user.
    Called by the frontend each time the website is opened.
    """
    decoded = _get_current_user(authorization)
    phone = decoded.get("phone")
    if not phone:
        raise HTTPException(status_code=400, detail="Invalid token.")

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Look up current user_id by phone (JWT user_id can be stale)
                cur.execute("SELECT id FROM users WHERE phone = %s", (phone,))
                row = cur.fetchone()
                if row:
                    cur.execute(
                        "INSERT INTO user_events (user_id, event_type) VALUES (%s, 'page_visit')",
                        (row[0],),
                    )
    except Exception as e:
        logger.warning("Failed to record visit event for %s: %s", phone, e)

    return {"ok": True}


def _normalize_phone_for_meta(phone: Optional[str]) -> Optional[str]:
    if not phone:
        return None
    digits = "".join(ch for ch in str(phone) if ch.isdigit())
    if len(digits) == 10:
        digits = f"91{digits}"
    return digits if len(digits) >= 10 else None


def _sha256_lower(value: str) -> str:
    return hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest()


def _rolling_incoming_messages(
    current_text: Optional[str], new_message: str, max_messages: int = 4
) -> str:
    """
    Store the last `max_messages` inbound WhatsApp texts in one column as a JSON array string.
    Older rows that hold a single plain-text message are migrated on the next inbound event.
    """
    msg = (new_message or "").strip()
    if len(msg) > 2000:
        msg = msg[:2000]
    msgs: List[str] = []
    if current_text and str(current_text).strip():
        try:
            parsed = json.loads(current_text)
            if isinstance(parsed, list):
                msgs = [str(x) for x in parsed if x is not None]
            else:
                msgs = [str(current_text).strip()]
        except (json.JSONDecodeError, TypeError):
            msgs = [str(current_text).strip()]
    msgs.append(msg)
    msgs = msgs[-max_messages:]
    return json.dumps(msgs, ensure_ascii=False)


def _client_ip_for_meta(request: Request) -> Optional[str]:
    """Real client IP when behind a proxy (Render, Netlify, etc.)."""
    xff = request.headers.get("x-forwarded-for") or request.headers.get("X-Forwarded-For")
    if xff:
        return xff.split(",")[0].strip()
    if request.client:
        return request.client.host
    return None


@app.post("/api/meta/conversions-event", summary="Send website conversion event to Meta Conversions API")
def send_meta_conversion_event(body: MetaEventRequest, request: Request):
    """
    Server-side event forwarding to Meta Conversions API.
    Use this endpoint from frontend actions (OTP requested, etc).
    """
    if not META_PIXEL_ID or not META_ACCESS_TOKEN:
        raise HTTPException(status_code=500, detail="Meta Pixel is not configured.")

    user_data = {
        "client_ip_address": _client_ip_for_meta(request),
        "client_user_agent": request.headers.get("user-agent"),
    }

    normalized_phone = _normalize_phone_for_meta(body.phone)
    resolved_external_id = body.external_id

    # If external_id isn't passed, derive from users.id (best stable identifier) when possible.
    if not resolved_external_id and normalized_phone:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM users WHERE phone = %s LIMIT 1", (normalized_phone,))
                    row = cur.fetchone()
                    if row and row[0]:
                        resolved_external_id = f"user_{row[0]}"
        except Exception as e:
            logger.warning("Meta CAPI external_id lookup failed for phone=%s: %s", normalized_phone, e)

    # Final fallback external_id from phone if user row doesn't exist yet.
    if not resolved_external_id and normalized_phone:
        resolved_external_id = f"phone_{normalized_phone}"

    if normalized_phone:
        user_data["ph"] = [_sha256_lower(normalized_phone)]
    if resolved_external_id:
        # external_id should be hashed before sending to Meta.
        user_data["external_id"] = [_sha256_lower(resolved_external_id)]
    if body.fbc:
        user_data["fbc"] = body.fbc
    if body.fbp:
        user_data["fbp"] = body.fbp

    event_payload = {
        "event_name": body.event_name,
        "event_time": int(datetime.now(timezone.utc).timestamp()),
        "action_source": body.action_source or "website",
        "user_data": user_data,
    }
    if body.event_id:
        event_payload["event_id"] = body.event_id
    if body.event_source_url:
        event_payload["event_source_url"] = body.event_source_url

    payload = {"data": [event_payload]}
    if META_TEST_EVENT_CODE:
        payload["test_event_code"] = META_TEST_EVENT_CODE

    url = f"https://graph.facebook.com/v20.0/{META_PIXEL_ID}/events"
    try:
        resp = requests.post(
            url,
            params={"access_token": META_ACCESS_TOKEN},
            json=payload,
            timeout=15,
        )
        data = resp.json() if resp.content else {}
        if not resp.ok:
            logger.warning("Meta CAPI error: status=%s body=%s", resp.status_code, data)
            raise HTTPException(status_code=502, detail="Failed to send event to Meta.")
        return {"ok": True, "meta_response": data}
    except HTTPException:
        raise
    except Exception as e:
        logger.warning("Meta CAPI request failed: %s", e)
        raise HTTPException(status_code=502, detail="Failed to send event to Meta.")


@app.get("/api/auth/me", summary="Get current logged-in user")
def get_me(authorization: Optional[str] = Header(None)):
    """
    Returns the current user's profile.  Requires a valid JWT
    in the Authorization header: `Bearer <token>`
    """
    decoded = _get_current_user(authorization)
    auth_service = AuthService()
    user = auth_service.get_user(decoded["phone"])

    if not user:
        raise HTTPException(status_code=404, detail="User not found.")

    return {"user": user}


@app.put("/api/auth/me/name", summary="Update user display name")
def update_name(body: UpdateNameRequest, authorization: Optional[str] = Header(None)):
    """
    Updates the current user's display name.
    Requires a valid JWT in the Authorization header.
    """
    decoded = _get_current_user(authorization)
    auth_service = AuthService()
    updated = auth_service.update_user_name(decoded["phone"], body.name)

    if not updated:
        raise HTTPException(status_code=404, detail="User not found.")

    return {"message": "Name updated successfully.", "name": body.name}


# =========================================================================
# Stock Search & Watchlist
# =========================================================================

@app.get("/api/stocks/search", summary="Search companies by name or NSE symbol")
def search_stocks(
    q: str = Query(..., min_length=1, description="Search term (company name or NSE symbol)"),
    limit: int = Query(20, ge=1, le=50, description="Max results to return"),
):
    """
    Searches the company_master table for companies matching the query.
    Returns results sorted by market cap (largest first).
    """
    service = WatchlistService()
    results = service.search_companies(q, limit)
    return {"results": results, "count": len(results)}


class SaveWatchlistRequest(BaseModel):
    scrip_codes: List[int] = Field(..., description="List of BSE scrip codes (3-15)")
    receive_all_updates: bool = Field(False, description="Also receive updates for stocks not in watchlist")


@app.get("/api/user/watchlist", summary="Get current user's stock watchlist")
def get_watchlist(authorization: Optional[str] = Header(None)):
    """
    Returns the user's selected stocks and notification preferences.
    Requires a valid JWT in the Authorization header.
    """
    decoded = _get_current_user(authorization)
    service = WatchlistService()
    return service.get_watchlist(decoded["user_id"])


@app.put("/api/user/watchlist", summary="Save user's stock watchlist")
def save_watchlist(body: SaveWatchlistRequest, authorization: Optional[str] = Header(None)):
    """
    Replaces the user's entire watchlist with the given scrip codes.
    Must have 3-15 stocks. Also sets the 'receive_all_updates' preference.
    Requires a valid JWT in the Authorization header.
    """
    decoded = _get_current_user(authorization)
    service = WatchlistService()
    result = service.save_watchlist(
        user_id=decoded["user_id"],
        scrip_codes=body.scrip_codes,
        receive_all_updates=body.receive_all_updates,
    )

    if not result["success"]:
        raise HTTPException(status_code=400, detail=result["message"])

    return {"message": f"Watchlist saved with {result['count']} stocks.", "count": result["count"]}


# =========================================================================
# Test – Manual notification trigger (for debugging)
# =========================================================================

@app.post("/api/test/send-market-update", summary="[TEST] Send a market update to all users")
def test_send_market_update():
    """
    Sends a test market update notification to all active users.
    Uses dummy data to verify the template works end-to-end.
    """
    test_item = {
        "company_name": "Laxmi Organic Industries Ltd",
        "category": "Board Meeting",
        "impact": "POSITIVE",
        "summary": "Laxmi Organic Industries reported strong quarterly results with revenue growth of 12% YoY. The board has recommended a dividend of Rs 3 per share.",
        "news_time": "2026-03-02T14:30:00",
    }

    notif_service = NotificationService()
    result = notif_service.notify_all_users(test_item)

    return {
        "message": "Test notification triggered.",
        "result": result,
    }


# =========================================================================
# Backfill – Populate whatsapp_broadcast from existing ui_data
# =========================================================================

@app.post("/api/admin/backfill-whatsapp-broadcast", summary="[ADMIN] Backfill whatsapp_broadcast from today's ui_data")
def backfill_whatsapp_broadcast():
    """
    Backfills the whatsapp_broadcast table from today's ui_data records only.
    Applies the same filtering rules as the live pipeline:
    - Financial Results category: STRONGLY POSITIVE or BEAT only (no POSITIVE/NEUTRAL/NEGATIVE/etc.)
    - Other categories: STRONGLY POSITIVE/BEAT (all); STRONGLY NEGATIVE only if market cap > 10,000 Cr
    - Regular NEGATIVE (not STRONGLY), MISSED: excluded

    Skips records already in whatsapp_broadcast (scrip_cd + news_time), or same scrip_cd + category
    as an existing row (dedupe repeated filings until cleanup removes old rows).
    """
    logger.info("🔄 Starting whatsapp_broadcast backfill from today's ui_data...")
    
    company_service = CompanyService()
    ui_service = UIDataService()
    
    # Get today's ui_data records only
    target_date = _now_ist_naive()
    try:
        all_ui_data = ui_service.get_latest_ui_data(target_date=target_date)
        logger.info("  📊 Found %d records in ui_data for today (%s)", len(all_ui_data), target_date.strftime('%Y-%m-%d'))
    except Exception as e:
        logger.error("  ❌ Failed to fetch ui_data: %s", e)
        raise HTTPException(status_code=500, detail=f"Failed to fetch ui_data: {e}")
    
    if not all_ui_data:
        return {
            "message": "No ui_data records found.",
            "processed": 0,
            "inserted": 0,
            "skipped": 0,
            "errors": 0,
        }
    
    # Get existing whatsapp_broadcast records to avoid duplicates
    existing_keys = set()
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT scrip_cd, news_time FROM whatsapp_broadcast")
                for row in cur.fetchall():
                    scrip_cd, news_time = row
                    if scrip_cd and news_time:
                        existing_keys.add((str(scrip_cd), str(news_time)))
        logger.info("  📋 Found %d existing records in whatsapp_broadcast", len(existing_keys))
    except Exception as e:
        logger.warning("  ⚠️ Could not fetch existing records: %s", e)
    
    inserted_count = 0
    skipped_count = 0
    error_count = 0
    
    # Process each ui_data record
    for item in all_ui_data:
        try:
            # Check if already exists
            scrip_cd = item.get("scrip_cd")
            news_time = item.get("news_time")
            if scrip_cd and news_time:
                key = (str(scrip_cd), str(news_time))
                if key in existing_keys:
                    skipped_count += 1
                    continue
            
            # Apply filtering logic
            should_broadcast, mkt_cap_cr = _should_include_in_whatsapp_broadcast(item, company_service)
            
            if not should_broadcast:
                skipped_count += 1
                continue

            if _whatsapp_broadcast_has_scrip_and_category(scrip_cd, item.get("category")):
                skipped_count += 1
                continue
            
            # Insert into whatsapp_broadcast (check for duplicates manually since no unique constraint)
            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        # Check if already exists (convert news_time to IST for comparison)
                        news_time_ist_for_check = _to_ist(news_time) if news_time else None
                        cur.execute(
                            """
                            SELECT COUNT(*) FROM whatsapp_broadcast
                            WHERE scrip_cd = %s AND news_time = %s
                            """,
                            (scrip_cd, news_time_ist_for_check),
                        )
                        if cur.fetchone()[0] > 0:
                            skipped_count += 1
                            continue
                        
                        # Convert news_time to IST before storing
                        news_time_ist = _to_ist(news_time) if news_time else None
                        current_time_ist = _now_ist_naive().replace(tzinfo=IST)  # Current time in IST
                        
                        # Insert new record
                        cur.execute(
                            """
                            INSERT INTO whatsapp_broadcast
                                (scrip_cd, company_name, impact, category, summary, pdf_link, news_time, mkt_cap_cr, data, created_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                item.get("scrip_cd"),
                                item.get("company_name", "Unknown"),
                                item.get("impact"),
                                item.get("category"),
                                item.get("summary"),
                                item.get("pdf_link"),
                                news_time_ist,
                                mkt_cap_cr,
                                Json(_make_json_serializable(item)),  # Convert Timestamps to strings
                                current_time_ist,  # Explicitly set created_at in IST
                            ),
                        )
                        inserted_count += 1
                        if scrip_cd and news_time:
                            existing_keys.add((str(scrip_cd), str(news_time)))
            except Exception as e:
                logger.warning("  ⚠️ Failed to insert record for %s: %s", item.get("company_name"), e)
                error_count += 1
        except Exception as e:
            logger.warning("  ⚠️ Error processing record: %s", e)
            error_count += 1
    
    logger.info("  ✅ Backfill complete: %d inserted, %d skipped, %d errors", inserted_count, skipped_count, error_count)
    
    return {
        "message": f"Backfill complete. {inserted_count} records inserted into whatsapp_broadcast.",
        "processed": len(all_ui_data),
        "inserted": inserted_count,
        "skipped": skipped_count,
        "errors": error_count,
    }


@app.post(
    "/api/admin/send-user-training",
    summary="[ADMIN] Send the 'user_training' utility template (feature explainer) to all active users",
)
async def send_user_training(
    dry_run: bool = Query(False, description="If true, return the recipient list without sending."),
    force: bool = Query(False, description="If true, bypass the 15-day cadence and send immediately."),
):
    """
    Sends the 'user_training' WhatsApp utility template (Gupshup ID
    b6082566-52b3-43e4-b13a-3053db1f456b) to every active user.

    Template params per user:
      {{1}} = "user"               (literal — the body reads "Hi user,")
      {{2}} = comma-separated list of company names in the user's watchlist
              (or "no stocks added yet" if the watchlist is empty)
      {{3}} = "turned on"  if receive_all_updates = TRUE
              "turned off" otherwise

    Cadence: this endpoint is also driven automatically every
    USER_TRAINING_INTERVAL_DAYS days by the background scheduler. Use
    `force=true` to bypass the cadence check and send right now.
    """
    if dry_run:
        recipients = _fetch_user_training_recipients()
        sample = [
            {
                "phone": r["phone"],
                "watchlist_csv": _format_watchlist_csv(r["watchlist_companies"]),
                "high_impact_label": _format_high_impact_label(r["receive_all_updates"]),
            }
            for r in recipients[:25]
        ]
        return {
            "message": "Dry run – no messages sent.",
            "total_recipients": len(recipients),
            "sample": sample,
        }

    if force:
        # Bypass cadence: still update last_run_at so the next periodic
        # tick doesn't re-fire for another USER_TRAINING_INTERVAL_DAYS days.
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO scheduled_jobs (job_name, last_run_at, last_status, updated_at)
                        VALUES (%s, NOW(), 'running', NOW())
                        ON CONFLICT (job_name) DO UPDATE
                            SET last_run_at = NOW(),
                                last_status = 'running',
                                updated_at = NOW()
                        """,
                        (USER_TRAINING_JOB_NAME,),
                    )
        except Exception as e:
            logger.warning("  ⚠️ force send: failed to stamp scheduled_jobs: %s", e)
        try:
            result = await asyncio.to_thread(_run_user_training_broadcast)
            _record_scheduled_job_result(USER_TRAINING_JOB_NAME, "success", result)
            return {"message": "user_training broadcast sent (forced).", **result}
        except Exception as e:
            _record_scheduled_job_result(USER_TRAINING_JOB_NAME, "failed", {"error": str(e)})
            raise HTTPException(status_code=500, detail=f"user_training broadcast failed: {e}")

    if not _claim_scheduled_job(USER_TRAINING_JOB_NAME, USER_TRAINING_INTERVAL_DAYS):
        return {
            "message": (
                f"Skipped — last user_training broadcast is less than "
                f"{USER_TRAINING_INTERVAL_DAYS} days old. Use force=true to override."
            ),
            "sent": 0,
            "failed": 0,
            "total": 0,
        }

    try:
        result = await asyncio.to_thread(_run_user_training_broadcast)
        _record_scheduled_job_result(USER_TRAINING_JOB_NAME, "success", result)
        return {"message": "user_training broadcast sent.", **result}
    except Exception as e:
        _record_scheduled_job_result(USER_TRAINING_JOB_NAME, "failed", {"error": str(e)})
        raise HTTPException(status_code=500, detail=f"user_training broadcast failed: {e}")


@app.get(
    "/api/admin/user-training-status",
    summary="[ADMIN] Last run timestamp + status for the user_training broadcast",
)
def user_training_status():
    """Returns last_run_at, last_status, and the next scheduled run for the user_training job."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT last_run_at, last_status, last_meta, updated_at
                    FROM scheduled_jobs
                    WHERE job_name = %s
                    """,
                    (USER_TRAINING_JOB_NAME,),
                )
                row = cur.fetchone()
        if not row:
            return {
                "job_name": USER_TRAINING_JOB_NAME,
                "interval_days": USER_TRAINING_INTERVAL_DAYS,
                "last_run_at": None,
                "next_run_at": "due now",
                "last_status": None,
                "last_meta": None,
            }
        last_run_at, last_status, last_meta, updated_at = row
        next_run_at = (
            (last_run_at + timedelta(days=USER_TRAINING_INTERVAL_DAYS)).isoformat()
            if last_run_at
            else "due now"
        )
        return {
            "job_name": USER_TRAINING_JOB_NAME,
            "interval_days": USER_TRAINING_INTERVAL_DAYS,
            "last_run_at": last_run_at.isoformat() if last_run_at else None,
            "next_run_at": next_run_at,
            "last_status": last_status,
            "last_meta": last_meta,
            "updated_at": updated_at.isoformat() if updated_at else None,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read user_training status: {e}")


@app.post("/api/admin/send-pending-broadcasts", summary="[ADMIN] Send notifications for unsent whatsapp_broadcast entries")
def send_pending_broadcasts():
    """
    Sends WhatsApp notifications for entries in whatsapp_broadcast that haven't been sent yet
    (where sent_at IS NULL). This is useful if notifications failed during the pipeline run.
    
    Returns:
        {"message": str, "processed": int, "sent": int, "failed": int}
    """
    logger.info("🔄 Starting send-pending-broadcasts...")
    
    # Get all unsent entries from whatsapp_broadcast
    unsent_entries = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, scrip_cd, company_name, impact, category, summary, pdf_link, 
                       news_time, mkt_cap_cr, data
                FROM whatsapp_broadcast
                WHERE sent_at IS NULL
                ORDER BY created_at DESC
                """
            )
            rows = cur.fetchall()
            
            for row in rows:
                entry = {
                    "id": row[0],
                    "scrip_cd": str(row[1]) if row[1] else None,
                    "company_name": row[2],
                    "impact": row[3],
                    "category": row[4],
                    "summary": row[5],
                    "pdf_link": row[6],
                    "news_time": row[7],
                    "mkt_cap_cr": float(row[8]) if row[8] else None,
                }
                
                # Merge data from JSONB if available
                if row[9]:
                    data_dict = row[9] if isinstance(row[9], dict) else {}
                    entry.update(data_dict)
                
                unsent_entries.append(entry)
    
    if not unsent_entries:
        logger.info("  ✅ No unsent entries found in whatsapp_broadcast")
        return {
            "message": "No unsent entries found in whatsapp_broadcast",
            "processed": 0,
            "sent": 0,
            "failed": 0,
        }
    
    logger.info("  📋 Found %d unsent entries in whatsapp_broadcast", len(unsent_entries))
    
    # Send notifications
    notif_service = NotificationService()
    total_sent = 0
    total_failed = 0
    
    for entry in unsent_entries:
        try:
            result = notif_service.notify_all_users(entry)
            if result["sent"] > 0:
                total_sent += result["sent"]
                # Mark as sent
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        # Set sent_at in IST
                        sent_at_ist = _now_ist_naive().replace(tzinfo=IST)
                        cur.execute(
                            """
                            UPDATE whatsapp_broadcast
                            SET sent_at = %s
                            WHERE id = %s
                            """,
                            (sent_at_ist, entry["id"]),
                        )
            else:
                total_failed += 1
        except Exception as e:
            logger.warning("  ⚠️ Failed to send notification for entry %s: %s", entry.get("id"), e)
            total_failed += 1
    
    logger.info("  ✅ Send-pending-broadcasts complete: %d sent, %d failed", total_sent, total_failed)
    
    return {
        "message": f"Processed {len(unsent_entries)} entries. {total_sent} notifications sent, {total_failed} failed.",
        "processed": len(unsent_entries),
        "sent": total_sent,
        "failed": total_failed,
    }


@app.post("/api/admin/send-last-broadcast", summary="[ADMIN] Manually send the last entry from whatsapp_broadcast")
def send_last_broadcast():
    """
    Gets the most recent entry from whatsapp_broadcast and sends notifications
    to all eligible users. This will resend even if sent_at is already set.
    
    Returns:
        {"message": str, "entry": dict, "sent": int, "failed": int}
    """
    logger.info("🔄 Starting send-last-broadcast...")
    
    # Get the last entry from whatsapp_broadcast
    last_entry = None
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, scrip_cd, company_name, impact, category, summary, pdf_link, 
                       news_time, mkt_cap_cr, data, created_at
                FROM whatsapp_broadcast
                ORDER BY created_at DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            
            if not row:
                logger.info("  ✅ No entries found in whatsapp_broadcast")
                return {
                    "message": "No entries found in whatsapp_broadcast",
                    "entry": None,
                    "sent": 0,
                    "failed": 0,
                }
            
            last_entry = {
                "id": row[0],
                "scrip_cd": str(row[1]) if row[1] else None,
                "company_name": row[2],
                "impact": row[3],
                "category": row[4],
                "summary": row[5],
                "pdf_link": row[6],
                "news_time": row[7],
                "mkt_cap_cr": float(row[8]) if row[8] else None,
                "created_at": row[10].isoformat() if row[10] else None,
            }
            
            # Merge data from JSONB if available
            if row[9]:
                data_dict = row[9] if isinstance(row[9], dict) else {}
                last_entry.update(data_dict)
    
    logger.info("  📋 Found last entry: %s (ID: %s, created: %s)", 
                last_entry.get("company_name"), last_entry.get("id"), last_entry.get("created_at"))
    
    # Send notifications
    notif_service = NotificationService()
    try:
        result = notif_service.notify_all_users(last_entry)
        sent = result.get("sent", 0)
        failed = result.get("failed", 0)
        
        # Mark as sent (update sent_at even if it was already set)
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Set sent_at in IST
                sent_at_ist = _now_ist_naive().replace(tzinfo=IST)
                cur.execute(
                    """
                    UPDATE whatsapp_broadcast
                    SET sent_at = %s
                    WHERE id = %s
                    """,
                    (sent_at_ist, last_entry["id"]),
                )
        
        logger.info("  ✅ Send-last-broadcast complete: %d sent, %d failed", sent, failed)
        
        return {
            "message": f"Sent notification for '{last_entry.get('company_name')}': {sent} sent, {failed} failed",
            "entry": {
                "id": last_entry.get("id"),
                "company_name": last_entry.get("company_name"),
                "scrip_cd": last_entry.get("scrip_cd"),
                "impact": last_entry.get("impact"),
                "created_at": last_entry.get("created_at"),
            },
            "sent": sent,
            "failed": failed,
        }
    except Exception as e:
        logger.error("  ❌ Failed to send notification: %s", e)
        return {
            "message": f"Failed to send notification: {str(e)}",
            "entry": {
                "id": last_entry.get("id"),
                "company_name": last_entry.get("company_name"),
            },
            "sent": 0,
            "failed": 0,
        }


class SendToPhonesRequest(BaseModel):
    """Request model for sending broadcast to specific phone numbers."""
    phones: List[str] = Field(..., description="List of phone numbers (with country code, no '+')")


@app.post("/api/admin/send-last-broadcast-to-phones", summary="[ADMIN] Send last broadcast entry to specific phone numbers")
def send_last_broadcast_to_phones(request: SendToPhonesRequest):
    """
    Gets the most recent entry from whatsapp_broadcast and sends notifications
    to the specified phone numbers. Useful for testing.
    
    Args:
        request: JSON body with "phones" array of phone numbers
        
    Returns:
        {"message": str, "entry": dict, "sent": int, "failed": int}
    """
    logger.info("🔄 Starting send-last-broadcast-to-phones for %d phone(s)...", len(request.phones))
    
    # Get the last entry from whatsapp_broadcast
    last_entry = None
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, scrip_cd, company_name, impact, category, summary, pdf_link, 
                       news_time, mkt_cap_cr, data, created_at
                FROM whatsapp_broadcast
                ORDER BY created_at DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            
            if not row:
                logger.info("  ✅ No entries found in whatsapp_broadcast")
                return {
                    "message": "No entries found in whatsapp_broadcast",
                    "entry": None,
                    "sent": 0,
                    "failed": 0,
                }
            
            last_entry = {
                "id": row[0],
                "scrip_cd": str(row[1]) if row[1] else None,
                "company_name": row[2],
                "impact": row[3],
                "category": row[4],
                "summary": row[5],
                "pdf_link": row[6],
                "news_time": row[7],
                "mkt_cap_cr": float(row[8]) if row[8] else None,
                "created_at": row[10].isoformat() if row[10] else None,
            }
            
            # Merge data from JSONB if available
            if row[9]:
                data_dict = row[9] if isinstance(row[9], dict) else {}
                last_entry.update(data_dict)
    
    logger.info("  📋 Found last entry: %s (ID: %s, created: %s)", 
                last_entry.get("company_name"), last_entry.get("id"), last_entry.get("created_at"))
    
    # Send notifications to specified phones using WhatsApp service directly
    whatsapp_service = WhatsAppService()
    try:
        # Regular broadcast (not watchlist-only) since it's from whatsapp_broadcast table
        result = whatsapp_service.send_market_update_broadcast(request.phones, last_entry, is_watchlist=False)
        sent = result.get("sent", 0)
        failed = result.get("failed", 0)
        
        logger.info("  ✅ Send-last-broadcast-to-phones complete: %d sent, %d failed", sent, failed)
        
        return {
            "message": f"Sent notification for '{last_entry.get('company_name')}' to {len(request.phones)} phone(s): {sent} sent, {failed} failed",
            "entry": {
                "id": last_entry.get("id"),
                "company_name": last_entry.get("company_name"),
                "scrip_cd": last_entry.get("scrip_cd"),
                "impact": last_entry.get("impact"),
                "created_at": last_entry.get("created_at"),
            },
            "phones_requested": len(request.phones),
            "sent": sent,
            "failed": failed,
        }
    except Exception as e:
        logger.error("  ❌ Failed to send notification: %s", e)
        return {
            "message": f"Failed to send notification: {str(e)}",
            "entry": {
                "id": last_entry.get("id"),
                "company_name": last_entry.get("company_name"),
            },
            "phones_requested": len(request.phones),
            "sent": 0,
            "failed": 0,
        }


# ──────────────────────────────────────────────────────────────────────
# Gupshup Webhook Endpoint for Message Delivery Status
# ──────────────────────────────────────────────────────────────────────

@app.post("/api/webhooks/gupshup-delivery", summary="Gupshup webhook for message delivery status and inbound messages")
async def gupshup_delivery_webhook(request: Request):
    """
    Receives delivery status updates from Gupshup webhooks.
    
    Handles both Gupshup v2 format and Meta v3 format.
    Stores delivery status (sent, delivered, read, failed) in database.
    
    Events received:
    - sent: Message was sent to WhatsApp
    - delivered: Message was delivered to user's device
    - read: Message was read by user
    - failed: Message failed to send
    - enqueued: Message is queued for sending
    """
    try:
        # Get raw body to store for debugging
        body = await request.body()
        payload = await request.json() if body else {}
        
        logger.info("  📥 Received Gupshup webhook: %s", payload)
        
        # Handle both Gupshup v2 and Meta v3 formats
        # Gupshup v2 format: {"type": "message-event", "payload": {...}}
        # Meta v3 format: {"entry": [{"changes": [{"value": {...}}]}]}
        
        message_id = None
        phone = None
        status = None
        error_code = None
        error_message = None
        timestamp = None
        
        # Try Gupshup v2 format first
        if payload.get("type") == "message-event":
            event_payload = payload.get("payload", {})
            message_id = event_payload.get("messageId") or event_payload.get("id")
            phone = event_payload.get("destination") or event_payload.get("phone")
            status = event_payload.get("eventType") or event_payload.get("status", "").lower()
            error_code = event_payload.get("errorCode")
            error_message = event_payload.get("errorMessage") or event_payload.get("error")
            timestamp_str = event_payload.get("timestamp") or event_payload.get("time")
            if timestamp_str:
                try:
                    # Handle Unix timestamp or ISO format
                    if isinstance(timestamp_str, (int, float)):
                        timestamp = datetime.fromtimestamp(timestamp_str, tz=timezone.utc)
                    else:
                        timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                except Exception:
                    timestamp = datetime.now(timezone.utc)
            else:
                timestamp = datetime.now(timezone.utc)
        
        # Try Meta v3 format
        elif payload.get("entry"):
            inbound_processed = False
            for entry in payload.get("entry", []):
                for change in entry.get("changes", []):
                    value = change.get("value", {})
                    # Inbound messages from users
                    if "messages" in value:
                        try:
                            messages = value.get("messages", [])
                            contacts = value.get("contacts", [])
                            contact_phone = None
                            if contacts:
                                contact_phone = contacts[0].get("wa_id") or contacts[0].get("phone")
                            for m in messages:
                                msg_from = m.get("from") or contact_phone
                                msg_ts_raw = m.get("timestamp")
                                msg_ts = None
                                if msg_ts_raw:
                                    try:
                                        ts_val = int(msg_ts_raw)
                                        if ts_val > 1e12:
                                            ts_val = ts_val / 1000
                                        msg_ts = datetime.fromtimestamp(ts_val, tz=timezone.utc)
                                    except Exception:
                                        msg_ts = datetime.now(timezone.utc)
                                else:
                                    msg_ts = datetime.now(timezone.utc)
                                # Extract text body if available
                                text_body = None
                                if m.get("text") and isinstance(m.get("text"), dict):
                                    text_body = m["text"].get("body")
                                elif m.get("button") and isinstance(m.get("button"), dict):
                                    text_body = m["button"].get("text")
                                elif m.get("interactive") and isinstance(m.get("interactive"), dict):
                                    # Many interactive types; store as a short label
                                    text_body = m["interactive"].get("type")

                                if msg_from and text_body:
                                    norm_phone = str(msg_from).replace("+", "").replace(" ", "").replace("-", "")
                                    if len(norm_phone) == 10:
                                        norm_phone = "91" + norm_phone
                                    # Rolling last 4 inbound messages in last_incoming_message_text (JSON array string)
                                    try:
                                        with get_conn() as conn:
                                            with conn.cursor() as cur:
                                                cur.execute(
                                                    "SELECT last_incoming_message_text FROM users WHERE phone = %s",
                                                    (norm_phone,),
                                                )
                                                row = cur.fetchone()
                                                prev = row[0] if row else None
                                                combined = _rolling_incoming_messages(prev, text_body, max_messages=4)
                                                cur.execute(
                                                    """
                                                    UPDATE users
                                                    SET last_incoming_message_text = %s,
                                                        last_incoming_message_at = %s,
                                                        updated_at = NOW()
                                                    WHERE phone = %s
                                                    """,
                                                    (combined, msg_ts, norm_phone),
                                                )
                                    except Exception as e:
                                        logger.warning("  ⚠️ Failed to update last incoming message for %s: %s", norm_phone, e)
                            inbound_processed = True
                        except Exception as e:
                            logger.warning("  ⚠️ Failed processing inbound message payload: %s", e)
                    if "statuses" in value:
                        for status_item in value.get("statuses", []):
                            # Use gs_id (Gupshup message ID) or meta_msg_id (WhatsApp message ID) or id (Meta ID)
                            message_id = status_item.get("gs_id") or status_item.get("meta_msg_id") or status_item.get("id")
                            phone = status_item.get("recipient_id")
                            status_raw = status_item.get("status", "").lower()
                            # Map Meta statuses to our format
                            status_map = {
                                "sent": "sent",
                                "delivered": "delivered",
                                "read": "read",
                                "failed": "failed",
                                "pending": "enqueued",
                            }
                            status = status_map.get(status_raw, status_raw)
                            error_code = status_item.get("errors", [{}])[0].get("code") if status_item.get("errors") else None
                            error_message = status_item.get("errors", [{}])[0].get("title") if status_item.get("errors") else None
                            timestamp_raw = status_item.get("timestamp")
                            if timestamp_raw:
                                try:
                                    # Handle both string and integer timestamps
                                    # Gupshup sends timestamps as either:
                                    # - Integer milliseconds: 1773477862554 (13 digits)
                                    # - String seconds: "1773477864" (10 digits)
                                    if isinstance(timestamp_raw, str):
                                        timestamp_val = int(timestamp_raw)
                                    else:
                                        timestamp_val = timestamp_raw
                                    
                                    # If timestamp is > 1e12, it's in milliseconds, convert to seconds
                                    if timestamp_val > 1e12:
                                        timestamp_val = timestamp_val / 1000
                                    
                                    timestamp = datetime.fromtimestamp(timestamp_val, tz=timezone.utc)
                                except (ValueError, TypeError, OSError) as e:
                                    logger.warning("  ⚠️ Failed to parse timestamp %s: %s", timestamp_raw, e)
                                    timestamp = datetime.now(timezone.utc)
                            else:
                                timestamp = datetime.now(timezone.utc)
            # If we processed inbound messages, we can return success immediately
            if inbound_processed and not status:
                logger.info("  ✅ Inbound message processed and user record updated.")
                return {"status": "ok", "message": "Inbound message stored"}
        
        # If we couldn't parse, log and return
        if not message_id or not phone or not status:
            logger.warning("  ⚠️ Could not parse webhook payload: %s", payload)
            return {"status": "ok", "message": "Webhook received but could not parse"}
        
        # Normalize phone number (remove +, ensure country code)
        phone = phone.replace("+", "").replace(" ", "").replace("-", "")
        if len(phone) == 10:
            phone = "91" + phone
        
        # Look up existing record to preserve user_name and message_title
        existing_user_name = None
        existing_message_title = None
        
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # Get existing record if it exists
                    cur.execute("SELECT user_name, message_title FROM message_delivery_status WHERE message_id = %s", (message_id,))
                    existing_row = cur.fetchone()
                    if existing_row:
                        existing_user_name = existing_row[0]
                        existing_message_title = existing_row[1]
                    
                    # If user_name not set, look it up from users table
                    if not existing_user_name:
                        cur.execute("SELECT name FROM users WHERE phone = %s", (phone,))
                        user_row = cur.fetchone()
                        if user_row:
                            existing_user_name = user_row[0]
        except Exception as e:
            logger.warning("  ⚠️ Failed to look up existing record or user_name: %s", e)
        
        # Convert timestamp to IST before storing
        if timestamp:
            timestamp_ist = timestamp.astimezone(IST) if timestamp.tzinfo else timestamp.replace(tzinfo=IST)
        else:
            timestamp_ist = _now_ist_naive().replace(tzinfo=IST)
        
        # Store in database - preserve existing user_name and message_title
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO message_delivery_status 
                            (message_id, phone, user_name, message_title, status, error_code, error_message, timestamp, raw_payload)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (message_id) DO UPDATE SET
                            status = EXCLUDED.status,
                            error_code = EXCLUDED.error_code,
                            error_message = EXCLUDED.error_message,
                            timestamp = EXCLUDED.timestamp,
                            updated_at = NOW(),
                            raw_payload = EXCLUDED.raw_payload,
                            -- Preserve existing user_name and message_title (only set if NULL)
                            user_name = COALESCE(message_delivery_status.user_name, EXCLUDED.user_name),
                            message_title = COALESCE(message_delivery_status.message_title, EXCLUDED.message_title)
                        """,
                        (message_id, phone, existing_user_name, existing_message_title, status, error_code, error_message, timestamp_ist, Json(payload)),
                    )
            logger.info("  ✅ Stored delivery status: message_id=%s, phone=%s, user=%s, title=%s, status=%s", 
                       message_id, phone, existing_user_name or "N/A", existing_message_title or "N/A", status)
        except Exception as e:
            logger.error("  ❌ Failed to store delivery status: %s", e)
        
        return {"status": "ok", "message": "Webhook processed successfully"}
        
    except Exception as e:
        logger.error("  ❌ Webhook processing error: %s", e)
        return {"status": "error", "message": str(e)}


# ──────────────────────────────────────────────────────────────────────
# Analytics Endpoint for Message Delivery Status
# ──────────────────────────────────────────────────────────────────────

class DeliveryStatusQuery(BaseModel):
    """Query parameters for delivery status analytics."""
    phone: Optional[str] = Field(None, description="Filter by phone number")
    status: Optional[str] = Field(None, description="Filter by status (sent, delivered, read, failed)")
    hours: Optional[int] = Field(24, description="Number of hours to look back (default: 24)")
    limit: Optional[int] = Field(100, description="Maximum number of records to return")


@app.get("/api/admin/message-delivery-status", summary="[ADMIN] Get message delivery status analytics")
def get_delivery_status(
    phone: Optional[str] = Query(None, description="Filter by phone number"),
    status: Optional[str] = Query(None, description="Filter by status (sent, delivered, read, failed)"),
    hours: int = Query(24, description="Number of hours to look back"),
    limit: int = Query(100, description="Maximum number of records to return"),
):
    """
    Returns message delivery status analytics.
    
    Shows which messages were sent, delivered, read, or failed.
    Useful for tracking campaign performance.
    """
    try:
        cutoff = _now_ist_naive().replace(tzinfo=IST) - timedelta(hours=hours)
        
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Build query with filters
                query = """
                    SELECT 
                        message_id,
                        phone,
                        user_name,
                        message_title,
                        status,
                        error_code,
                        error_message,
                        timestamp,
                        created_at
                    FROM message_delivery_status
                    WHERE timestamp >= %s
                """
                params = [cutoff]
                
                if phone:
                    query += " AND phone = %s"
                    params.append(phone)
                
                if status:
                    query += " AND status = %s"
                    params.append(status.lower())
                
                query += " ORDER BY timestamp DESC LIMIT %s"
                params.append(limit)
                
                cur.execute(query, params)
                rows = cur.fetchall()
                
                # Get summary statistics
                cur.execute(
                    """
                    SELECT 
                        status,
                        COUNT(*) as count
                    FROM message_delivery_status
                    WHERE timestamp >= %s
                    GROUP BY status
                    """,
                    (cutoff,),
                )
                stats_rows = cur.fetchall()
                
                stats = {row[0]: row[1] for row in stats_rows}
                
                results = []
                for row in rows:
                    results.append({
                        "message_id": row[0],
                        "phone": row[1],
                        "user_name": row[2],
                        "message_title": row[3],
                        "status": row[4],
                        "error_code": row[5],
                        "error_message": row[6],
                        "timestamp": row[7].isoformat() if row[7] else None,
                        "created_at": row[8].isoformat() if row[8] else None,
                    })
                
                return {
                    "total_records": len(results),
                    "time_range_hours": hours,
                    "summary": {
                        "sent": stats.get("sent", 0),
                        "delivered": stats.get("delivered", 0),
                        "read": stats.get("read", 0),
                        "failed": stats.get("failed", 0),
                        "enqueued": stats.get("enqueued", 0),
                    },
                    "records": results,
                }
    except Exception as e:
        logger.error("  ❌ Failed to get delivery status: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
