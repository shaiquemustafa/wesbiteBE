import os
import pandas as pd
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, Query, Path, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
import logging

from announcements import fetch_and_filter_announcements
from results import analyze_announcements
from stock_enrichment import enrich_prediction
from database import connect_to_db, close_db_connection, get_conn
from service.announcement_service import AnnouncementService
from service.ui_data_service import UIDataService
from entity.ui_data import UIDataItem
from typing import List

# Fixed IST offset (no tzdata dependency)
IST = timezone(timedelta(hours=5, minutes=30))

# Scheduler config (env-overridable)
SCHEDULER_INTERVAL_MIN = int(os.getenv("SCHEDULER_INTERVAL_MIN", "2"))
SCHEDULER_ENABLED = os.getenv("SCHEDULER_ENABLED", "true").lower() == "true"


def _now_ist_naive() -> datetime:
    """Current time in IST as a naive datetime."""
    return datetime.now(IST).replace(tzinfo=None)


analysis_lock = asyncio.Lock()
logger = logging.getLogger("uvicorn.error")
_scheduler_task: asyncio.Task | None = None


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
        if raw_del or pred_del or ui_del:
            logger.info("Cleanup (>48h): deleted %s raw, %s predictions, %s ui_data.", raw_del, pred_del, ui_del)
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
                    market_cap_end=25000,
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

    yield

    # Shutdown: cancel scheduler + close DB
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
    market_cap_end: int = Query(25000, description="End of market cap range (Crores)."),
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

    # --- Step 2: Analyse PDFs in batches of 25 ---
    BATCH_SIZE = 25
    total = len(filtered_df)
    total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
    total_predictions = 0
    total_ui = 0

    logger.info("  [Step 7] Analysing %s PDFs in %s batch(es) of %s ...",
                total, total_batches, BATCH_SIZE)

    for batch_start in range(0, total, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total)
        batch_df = filtered_df.iloc[batch_start:batch_end].copy()
        batch_num = (batch_start // BATCH_SIZE) + 1

        logger.info("    --- Batch %s/%s  (%s PDFs) ---",
                    batch_num, total_batches, len(batch_df))

        # Collect NEWSIDs for this batch
        batch_newsids = []
        if "NEWSID" in batch_df.columns:
            batch_newsids = batch_df["NEWSID"].dropna().astype(str).tolist()

        # 2a) Analyse PDFs in this batch
        ranked_df = analyze_announcements(batch_df)

        # 2b) Mark this batch as analyzed (even if no predictions)
        if batch_newsids:
            try:
                announcement_service.mark_as_analyzed(batch_newsids)
            except Exception as e:
                logger.warning("    Failed to mark analyzed: %s", e)

        if ranked_df is None or ranked_df.empty:
            logger.info("    Batch %s result: 0 directional predictions.", batch_num)
            continue

        # 2c) Store predictions for this batch
        inserted = announcement_service.create_predictions(
            ranked_df, "predictions", force=force
        )
        total_predictions += len(inserted)

        # 2d) Enrich & store in ui_data
        enriched_items = []
        for _, row in ranked_df.iterrows():
            try:
                enriched = enrich_prediction(row.to_dict())
                enriched_items.append(enriched)
            except Exception as e:
                logger.warning("    Enrichment failed for SCRIP %s: %s",
                               row.get("SCRIP_CD"), e)

        ui_count_batch = 0
        if enriched_items:
            ui_count_batch = ui_service.bulk_store_enriched(enriched_items)
            total_ui += ui_count_batch

        logger.info("    Batch %s result: %s predictions → DB, %s ui_data → DB.",
                    batch_num, len(inserted), ui_count_batch)

    # --- Step 3: Summary ---
    summary["analyzed"] = total
    summary["inserted_predictions"] = total_predictions
    summary["ui_data_stored"] = total_ui

    # --- Step 4: Cleanup records older than 48 hours ---
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
