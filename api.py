import os
import pandas as pd
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, Query, Path, HTTPException, BackgroundTasks
from contextlib import asynccontextmanager
import asyncio
import logging

from urllib.parse import unquote
from announcements import fetch_and_filter_announcements
from data_to_pdf import download_pdfs_to_dataframe
from results import analyze_pdfs_from_dataframe
from database import connect_to_db, close_db_connection
from service.announcement_service import AnnouncementService
from service.ui_data_service import UIDataService
from entity.ui_data import UIDataItem
from typing import List


@asynccontextmanager
async def lifespan(app: FastAPI):
    connect_to_db()
    yield
    close_db_connection()


analysis_lock = asyncio.Lock()
logger = logging.getLogger("uvicorn.error")

app = FastAPI(
    title="BSE Announcements Analyzer API",
    description="Triggers a pipeline to fetch, filter, download, and analyze BSE announcements.",
    lifespan=lifespan,
)


# =========================================================================
# POST /analyze-announcements/
# =========================================================================
@app.post("/analyze-announcements/", summary="Run the full analysis pipeline")
async def run_analysis_pipeline(
    background_tasks: BackgroundTasks,
    date: str | None = Query(None, description="Target date in YYYY-MM-DD format.", pattern=r"^\d{4}-\d{2}-\d{2}$"),
    cut_off_time: str = Query("20:30:00", description="Cut-off time in HH:MM:SS format."),
    market_cap_st: int = Query(2500, description="Start of market cap range (in Crores)."),
    market_cap_end: int = Query(25000, description="End of market cap range (in Crores)."),
    hours: int = Query(5, description="Lookback window in hours from now."),
    force: bool = Query(False, description="Reprocess existing announcements in the window."),
    run_now: bool = Query(False, description="Run synchronously and return counts."),
):
    target_date = None
    if date:
        try:
            target_date = datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

    decoded_cut_off_time = unquote(cut_off_time)

    if analysis_lock.locked():
        raise HTTPException(status_code=409, detail="Analysis already running. Try again later.")

    if run_now:
        result = await run_analysis_in_background(
            target_date, market_cap_st, market_cap_end, decoded_cut_off_time, hours, force
        )
        return result

    background_tasks.add_task(
        run_analysis_in_background,
        target_date, market_cap_st, market_cap_end, decoded_cut_off_time, hours, force,
    )
    return {"message": "Analysis pipeline started in the background."}


async def run_analysis_in_background(
    target_date: datetime | None,
    market_cap_start: int,
    market_cap_end: int,
    cut_off_time_str: str,
    hours: int,
    force: bool,
):
    """The main analysis workflow."""
    async with analysis_lock:
        logger.info("=== Pipeline start (hours=%s, force=%s) ===", hours, force)
        summary = {
            "filtered": 0,
            "pdfs_downloaded": 0,
            "analyzed": 0,
            "inserted_predictions": 0,
            "message": "",
        }

        # --- Step 1: Fetch & filter ---
        # Use fixed UTC+5:30 offset (no tzdata dependency on Render)
        IST = timezone(timedelta(hours=5, minutes=30))
        end_dt = datetime.now(IST).replace(tzinfo=None)   # naive IST
        start_dt = end_dt - timedelta(hours=hours)

        logger.info("Time window: %s → %s (IST, naive)", start_dt, end_dt)

        filtered_df = fetch_and_filter_announcements(
            target_date=target_date or end_dt,
            market_cap_start=market_cap_start,
            market_cap_end=market_cap_end,
            cut_off_time_str=cut_off_time_str,
            start_datetime=start_dt,
            end_datetime=end_dt,
            force_reprocess=force,
        )
        summary["filtered"] = len(filtered_df)
        if filtered_df.empty:
            summary["message"] = "No announcements found after filtering."
            logger.info(summary["message"])
            return summary

        # --- Step 2: Download PDFs ---
        pdf_df = download_pdfs_to_dataframe(filtered_df)
        summary["pdfs_downloaded"] = len(pdf_df)
        if pdf_df.empty:
            summary["message"] = "No PDFs downloaded."
            logger.info(summary["message"])
            return summary

        # --- Step 3: Analyse PDFs & rank ---
        ranked_df = analyze_pdfs_from_dataframe(pdf_df)
        summary["analyzed"] = 0 if ranked_df is None else len(ranked_df)

        if ranked_df is None or ranked_df.empty:
            summary["message"] = "Analysis produced no results."
            logger.info(summary["message"])
            return summary

        # --- Step 4: Store predictions ---
        announcement_service = AnnouncementService()
        inserted = announcement_service.create_predictions(ranked_df, "predictions", force=force)
        summary["inserted_predictions"] = len(inserted)
        summary["message"] = "Analysis completed successfully."
        logger.info("Pipeline done. Inserted %s predictions.", len(inserted))

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
    target_date = datetime.utcnow() + timedelta(hours=5, minutes=30)
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
