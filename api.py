import os
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from fastapi import FastAPI, Query, Path, HTTPException, BackgroundTasks
from contextlib import asynccontextmanager
import asyncio
import logging

from urllib.parse import unquote
# Import the refactored functions
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
    # Code to run on startup
    connect_to_db()
    yield
    # Code to run on shutdown
    close_db_connection()

# A simple in-memory lock to prevent concurrent analysis runs.
analysis_lock = asyncio.Lock()
logger = logging.getLogger("uvicorn.error")

app = FastAPI(
    title="BSE Announcements Analyzer API",
    description="Triggers a pipeline to fetch, filter, download, and analyze BSE announcements.",
    lifespan=lifespan,
)

@app.post("/analyze-announcements/", summary="Run the full analysis pipeline")
async def run_analysis_pipeline(
    background_tasks: BackgroundTasks,
    date: str | None = Query(None, description="Target date in YYYY-MM-DD format.", regex=r"^\d{4}-\d{2}-\d{2}$"),
    cut_off_time: str = Query("20:30:00", description="Cut-off time in HH:MM:SS format."),
    market_cap_st: int = Query(2500, description="Start of market cap range (in Crores)."),
    market_cap_end: int = Query(25000, description="End of market cap range (in Crores)."),
    hours: int = Query(5, description="Lookback window in hours from now."),
    force: bool = Query(False, description="Reprocess existing announcements in the window."),
    run_now: bool = Query(False, description="Run synchronously and return counts.")
):
    target_date = None
    if date:
        try:
            target_date = datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Please use YYYY-MM-DD.")

    # Decode the cut_off_time string to handle URL-encoded characters like '%3A' for colons.
    decoded_cut_off_time = unquote(cut_off_time)

    if analysis_lock.locked():
        raise HTTPException(status_code=409, detail="An analysis process is already running. Please try again later.")

    if run_now:
        result = await run_analysis_in_background(
            target_date, market_cap_st, market_cap_end, decoded_cut_off_time, hours, force
        )
        return result

    background_tasks.add_task(
        run_analysis_in_background,
        target_date, market_cap_st, market_cap_end, decoded_cut_off_time, hours, force
    )

    return {"message": "Analysis pipeline started in the background."}

async def run_analysis_in_background(
    target_date: datetime,
    market_cap_start: int,
    market_cap_end: int,
    cut_off_time_str: str,
    hours: int,
    force: bool
):
    """The main analysis workflow, designed to be run as a background task."""
    async with analysis_lock:
        logger.info("Starting analysis for last %s hours (force=%s).", hours, force)
        summary = {
            "filtered": 0,
            "pdfs": 0,
            "analyzed": 0,
            "inserted_predictions": 0,
            "message": ""
        }
    
        # --- Step 1: Fetch and Filter Announcements ---
        end_dt = datetime.now(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None)
        start_dt = end_dt - timedelta(hours=hours)
        filtered_df = fetch_and_filter_announcements(
            target_date=target_date or end_dt,
            market_cap_start=market_cap_start,
            market_cap_end=market_cap_end,
            cut_off_time_str=cut_off_time_str,
            start_datetime=start_dt,
            end_datetime=end_dt,
            force_reprocess=force
        )
        summary["filtered"] = len(filtered_df)
        if filtered_df.empty:
            summary["message"] = "No announcements found after filtering."
            logger.info(summary["message"])
            return summary

        # --- Step 2: Download PDFs ---
        pdf_df = download_pdfs_to_dataframe(filtered_df)
        summary["pdfs"] = len(pdf_df)
        if pdf_df.empty:
            summary["message"] = "No PDFs downloaded."
            logger.info(summary["message"])
            return summary

        # --- Step 3: Analyze PDFs and Rank ---
        ranked_df = analyze_pdfs_from_dataframe(pdf_df)
        summary["analyzed"] = 0 if ranked_df is None else len(ranked_df)

        if ranked_df is None or ranked_df.empty:
            summary["message"] = "Analysis produced no results."
            logger.info(summary["message"])
            return summary

        # --- Step 4: Merge with original data to add News_submission_dt ---
        pdf_df['SCRIP_CD'] = pdf_df['SCRIP_CD'].astype(str)
        merge_cols = pdf_df[['SCRIP_CD', 'News_submission_dt']].drop_duplicates(subset=['SCRIP_CD'])
        final_df = pd.merge(ranked_df, merge_cols, on='SCRIP_CD', how='left')

        # --- Step 5: Store predictions in MongoDB ---
        announcement_service = AnnouncementService()
        collection_name = "predictions"
        inserted_predictions = announcement_service.create_predictions(final_df, collection_name)
        summary["inserted_predictions"] = len(inserted_predictions)
        summary["message"] = "Analysis completed."
        logger.info("Analysis completed. Inserted %s predictions.", summary["inserted_predictions"])

        #print("Background analysis task completed successfully.")
        return summary


@app.get("/predictions/{date}", summary="Fetch predictions by date")
def get_predictions(
    date: str = Path(..., description="Target date in YYYY-MM-DD format.", regex=r"^\d{4}-\d{2}-\d{2}$")
):
    """
    Retrieves the stored prediction results for a given date from MongoDB.
    """
    try:
        target_date = datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Please use YYYY-MM-DD.")

    announcement_service = AnnouncementService()
    try:
        predictions_df = announcement_service.get_predictions_by_date(target_date)
    except ConnectionError as e:
        raise HTTPException(status_code=500, detail=str(e))

    if predictions_df.empty:
        return {"message": f"No predictions found for date {date}."}

    # Replace NaN with None for JSON compatibility before converting to dict.
    # pd.NA can also be used, but None is more universally compatible.
    predictions_df = predictions_df.where(pd.notna(predictions_df), None)

    return predictions_df.to_dict("records")


@app.post("/ui-data/", summary="Store UI Data Document")
def store_ui_data(
    data_item: UIDataItem
):
    """
    Receives a single UI data item, validates it, and stores it
    as a separate entry in MongoDB.
    """
    ui_service = UIDataService()
    # The service expects a list, so we wrap our single item's dict in a list.
    data_item_as_dict_list = [data_item.model_dump()]
    
    try:
        # The service returns a list of results, one for each item we sent.
        results = ui_service.create_ui_data_document(data_item_as_dict_list)
    except ConnectionError as e:
        raise HTTPException(status_code=500, detail=str(e))

    # Since we only sent one item, we can inspect the first result.
    if not results or results[0].get("errors"):
        error_details = results[0]["errors"] if results else "An unknown error occurred."
        raise HTTPException(status_code=422, detail=error_details)

    return {"message": "UI data stored successfully", "inserted_id": results[0]["inserted_id"]}


@app.get("/ui-data/today", summary="Fetch latest UI data for the current date")
def get_todays_ui_data():
    """
    Retrieves UI data items from MongoDB that have 'news_time'
    from the previous day's 15:30:00 up to the current system time in IST.
    """
    # Get current UTC time and manually add 5 hours and 30 minutes to approximate IST.
    # WARNING: This results in a naive datetime object, which PyMongo treats as UTC.
    # This can lead to incorrect query results if not handled carefully.
    target_date = datetime.utcnow() + timedelta(hours=5, minutes=30)
    #print(target_date)
    ui_service = UIDataService()
    try:
        latest_data = ui_service.get_latest_ui_data(target_date=target_date)
    except ConnectionError as e:
        raise HTTPException(status_code=500, detail=str(e))

    if not latest_data:
        raise HTTPException(status_code=404, detail=f"No UI data found for today's date ({target_date.strftime('%Y-%m-%d')}).")
    return latest_data


@app.get("/announcements/latest", summary="Fetch the latest raw announcement")
def get_latest_announcement():
    """
    Retrieves the single most recent raw announcement from the 'raw_bse_announcements'
    collection in MongoDB, sorted by submission time.
    """
    announcement_service = AnnouncementService()
    try:
        latest_announcement = announcement_service.get_latest_announcements()
    except ConnectionError as e:
        raise HTTPException(status_code=500, detail=str(e))

    if not latest_announcement:
        return {"message": "No announcements found in the database."}

    return latest_announcement

@app.get("/ping", summary="Health check endpoint")
def ping():
    """
    Returns a simple 'pong' message to indicate the API is running.
    """
    return {"message": "pong"}