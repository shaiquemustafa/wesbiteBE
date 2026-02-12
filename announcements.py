# announcements.py – Fetch, store & filter BSE announcements
import logging
from datetime import datetime
from bse import BSE
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import os

from service.announcement_service import AnnouncementService

logger = logging.getLogger("uvicorn.error")


def fetch_and_filter_announcements(
    target_date: datetime,
    market_cap_start: int = 2500,
    market_cap_end: int = 25000,
    mcap_csv_path: str = "./assets/bse_market_cap_f5.csv",
    output_dir: str = "./bse_announcements",
    start_datetime: datetime | None = None,
    end_datetime: datetime | None = None,
    force_reprocess: bool = False,
) -> pd.DataFrame:
    """
    Pipeline:
      1. Fetch ALL BSE announcements for target_date (full day from BSE).
      2. Store raw data in PostgreSQL (skip duplicates).
      3. Pick announcements to process:
         - force=True  → ALL matching from DB (for manual reruns)
         - force=False → newly inserted  +  any previously-stored-but-unanalyzed
      4. Merge with market-cap CSV & filter by range.
      5. Optionally filter by time window (if start/end provided).
      6. Build full PDF URLs.
    Returns a DataFrame ready for PDF download.
    """
    announcement_service = AnnouncementService()
    os.makedirs(output_dir, exist_ok=True)

    # ---- Determine which date(s) to query from BSE ----
    query_date = target_date.date() if hasattr(target_date, "date") else target_date

    # If incremental window spans midnight, also query previous day
    from_date = query_date
    to_date = query_date
    if start_datetime and end_datetime:
        sd = start_datetime.date() if hasattr(start_datetime, "date") else start_datetime
        ed = end_datetime.date() if hasattr(end_datetime, "date") else end_datetime
        from_date = min(sd, from_date)
        to_date = max(ed, to_date)

    logger.info("BSE query dates: %s → %s", from_date, to_date)

    # ---- Step 1: Fetch all pages from BSE ----
    b = BSE(download_folder="./bse_downloads")
    try:
        first_page = b.announcements(page_no=1, from_date=from_date, to_date=to_date)
    except Exception as e:
        logger.error("BSE API call failed: %s", e)
        return pd.DataFrame()

    # BSE returns Table1 with row count metadata
    table1 = first_page.get("Table1")
    if not table1 or not isinstance(table1, list) or len(table1) == 0:
        logger.warning("BSE returned no Table1 metadata for %s→%s. "
                        "Response keys: %s", from_date, to_date,
                        list(first_page.keys()) if isinstance(first_page, dict) else type(first_page))
        total_rows = 0
    else:
        try:
            total_rows = int(table1[0].get("ROWCNT", 0))
        except (ValueError, TypeError):
            total_rows = 0

    all_rows = []
    if total_rows > 0:
        rows_per_page = 50
        total_pages = math.ceil(total_rows / rows_per_page)
        logger.info("Fetching %s announcements across %s pages...", total_rows, total_pages)

        # First page already fetched
        all_rows.extend(first_page.get("Table", []))

        if total_pages > 1:
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = {
                    executor.submit(b.announcements, page_no=p, from_date=from_date, to_date=to_date): p
                    for p in range(2, total_pages + 1)
                }
                for future in as_completed(futures):
                    page_no = futures[future]
                    try:
                        page_data = future.result()
                        all_rows.extend(page_data.get("Table", []))
                    except Exception as exc:
                        logger.warning("  Page %s error: %s", page_no, exc)

        logger.info("Fetched %s raw announcements from BSE.", len(all_rows))
    else:
        logger.info("BSE reports 0 announcements for %s→%s.", from_date, to_date)

    # ---- Step 2: Store raw announcements (if any) ----
    inserted = []
    if all_rows:
        df_raw = pd.DataFrame(all_rows)
        inserted = announcement_service.create_announcements(df_raw, "raw_bse_announcements")
        logger.info("Stored %s NEW raw announcements in DB.", len(inserted))

    # ---- Step 3: Decide which announcements to process ----
    if force_reprocess:
        # FORCE mode: Pull ALL matching from DB
        if start_datetime and end_datetime:
            df_to_process = announcement_service.get_raw_announcements_by_window(
                start_dt=start_datetime, end_dt=end_datetime
            )
        else:
            day_start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
            day_end = target_date.replace(hour=23, minute=59, second=59, microsecond=999999)
            logger.info("Full-day reprocess: %s → %s", day_start, day_end)
            df_to_process = announcement_service.get_raw_announcements_by_window(
                start_dt=day_start, end_dt=day_end
            )
        if df_to_process.empty:
            logger.info("No announcements found in DB for reprocessing.")
            return pd.DataFrame()
        logger.info("Processing %s announcements from DB (force=True).", len(df_to_process))

    elif inserted:
        # We have brand-new announcements just stored
        df_to_process = pd.DataFrame(inserted)
        logger.info("Processing %s newly inserted announcements.", len(df_to_process))

    else:
        # No new insertions — but there may be announcements stored in PREVIOUS
        # scheduler runs that were never analyzed (e.g. service restarted mid-run).
        # Query for unanalyzed announcements for today.
        day_start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = target_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        df_to_process = announcement_service.get_unanalyzed_announcements(
            start_dt=day_start, end_dt=day_end
        )
        if df_to_process.empty:
            logger.info("All announcements already analyzed. Nothing to do.")
            return pd.DataFrame()
        logger.info("Found %s UNANALYZED announcements in DB — processing now.", len(df_to_process))

    # ---- Step 4: Merge with market cap CSV ----
    try:
        df_mcap = pd.read_csv(mcap_csv_path)
    except FileNotFoundError:
        logger.warning("Market cap file not found at %s.", mcap_csv_path)
        return df_to_process

    df_to_process["SCRIP_CD"] = pd.to_numeric(df_to_process["SCRIP_CD"], errors="coerce")
    df_mcap["FinInstrmId"] = pd.to_numeric(df_mcap["FinInstrmId"], errors="coerce")
    df_to_process = df_to_process.drop(columns=["Market Cap", "FinInstrmId", "market_cap"], errors="ignore")

    df_merged = df_to_process.merge(
        df_mcap[["FinInstrmId", "Market Cap"]],
        left_on="SCRIP_CD",
        right_on="FinInstrmId",
        how="left",
    )

    # ---- Step 5: Filter by market cap ----
    df_filtered = df_merged[
        (df_merged["Market Cap"] >= market_cap_start) & (df_merged["Market Cap"] <= market_cap_end)
    ].copy()
    logger.info("After market cap filter (%s–%s Cr): %s announcements.",
                market_cap_start, market_cap_end, len(df_filtered))

    if df_filtered.empty:
        return pd.DataFrame()

    # ---- Step 6: Filter by time window (ONLY if both start & end given) ----
    df_filtered["DT_TM"] = pd.to_datetime(df_filtered["DT_TM"], errors="coerce")

    if start_datetime and end_datetime:
        # Incremental mode — filter to the time window
        df_final = df_filtered[
            (df_filtered["DT_TM"] >= start_datetime) & (df_filtered["DT_TM"] <= end_datetime)
        ].copy()
        logger.info("After time filter (%s → %s): %s announcements.",
                     start_datetime, end_datetime, len(df_final))
    else:
        # Full-day mode — keep ALL announcements for the date (no time filter)
        df_final = df_filtered.copy()
        logger.info("Full-day mode: keeping all %s announcements (no time filter).", len(df_final))

    if df_final.empty:
        return pd.DataFrame()

    # ---- Step 7: Build full PDF URLs ----
    df_final["ATTACHMENTNAME"] = df_final["ATTACHMENTNAME"].apply(
        lambda x: x if str(x).startswith("http") else
        "https://www.bseindia.com/xml-data/corpfiling/AttachLive/" + str(x)
    )

    if "News_submission_dt" in df_final.columns:
        df_final["News_submission_dt"] = pd.to_datetime(df_final["News_submission_dt"], errors="coerce")

    logger.info("Returning %s filtered announcements for PDF download.", len(df_final))
    return df_final


if __name__ == "__main__":
    today = datetime.now()
    fetch_and_filter_announcements(target_date=today)
