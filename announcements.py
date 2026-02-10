# announcements.py – Fetch, store & filter BSE announcements
from datetime import datetime
from bse import BSE
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import os

from service.announcement_service import AnnouncementService


def fetch_and_filter_announcements(
    target_date: datetime,
    market_cap_start: int = 2500,
    market_cap_end: int = 25000,
    cut_off_time_str: str = "20:30:00",
    mcap_csv_path: str = "./assets/bse_market_cap_f5.csv",
    output_dir: str = "./bse_announcements",
    start_datetime: datetime | None = None,
    end_datetime: datetime | None = None,
    force_reprocess: bool = False,
) -> pd.DataFrame:
    """
    1. Fetch all BSE announcements for the date range
    2. Store raw data in PostgreSQL (skip duplicates)
    3. Merge with market-cap CSV
    4. Filter by market cap range + time window
    5. Build full PDF URLs
    Returns a DataFrame ready for PDF download.
    """
    announcement_service = AnnouncementService()
    os.makedirs(output_dir, exist_ok=True)

    from_date = (start_datetime or target_date).date() if start_datetime else target_date
    to_date = (end_datetime or target_date).date() if end_datetime else target_date
    # If from_date and to_date are the same datetime, just use that date
    if hasattr(from_date, "date"):
        from_date = from_date.date()
    if hasattr(to_date, "date"):
        to_date = to_date.date()

    # ---- Step 1: Fetch all pages from BSE ----
    b = BSE(download_folder="./bse_downloads")
    try:
        first_page = b.announcements(page_no=1, from_date=from_date, to_date=to_date)
        total_rows = int(first_page["Table1"][0]["ROWCNT"])
    except (IndexError, KeyError, TypeError) as e:
        print(f"No announcements found from BSE: {e}")
        return pd.DataFrame()

    rows_per_page = 50
    total_pages = math.ceil(total_rows / rows_per_page)
    all_rows = []

    print(f"Fetching {total_rows} announcements across {total_pages} pages...")
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(b.announcements, page_no=p, from_date=from_date, to_date=to_date): p
            for p in range(1, total_pages + 1)
        }
        for future in as_completed(futures):
            page_no = futures[future]
            try:
                page_data = future.result()
                all_rows.extend(page_data.get("Table", []))
            except Exception as exc:
                print(f"  Page {page_no} error: {exc}")

    if not all_rows:
        print("BSE returned zero rows.")
        return pd.DataFrame()

    df_raw = pd.DataFrame(all_rows)
    print(f"Fetched {len(df_raw)} raw announcements from BSE.")

    # ---- Step 2: Store raw announcements ----
    inserted = announcement_service.create_announcements(df_raw, "raw_bse_announcements")
    print(f"Stored {len(inserted)} NEW raw announcements in DB.")

    # ---- Step 3: Decide which announcements to process ----
    if inserted:
        # New announcements were inserted – process them
        df_to_process = pd.DataFrame(inserted)
        print(f"Processing {len(df_to_process)} newly inserted announcements.")
    elif force_reprocess:
        # No new ones, but user wants to reprocess existing data in the window
        df_to_process = announcement_service.get_raw_announcements_by_window(
            start_dt=start_datetime, end_dt=end_datetime
        )
        if df_to_process.empty:
            print("Force-reprocess: no announcements found in the DB for this window.")
            return pd.DataFrame()
        print(f"Force-reprocessing {len(df_to_process)} existing announcements from DB.")
    else:
        print("No new announcements and force=False. Nothing to process.")
        return pd.DataFrame()

    # ---- Step 4: Merge with market cap CSV ----
    try:
        df_mcap = pd.read_csv(mcap_csv_path)
    except FileNotFoundError:
        print(f"WARNING: Market cap file not found at {mcap_csv_path}. Skipping mcap filter.")
        return df_to_process

    # Ensure SCRIP_CD is numeric for merge
    df_to_process["SCRIP_CD"] = pd.to_numeric(df_to_process["SCRIP_CD"], errors="coerce")
    df_mcap["FinInstrmId"] = pd.to_numeric(df_mcap["FinInstrmId"], errors="coerce")

    # Drop any existing market cap columns to avoid conflicts
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
    print(f"After market cap filter ({market_cap_start}–{market_cap_end} Cr): {len(df_filtered)} announcements.")

    if df_filtered.empty:
        return pd.DataFrame()

    # ---- Step 6: Filter by time window ----
    df_filtered["DT_TM"] = pd.to_datetime(df_filtered["DT_TM"], errors="coerce")

    if start_datetime and end_datetime:
        df_final = df_filtered[
            (df_filtered["DT_TM"] >= start_datetime) & (df_filtered["DT_TM"] <= end_datetime)
        ].copy()
    else:
        date_str = target_date.strftime("%Y-%m-%d")
        cutoff = datetime.strptime(f"{date_str} {cut_off_time_str}", "%Y-%m-%d %H:%M:%S")
        df_final = df_filtered[df_filtered["DT_TM"] > cutoff].copy()

    print(f"After time filter: {len(df_final)} announcements.")

    if df_final.empty:
        return pd.DataFrame()

    # ---- Step 7: Build full PDF URLs ----
    df_final["ATTACHMENTNAME"] = (
        "https://www.bseindia.com/xml-data/corpfiling/AttachLive/"
        + df_final["ATTACHMENTNAME"].astype(str)
    )

    # Ensure News_submission_dt is present (some JSONB rows may have it as string)
    if "News_submission_dt" in df_final.columns:
        df_final["News_submission_dt"] = pd.to_datetime(df_final["News_submission_dt"], errors="coerce")

    print(f"Returning {len(df_final)} filtered announcements for PDF download.")
    return df_final


if __name__ == "__main__":
    today = datetime.now()
    fetch_and_filter_announcements(target_date=today)
