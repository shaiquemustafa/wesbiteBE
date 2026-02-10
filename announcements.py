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
      3. Merge with market-cap CSV & filter by range.
      4. Optionally filter by time window (if start/end provided).
         If both are None → keep the FULL DAY (no time filter).
      5. Build full PDF URLs.
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

    print(f"BSE query dates: {from_date} → {to_date}")

    # ---- Step 1: Fetch all pages from BSE ----
    b = BSE(download_folder="./bse_downloads")
    try:
        first_page = b.announcements(page_no=1, from_date=from_date, to_date=to_date)
    except Exception as e:
        print(f"BSE API call failed: {e}")
        return pd.DataFrame()

    # BSE returns Table1 with row count metadata
    table1 = first_page.get("Table1")
    if not table1 or not isinstance(table1, list) or len(table1) == 0:
        print(f"BSE returned no Table1 metadata for {from_date}→{to_date}. "
              f"Response keys: {list(first_page.keys()) if isinstance(first_page, dict) else type(first_page)}")
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
        print(f"Fetching {total_rows} announcements across {total_pages} pages...")

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
                        print(f"  Page {page_no} error: {exc}")

        print(f"Fetched {len(all_rows)} raw announcements from BSE.")
    else:
        print(f"BSE reports 0 announcements for {from_date}→{to_date}.")

    # ---- Step 2: Store raw announcements (if any) ----
    inserted = []
    if all_rows:
        df_raw = pd.DataFrame(all_rows)
        inserted = announcement_service.create_announcements(df_raw, "raw_bse_announcements")
        print(f"Stored {len(inserted)} NEW raw announcements in DB.")

    # ---- Step 3: Decide which announcements to process ----
    # When force=True, ALWAYS pull ALL matching announcements from DB
    # (not just newly inserted ones) so existing records get analysed too.
    if force_reprocess:
        if start_datetime and end_datetime:
            df_to_process = announcement_service.get_raw_announcements_by_window(
                start_dt=start_datetime, end_dt=end_datetime
            )
        else:
            day_start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
            day_end = target_date.replace(hour=23, minute=59, second=59, microsecond=999999)
            print(f"Full-day reprocess: {day_start} → {day_end}")
            df_to_process = announcement_service.get_raw_announcements_by_window(
                start_dt=day_start, end_dt=day_end
            )
        if df_to_process.empty:
            print("No announcements found in DB for reprocessing.")
            return pd.DataFrame()
        print(f"Processing {len(df_to_process)} announcements from DB (force=True).")
    elif inserted:
        df_to_process = pd.DataFrame(inserted)
        print(f"Processing {len(df_to_process)} newly inserted announcements.")
    else:
        print("No new announcements and force=False. Nothing to process.")
        return pd.DataFrame()

    # ---- Step 4: Merge with market cap CSV ----
    try:
        df_mcap = pd.read_csv(mcap_csv_path)
    except FileNotFoundError:
        print(f"WARNING: Market cap file not found at {mcap_csv_path}.")
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
    print(f"After market cap filter ({market_cap_start}–{market_cap_end} Cr): {len(df_filtered)} announcements.")

    if df_filtered.empty:
        return pd.DataFrame()

    # ---- Step 6: Filter by time window (ONLY if both start & end given) ----
    df_filtered["DT_TM"] = pd.to_datetime(df_filtered["DT_TM"], errors="coerce")

    if start_datetime and end_datetime:
        # Incremental mode — filter to the time window
        df_final = df_filtered[
            (df_filtered["DT_TM"] >= start_datetime) & (df_filtered["DT_TM"] <= end_datetime)
        ].copy()
        print(f"After time filter ({start_datetime} → {end_datetime}): {len(df_final)} announcements.")
    else:
        # Full-day mode — keep ALL announcements for the date (no time filter)
        df_final = df_filtered.copy()
        print(f"Full-day mode: keeping all {len(df_final)} announcements (no time filter).")

    if df_final.empty:
        return pd.DataFrame()

    # ---- Step 7: Build full PDF URLs ----
    df_final["ATTACHMENTNAME"] = df_final["ATTACHMENTNAME"].apply(
        lambda x: x if str(x).startswith("http") else
        "https://www.bseindia.com/xml-data/corpfiling/AttachLive/" + str(x)
    )

    if "News_submission_dt" in df_final.columns:
        df_final["News_submission_dt"] = pd.to_datetime(df_final["News_submission_dt"], errors="coerce")

    print(f"Returning {len(df_final)} filtered announcements for PDF download.")
    return df_final


if __name__ == "__main__":
    today = datetime.now()
    fetch_and_filter_announcements(target_date=today)
