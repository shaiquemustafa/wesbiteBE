from datetime import datetime
from bse import BSE
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import os

from service.announcement_service import AnnouncementService # Import the service

def fetch_and_filter_announcements(
    target_date: datetime,
    market_cap_start: int = 2500,
    market_cap_end: int = 25000,
    cut_off_time_str: str = "20:30:00",
    mcap_csv_path: str = "./assets/bse_market_cap_f5.csv",
    output_dir: str = "./bse_announcements"
) -> pd.DataFrame:
    """
    Fetches BSE announcements for a specific date, filters them by market cap and time,
    and returns a processed DataFrame.
    """
    #print(f"Storing raw announcements for in MongoDB...")

    announcement_service = AnnouncementService() # Instantiate the service

    os.makedirs(output_dir, exist_ok=True)
    date_str = target_date.strftime('%Y-%m-%d')

    # 1. Fetch Announcements
    # Increase timeout to 60 seconds to prevent request timeouts on slow responses
    b = BSE(download_folder="./bse_downloads")
    try:
        latest = b.announcements(page_no=1, from_date=target_date, to_date=target_date)
        total_rows = int(latest['Table1'][0]['ROWCNT'])
    except (IndexError, KeyError):
        #print(f"No announcements found for {date_str}.")
        return pd.DataFrame()

    rows_per_page = 50
    total_pages = math.ceil(total_rows / rows_per_page)
    all_rows = []

    #print(f"Fetching {total_rows} announcements across {total_pages} pages...")
    with ThreadPoolExecutor(max_workers=2) as executor:
        # Submit all page fetch tasks to the thread pool
        future_to_page = {
            executor.submit(b.announcements, page_no=p, from_date=target_date, to_date=target_date): p
            for p in range(1, total_pages + 1)
        }

        # Process results as they complete
        for i, future in enumerate(as_completed(future_to_page), 1):
            page_no = future_to_page[future]
            try:
                page_data = future.result()
                all_rows.extend(page_data.get("Table", []))
                #print(f"Processed page {i}/{total_pages} (Page #{page_no}), total rows so far: {len(all_rows)}")
            except Exception as exc:
                print(f"Page {page_no} generated an exception: {exc}")

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    # Store raw announcements in MongoDB using the create_filtered_announcements service method
    #print(f"Storing raw announcements for {date_str} in MongoDB...")
    inserted_announcements = announcement_service.create_announcements(df, f"raw_bse_announcements")
    #print(f"MongoDB: Inserted {len(inserted_announcements)} new raw announcements.")

    if not inserted_announcements:
        #print("No new announcements to filter.")
        return pd.DataFrame()

    # Use only the newly inserted announcements for further processing.
    df_new = pd.DataFrame(inserted_announcements)

    # 2. Filter by Market Cap and Time
    try:
        df_mcap = pd.read_csv(mcap_csv_path)
    except FileNotFoundError:
        #print(f"Error: Market cap file not found at {mcap_csv_path}")
        return df_new # Return unfiltered new data if mcap file is missing

    # Drop the 'Market Cap' column from df_new if it exists, to avoid merge conflicts.
    df_new = df_new.drop(columns=['Market Cap'], errors='ignore')

    df_merged = df_new.merge(df_mcap[["FinInstrmId", "Market Cap"]], left_on="SCRIP_CD", right_on="FinInstrmId", how="left")
    df_filtered_mcap = df_merged[(df_merged["Market Cap"] >= market_cap_start) & (df_merged["Market Cap"] <= market_cap_end)]

    cutoff_datetime = datetime.strptime(f"{date_str} {cut_off_time_str}", "%Y-%m-%d %H:%M:%S")
    df_filtered_mcap["DT_TM"] = pd.to_datetime(df_filtered_mcap["DT_TM"], errors='coerce')
    df_final = df_filtered_mcap[df_filtered_mcap["DT_TM"] > cutoff_datetime].copy()
    df_final['ATTACHMENTNAME'] = "https://www.bseindia.com/xml-data/corpfiling/AttachLive/" + df_final['ATTACHMENTNAME'].astype(str)

    #print(f"Returning {len(df_final)} filtered announcements.")
    return df_final

if __name__ == "__main__":
    # This block runs when the script is executed directly
    today = datetime.now()
    fetch_and_filter_announcements(target_date=today)