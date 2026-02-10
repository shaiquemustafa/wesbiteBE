import os
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Union, Optional # Using Union or Optional fixes the error


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.bseindia.com/",  # <- *critical*
}

def _download_pdf_to_memory(url: str) -> Union[bytes, None]:
    """Downloads a single PDF into a bytes buffer."""
    try:
        with requests.get(url, headers=HEADERS, stream=True, timeout=300, verify=False) as r:
            r.raise_for_status()  # 4xx/5xx → exception
            return r.content
    except Exception as exc:
        #print(f"FAIL: {url} ({exc})")
        return None

def download_pdfs_to_dataframe(announcements_df: pd.DataFrame) -> pd.DataFrame:
    """
    Downloads all PDF announcements from a DataFrame into a new 'pdf_content' column.
    """
    # Create a list of tuples (index, url) for valid rows
    download_tasks = []
    for index, row in announcements_df.iterrows():
        url = row.get("ATTACHMENTNAME")
        if pd.notnull(url) and str(url).startswith("http"):
            download_tasks.append((index, url))

    if not download_tasks:
        #print("No valid URLs found in the DataFrame.")
        announcements_df['pdf_content'] = None
        return announcements_df

    # Limit workers to a low number suitable for a free-tier EC2 instance.
    num_workers = 2
    #print(f"Starting PDF download with {num_workers} workers for {len(download_tasks)} URLs.")

    # Use a dictionary to store results, keyed by the DataFrame index.
    # This avoids issues with non-sequential or non-zero-based indices.
    pdf_contents_map = {}
    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        future_to_task = {pool.submit(_download_pdf_to_memory, url): (index, url) for index, url in download_tasks}
        for i, fut in enumerate(as_completed(future_to_task), 1):
            index, url = future_to_task[fut]
            pdf_bytes = fut.result()
            if pdf_bytes:
                pdf_contents_map[index] = pdf_bytes
            #print(f"{i}/{len(download_tasks)} → Downloaded {url}")
    
    # Map the downloaded content back to the DataFrame using its index.
    announcements_df['pdf_content'] = announcements_df.index.map(pdf_contents_map)
    return announcements_df.dropna(subset=['pdf_content'])
