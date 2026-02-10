import os
from datetime import datetime

# Import the refactored functions from your other scripts
from announcements import fetch_and_filter_announcements
from data_to_pdf import download_announcement_pdfs
from results import analyze_and_rank_pdfs

if __name__ == "__main__":
    date_str = datetime.now().strftime('%Y-%m-%d')

    # 1. Fetch and filter announcements
    today = datetime.now()
    fetch_and_filter_announcements(target_date=today)

    # 2. Download the PDFs from the filtered list
    filtered_csv_path = f"./bse_announcements/filtered_announcements_{date_str}.csv"
    pdf_folder = f"./reports/reports_{date_str}"
    download_announcement_pdfs(input_csv_path=filtered_csv_path, output_pdf_dir=pdf_folder)

    # 3. Analyze the downloaded PDFs
    output_excel_file = f"./output/summary_price_jump_{date_str}.xlsx"
    os.makedirs(os.path.dirname(output_excel_file), exist_ok=True)
    analyze_and_rank_pdfs(pdf_folder_path=pdf_folder, output_file_path=output_excel_file)