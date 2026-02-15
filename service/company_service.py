# service/company_service.py – Manages the company_master table
import logging
import re
import requests
import pandas as pd
from typing import Optional
from psycopg2.extras import execute_values

from database import get_conn

logger = logging.getLogger("uvicorn.error")

BSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.bseindia.com/",
}


class CompanyService:
    """Service class for company_master table operations."""

    # ------------------------------------------------------------------
    # Bulk load from Excel / DataFrame
    # ------------------------------------------------------------------
    def bulk_load_from_dataframe(self, df: pd.DataFrame) -> int:
        """
        Loads companies from a DataFrame with columns:
        ISIN, Company Name, BSE_Scrip_Code, NSE_Symbol, MktCapFull
        Returns number of rows upserted.
        """
        rows = []
        for _, r in df.iterrows():
            bse_code = r.get("BSE_Scrip_Code")
            if pd.isna(bse_code):
                continue
            bse_code = int(bse_code)

            isin = r.get("ISIN")
            isin = str(isin) if pd.notna(isin) else None

            company_name = str(r.get("Company Name", "")).strip()
            if not company_name:
                continue

            nse_symbol = r.get("NSE_Symbol")
            nse_symbol = str(nse_symbol).strip() if pd.notna(nse_symbol) and str(nse_symbol).strip() else None

            mkt_cap = r.get("MktCapFull")
            mkt_cap = float(mkt_cap) if pd.notna(mkt_cap) else None

            rows.append((bse_code, isin, company_name, nse_symbol, mkt_cap))

        if not rows:
            return 0

        with get_conn() as conn:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO company_master (bse_scrip_code, isin, company_name, nse_symbol, mkt_cap_full)
                    VALUES %s
                    ON CONFLICT (bse_scrip_code) DO UPDATE SET
                        isin = EXCLUDED.isin,
                        company_name = EXCLUDED.company_name,
                        nse_symbol = COALESCE(EXCLUDED.nse_symbol, company_master.nse_symbol),
                        mkt_cap_full = EXCLUDED.mkt_cap_full,
                        updated_at = NOW()
                    """,
                    rows,
                    page_size=500,
                )
                return len(rows)

    # ------------------------------------------------------------------
    # Lookup market cap for a list of BSE scrip codes
    # ------------------------------------------------------------------
    def get_market_caps(self, scrip_codes: list[int]) -> dict[int, float]:
        """
        Returns {bse_scrip_code: mkt_cap_full} for all codes found in company_master.
        Missing codes are simply absent from the dict.
        """
        if not scrip_codes:
            return {}

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT bse_scrip_code, mkt_cap_full
                    FROM company_master
                    WHERE bse_scrip_code = ANY(%s)
                      AND mkt_cap_full IS NOT NULL
                    """,
                    (scrip_codes,),
                )
                return {row[0]: row[1] for row in cur.fetchall()}

    # ------------------------------------------------------------------
    # Lookup company info for a single BSE scrip code
    # ------------------------------------------------------------------
    def get_company(self, bse_scrip_code: int) -> Optional[dict]:
        """Returns company info dict or None."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT bse_scrip_code, isin, company_name, nse_symbol, mkt_cap_full
                    FROM company_master
                    WHERE bse_scrip_code = %s
                    """,
                    (bse_scrip_code,),
                )
                row = cur.fetchone()
        if not row:
            return None
        return {
            "bse_scrip_code": row[0],
            "isin": row[1],
            "company_name": row[2],
            "nse_symbol": row[3],
            "mkt_cap_full": row[4],
        }

    # ------------------------------------------------------------------
    # Fetch market cap from BSE API for unknown scrip codes
    # ------------------------------------------------------------------
    def fetch_mcap_from_bse_api(self, scrip_code: int) -> Optional[float]:
        """
        Calls BSE API to get MktCapFull for a given scrip code.
        Returns market cap in Crores (float) or None on failure.
        """
        url = f"https://api.bseindia.com/BseIndiaAPI/api/StockTrading/w?flag=&scripcode={scrip_code}"
        try:
            session = requests.Session()
            resp = session.get(url, headers=BSE_HEADERS, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            mcap_str = data.get("MktCapFull", "")
            if not mcap_str or mcap_str == "":
                return None

            # Remove commas: "9,74,043.43" → "974043.43"
            mcap_clean = mcap_str.replace(",", "")
            return float(mcap_clean)
        except Exception as e:
            logger.warning("  BSE API MktCap fetch failed for %s: %s", scrip_code, e)
            return None

    # ------------------------------------------------------------------
    # Upsert a single company (for newly discovered scrip codes)
    # ------------------------------------------------------------------
    def upsert_company(
        self,
        bse_scrip_code: int,
        company_name: str = "",
        nse_symbol: Optional[str] = None,
        mkt_cap_full: Optional[float] = None,
        isin: Optional[str] = None,
    ):
        """Insert or update a single company in company_master."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO company_master (bse_scrip_code, isin, company_name, nse_symbol, mkt_cap_full)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (bse_scrip_code) DO UPDATE SET
                        company_name = CASE WHEN EXCLUDED.company_name != '' THEN EXCLUDED.company_name
                                            ELSE company_master.company_name END,
                        nse_symbol = COALESCE(EXCLUDED.nse_symbol, company_master.nse_symbol),
                        mkt_cap_full = COALESCE(EXCLUDED.mkt_cap_full, company_master.mkt_cap_full),
                        isin = COALESCE(EXCLUDED.isin, company_master.isin),
                        updated_at = NOW()
                    """,
                    (bse_scrip_code, isin, company_name, nse_symbol, mkt_cap_full),
                )

    # ------------------------------------------------------------------
    # Update NSE symbol for a company
    # ------------------------------------------------------------------
    def update_nse_symbol(self, bse_scrip_code: int, nse_symbol: str):
        """Set the NSE symbol for a company (only if currently NULL)."""
        if not nse_symbol:
            return
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE company_master
                    SET nse_symbol = %s, updated_at = NOW()
                    WHERE bse_scrip_code = %s
                      AND nse_symbol IS NULL
                    """,
                    (nse_symbol, bse_scrip_code),
                )

    # ------------------------------------------------------------------
    # Get total count
    # ------------------------------------------------------------------
    def get_count(self) -> int:
        """Returns total number of companies in company_master."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM company_master")
                return cur.fetchone()[0]
