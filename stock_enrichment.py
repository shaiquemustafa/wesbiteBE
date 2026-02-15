# stock_enrichment.py – Enrich predictions with Indian Stock API data
# API key is read from environment variable INDIAN_API_KEY (set on Render)
import os
import re
import requests
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger("uvicorn.error")

INDIAN_API_KEY = os.getenv("INDIAN_API_KEY", "")
INDIAN_API_BASE = "https://stock.indianapi.in"
HEADERS = {"X-Api-Key": INDIAN_API_KEY}
TIMEOUT = 15  # seconds per API call


def _clean_stock_name(name: str) -> str:
    """Remove common suffixes so the Indian API can find the stock."""
    name = re.sub(r"\s*(Limited|Ltd\.?|Pvt\.?|Private|Inc\.?|Corporation|Corp\.?)\s*$",
                  "", name, flags=re.IGNORECASE).strip()
    return name


def _last_n_quarters(data: dict, n: int = 6) -> dict:
    """Keep only the last N entries (quarters) from a dict keyed by 'Mon YYYY'."""
    items = list(data.items())
    return dict(items[-n:]) if len(items) > n else data


# ---------------------------------------------------------------------------
# Individual API fetchers
# ---------------------------------------------------------------------------
def _fetch_stock_info(stock_name: str) -> Optional[dict]:
    """GET /stock?name=... → BSE price, % change, 52w high/low, analyst consensus."""
    try:
        resp = requests.get(
            f"{INDIAN_API_BASE}/stock",
            params={"name": stock_name},
            headers=HEADERS,
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        # Analyst consensus
        analyst_view = data.get("analystView") or []
        consensus = {}
        for entry in analyst_view:
            rating = entry.get("ratingName", "").lower().replace(" ", "_")
            count = entry.get("numberOfAnalystsLatest", "0")
            try:
                consensus[rating] = int(float(count))
            except (ValueError, TypeError):
                consensus[rating] = 0

        return {
            "current_price_bse": (data.get("currentPrice") or {}).get("BSE"),
            "current_price_nse": (data.get("currentPrice") or {}).get("NSE"),
            "percent_change": data.get("percentChange"),
            "year_high": data.get("yearHigh"),
            "year_low": data.get("yearLow"),
            "analyst_consensus": consensus or None,
        }
    except Exception as e:
        logger.warning("Indian API /stock failed for '%s': %s", stock_name, e)
        return None


def _fetch_target_price(stock_name: str) -> Optional[dict]:
    """GET /stock_target_price?stock_id=... → analyst target price."""
    try:
        resp = requests.get(
            f"{INDIAN_API_BASE}/stock_target_price",
            params={"stock_id": stock_name},
            headers=HEADERS,
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        pt = data.get("priceTarget")
        if not pt or pt.get("Mean") is None:
            return None

        return {
            "mean": pt.get("Mean"),
            "median": pt.get("Median"),
            "high": pt.get("High"),
            "low": pt.get("Low"),
            "number_of_estimates": pt.get("NumberOfEstimates"),
        }
    except Exception as e:
        logger.warning("Indian API /stock_target_price failed for '%s': %s", stock_name, e)
        return None


def _fetch_quarterly_results(stock_name: str, quarters: int = 6) -> Optional[dict]:
    """
    GET /historical_stats?stock_name=...&stats=quarter_results
    Returns dict with keys like Sales, Expenses, etc. — each trimmed to last N quarters.
    """
    try:
        resp = requests.get(
            f"{INDIAN_API_BASE}/historical_stats",
            params={"stock_name": stock_name, "stats": "quarter_results"},
            headers=HEADERS,
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        if isinstance(data, list):
            # API returned an error list
            return None

        # Trim each metric to last N quarters
        trimmed = {}
        for metric, values in data.items():
            if isinstance(values, dict):
                trimmed[metric] = _last_n_quarters(values, quarters)

        return trimmed if trimmed else None
    except Exception as e:
        logger.warning("Indian API /historical_stats failed for '%s': %s", stock_name, e)
        return None


# ---------------------------------------------------------------------------
# Main enrichment function
# ---------------------------------------------------------------------------
def enrich_prediction(prediction: dict) -> dict:
    """
    Takes a single prediction dict and enriches it with Indian Stock API data:
      - BSE/NSE price, % change, 52-week high/low
      - Analyst consensus (Strong Buy / Buy / Hold / Sell / Strong Sell)
      - Target price (mean, high, low)
      - Quarterly results (last 6 quarters)

    Returns a new dict ready for ui_data storage.
    Gracefully handles API failures (fields will be None).
    """
    if not INDIAN_API_KEY:
        logger.warning("INDIAN_API_KEY not set — skipping stock enrichment.")
        return prediction

    company = prediction.get("Company") or prediction.get("company_name") or ""
    clean_name = _clean_stock_name(company)

    if not clean_name:
        return prediction

    logger.info("  Enriching '%s' ...", clean_name)

    nse_symbol = prediction.get("NSE_Symbol") or ""

    enriched = {
        # ---- From prediction ----
        "scrip_cd": str(prediction.get("SCRIP_CD", "")),
        "company_name": company,
        "nse_symbol": nse_symbol or None,
        "impact": prediction.get("Impact"),
        "impact_score": prediction.get("Impact_Score"),
        "mid_percentage": prediction.get("Mid_%"),
        "summary": prediction.get("Summary"),
        "rationale": prediction.get("Rationale"),
        "price_range": prediction.get("Price_Range"),
        "category": prediction.get("Category"),
        "pdf_link": prediction.get("PDF_Link"),
        "rank": prediction.get("Rank"),
        "news_time": prediction.get("News_submission_dt"),
    }

    # ---- Stock info (price + analyst consensus) ----
    stock_info = _fetch_stock_info(clean_name)
    if stock_info:
        enriched["current_price_bse"] = stock_info.get("current_price_bse")
        enriched["current_price_nse"] = stock_info.get("current_price_nse")
        enriched["percent_change"] = stock_info.get("percent_change")
        enriched["year_high"] = stock_info.get("year_high")
        enriched["year_low"] = stock_info.get("year_low")
        enriched["analyst_consensus"] = stock_info.get("analyst_consensus")
    else:
        enriched.update({
            "current_price_bse": None, "current_price_nse": None,
            "percent_change": None, "year_high": None, "year_low": None,
            "analyst_consensus": None,
        })

    # ---- Target price ----
    target = _fetch_target_price(clean_name)
    if target:
        enriched["target_price_mean"] = target.get("mean")
        enriched["target_price_high"] = target.get("high")
        enriched["target_price_low"] = target.get("low")
        enriched["number_of_estimates"] = target.get("number_of_estimates")
    else:
        enriched.update({
            "target_price_mean": None, "target_price_high": None,
            "target_price_low": None, "number_of_estimates": None,
        })

    # ---- Quarterly results (last 6 quarters) ----
    quarterly = _fetch_quarterly_results(clean_name, quarters=6)
    if quarterly:
        # Store as structured sub-fields for easy frontend consumption
        enriched["sales"] = quarterly.get("Sales")
        enriched["expenses"] = quarterly.get("Expenses")
        enriched["operating_profit"] = quarterly.get("Operating Profit")
        enriched["opm_percent"] = quarterly.get("OPM %")
        enriched["other_income"] = quarterly.get("Other Income")
        enriched["interest"] = quarterly.get("Interest")
        enriched["depreciation"] = quarterly.get("Depreciation")
        enriched["profit_before_tax"] = quarterly.get("Profit before tax")
        enriched["tax_percent"] = quarterly.get("Tax %")
        enriched["net_profit"] = quarterly.get("Net Profit")
        enriched["eps_in_rs"] = quarterly.get("EPS in Rs")
    else:
        for field in ["sales", "expenses", "operating_profit", "opm_percent",
                       "other_income", "interest", "depreciation",
                       "profit_before_tax", "tax_percent", "net_profit", "eps_in_rs"]:
            enriched[field] = None

    enriched["enrichment_time"] = datetime.utcnow().isoformat()

    return enriched
