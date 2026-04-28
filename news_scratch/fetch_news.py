"""
Pull raw news from a wide set of Indian financial RSS feeds and dump to Excel.

No LLM, no filtering, no company matching — just the raw RSS items so we can
eyeball coverage, freshness and signal quality.

Run:
    cd news_scratch && .venv/bin/python fetch_news.py

Output:
    news_scratch/news_today.xlsx   (sheets: all_news, today_only, source_counts)
    news_scratch/news_today.csv    (same as all_news)
"""

from __future__ import annotations

import html
import io
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

# Trust macOS / system keychain roots. Needed when an SSL-intercepting
# proxy or AV injects a self-signed root that isn't in certifi's bundle.
try:
    import truststore  # type: ignore
    truststore.inject_into_ssl()
except Exception:
    pass

import feedparser
import pandas as pd
import requests
from dateutil import parser as dateparser

IST = timezone(timedelta(hours=5, minutes=30))

# A single browser-ish UA. Some publishers (BS, Reuters) 403 non-browser UAs.
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

REQUEST_TIMEOUT = 20
MAX_WORKERS = 12

# (source_name, url)
# Kept broad on purpose — partnerships / industry moves / deals that don't hit
# BSE filings often show up in one of these within minutes.
FEEDS: List[tuple[str, str]] = [
    # ---- Mint (Livemint) ----
    ("Mint - Top",              "https://www.livemint.com/rss/news"),
    ("Mint - Markets",          "https://www.livemint.com/rss/markets"),
    ("Mint - Companies",        "https://www.livemint.com/rss/companies"),
    ("Mint - Money",            "https://www.livemint.com/rss/money"),
    ("Mint - Industry",         "https://www.livemint.com/rss/industry"),
    ("Mint - Economy",          "https://www.livemint.com/rss/economy"),

    # ---- Moneycontrol (direct feeds are Akamai-blocked from most IPs →
    # proxy through Google News site-scoped search) ----
    ("Moneycontrol (via GN)",   "https://news.google.com/rss/search?q=site:moneycontrol.com+when:1d&hl=en-IN&gl=IN&ceid=IN:en"),

    # ---- Economic Times ----
    ("ET - Markets",            "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"),
    ("ET - Stocks",             "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms"),
    ("ET - Industry",           "https://economictimes.indiatimes.com/industry/rssfeeds/13352306.cms"),
    ("ET - Economy",            "https://economictimes.indiatimes.com/news/economy/rssfeeds/1373380680.cms"),
    ("ET - Earnings",           "https://economictimes.indiatimes.com/markets/earnings/rssfeeds/13357270.cms"),
    ("ET - Company",            "https://economictimes.indiatimes.com/industry/rssfeeds/13352306.cms"),

    # ---- Business Standard ----
    ("BS - Markets",            "https://www.business-standard.com/rss/markets-106.rss"),
    ("BS - Companies",          "https://www.business-standard.com/rss/companies-101.rss"),
    ("BS - Economy",            "https://www.business-standard.com/rss/economy-102.rss"),
    ("BS - Finance",            "https://www.business-standard.com/rss/finance-103.rss"),
    ("BS - Latest",             "https://www.business-standard.com/rss/latest.rss"),

    # ---- Hindu BusinessLine ----
    ("BL - Markets",            "https://www.thehindubusinessline.com/markets/feeder/default.rss"),
    ("BL - Companies",          "https://www.thehindubusinessline.com/companies/feeder/default.rss"),
    ("BL - Economy",            "https://www.thehindubusinessline.com/economy/feeder/default.rss"),
    ("BL - Stocks",             "https://www.thehindubusinessline.com/markets/stock-markets/feeder/default.rss"),

    # ---- Financial Express (WordPress feeds disabled site-wide → Google News) ----
    ("Financial Express (GN)",  "https://news.google.com/rss/search?q=site:financialexpress.com+when:1d&hl=en-IN&gl=IN&ceid=IN:en"),

    # ---- NDTV Profit / former BQ Prime (Akamai-blocked → Google News) ----
    ("NDTV Profit (via GN)",    "https://news.google.com/rss/search?q=site:ndtvprofit.com+when:1d&hl=en-IN&gl=IN&ceid=IN:en"),

    # ---- CNBC TV18 ----
    ("CNBC TV18 - Market",      "https://www.cnbctv18.com/commonfeeds/v1/cne/rss/market.xml"),
    ("CNBC TV18 - Business",    "https://www.cnbctv18.com/commonfeeds/v1/cne/rss/business.xml"),
    ("CNBC TV18 - Economy",     "https://www.cnbctv18.com/commonfeeds/v1/cne/rss/economy.xml"),

    # ---- Business Today ----
    ("Business Today - Latest", "https://www.businesstoday.in/rssfeeds/?id=home"),
    ("Business Today - Markets","https://www.businesstoday.in/rssfeeds/?id=markets"),

    # ---- Zee Business (Akamai-blocked → Google News) ----
    ("Zee Business (via GN)",   "https://news.google.com/rss/search?q=site:zeebiz.com+when:1d&hl=en-IN&gl=IN&ceid=IN:en"),

    # ---- Other Indian business via Google News site-scoped searches ----
    ("BQ Prime (via GN)",       "https://news.google.com/rss/search?q=site:bqprime.com+when:1d&hl=en-IN&gl=IN&ceid=IN:en"),
    ("Outlook Business (GN)",   "https://news.google.com/rss/search?q=site:outlookbusiness.com+when:1d&hl=en-IN&gl=IN&ceid=IN:en"),
    ("Forbes India (GN)",       "https://news.google.com/rss/search?q=site:forbesindia.com+when:1d&hl=en-IN&gl=IN&ceid=IN:en"),
    ("The Ken (GN)",            "https://news.google.com/rss/search?q=site:the-ken.com+when:1d&hl=en-IN&gl=IN&ceid=IN:en"),
    ("Entrackr (GN)",           "https://news.google.com/rss/search?q=site:entrackr.com+when:1d&hl=en-IN&gl=IN&ceid=IN:en"),
    ("Inc42 (GN)",              "https://news.google.com/rss/search?q=site:inc42.com+when:1d&hl=en-IN&gl=IN&ceid=IN:en"),
    ("YourStory (GN)",          "https://news.google.com/rss/search?q=site:yourstory.com+when:1d&hl=en-IN&gl=IN&ceid=IN:en"),
    ("Reuters India (GN)",      "https://news.google.com/rss/search?q=site:reuters.com+India+when:1d&hl=en-IN&gl=IN&ceid=IN:en"),
    ("Bloomberg India (GN)",    "https://news.google.com/rss/search?q=site:bloomberg.com+India+when:1d&hl=en-IN&gl=IN&ceid=IN:en"),

    # ---- Reuters India (via Google News proxy — official Reuters RSS was deprecated) ----
    ("Google News - India biz", "https://news.google.com/rss/search?q=India+business+when:1d&hl=en-IN&gl=IN&ceid=IN:en"),
    ("Google News - Nifty",     "https://news.google.com/rss/search?q=Nifty+OR+Sensex+OR+%22Indian+stocks%22+when:1d&hl=en-IN&gl=IN&ceid=IN:en"),
    ("Google News - Deals IN",  "https://news.google.com/rss/search?q=%22India%22+%28acquires+OR+merger+OR+partnership+OR+stake%29+when:1d&hl=en-IN&gl=IN&ceid=IN:en"),
]


def _clean(text: Any) -> str:
    if text is None:
        return ""
    s = str(text)
    # strip HTML tags
    s = re.sub(r"<[^>]+>", " ", s)
    # unescape entities
    s = html.unescape(s)
    # collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _parse_dt(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    try:
        dt = dateparser.parse(raw)
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(IST)
    except Exception:
        return None


def fetch_one(source: str, url: str) -> List[Dict[str, Any]]:
    """Fetch + parse a single feed. Returns a list of row dicts (may be empty)."""
    started = time.time()
    status = "ok"
    rows: List[Dict[str, Any]] = []
    try:
        resp = requests.get(
            url,
            headers={
                "User-Agent": UA,
                "Accept": "application/rss+xml, application/xml, text/xml, */*;q=0.8",
                "Accept-Language": "en-IN,en;q=0.9",
                "Referer": "https://www.google.com/",
            },
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        parsed = feedparser.parse(io.BytesIO(resp.content))
        for entry in parsed.entries:
            raw_pub = (
                entry.get("published")
                or entry.get("updated")
                or entry.get("pubDate")
                or ""
            )
            dt_ist = _parse_dt(raw_pub)
            categories = ", ".join(
                _clean(t.get("term")) for t in entry.get("tags", []) if t.get("term")
            )
            rows.append({
                "source": source,
                "published_at_ist": dt_ist,
                "title": _clean(entry.get("title")),
                "summary": _clean(
                    entry.get("summary") or entry.get("description") or ""
                ),
                "link": entry.get("link") or "",
                "guid": entry.get("id") or entry.get("guid") or entry.get("link") or "",
                "categories": categories,
                "author": _clean(entry.get("author") or ""),
                "raw_pubdate": raw_pub,
            })
    except requests.HTTPError as e:
        status = f"http_{e.response.status_code if e.response is not None else '?'}"
    except requests.RequestException as e:
        status = f"req_err:{type(e).__name__}"
    except Exception as e:
        status = f"parse_err:{type(e).__name__}:{e}"
    elapsed = time.time() - started
    print(f"  [{status:>14}]  {elapsed:5.2f}s  items={len(rows):>3}  {source}")
    return rows


def fetch_all() -> pd.DataFrame:
    all_rows: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futs = {pool.submit(fetch_one, s, u): (s, u) for s, u in FEEDS}
        for fut in as_completed(futs):
            try:
                all_rows.extend(fut.result())
            except Exception as e:
                s, _ = futs[fut]
                print(f"  [FATAL]       0.00s  items=  0  {s}  ({e})")
    df = pd.DataFrame(all_rows)
    if df.empty:
        return df
    df = df.drop_duplicates(subset=["source", "link"], keep="first")
    df = df.sort_values("published_at_ist", ascending=False, na_position="last")
    return df.reset_index(drop=True)


def main() -> int:
    out_dir = Path(__file__).resolve().parent
    xlsx_path = out_dir / "news_today.xlsx"
    csv_path = out_dir / "news_today.csv"

    print(f"Fetching {len(FEEDS)} feeds with UA={UA[:40]}…\n")
    t0 = time.time()
    df = fetch_all()
    total = time.time() - t0

    if df.empty:
        print("\nNo rows fetched — check network / UA.")
        return 1

    now_ist = datetime.now(IST)
    today = now_ist.date()
    today_df = df[df["published_at_ist"].apply(
        lambda d: isinstance(d, datetime) and d.date() == today
    )].copy()

    counts = (
        df.groupby("source").size()
          .reset_index(name="items_total")
          .sort_values("items_total", ascending=False)
    )
    today_counts = (
        today_df.groupby("source").size()
          .reset_index(name="items_today")
          .sort_values("items_today", ascending=False)
    )
    source_counts = counts.merge(today_counts, on="source", how="left").fillna({"items_today": 0})
    source_counts["items_today"] = source_counts["items_today"].astype(int)

    # Excel is happier with tz-naive datetimes
    for frame in (df, today_df):
        if "published_at_ist" in frame.columns:
            frame["published_at_ist"] = frame["published_at_ist"].apply(
                lambda d: d.replace(tzinfo=None) if isinstance(d, datetime) else d
            )

    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as xw:
        df.to_excel(xw, sheet_name="all_news", index=False)
        today_df.to_excel(xw, sheet_name="today_only", index=False)
        source_counts.to_excel(xw, sheet_name="source_counts", index=False)

    df.to_csv(csv_path, index=False)

    print("\n" + "=" * 60)
    print(f"Total items        : {len(df):>5}")
    print(f"Items today (IST)  : {len(today_df):>5}  (today = {today})")
    print(f"Sources returning >0: {int((counts['items_total'] > 0).sum())} / {len(FEEDS)}")
    print(f"Elapsed             : {total:.1f}s")
    print(f"\nWrote:\n  {xlsx_path}\n  {csv_path}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
