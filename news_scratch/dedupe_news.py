"""
Filter news_today.xlsx to a single date (IST) and deduplicate:

  1. URL normalization + exact URL dedup.
  2. Fuzzy title clustering across sources (same story on Mint / ET / BS /
     Moneycontrol-via-GN etc. collapses into one).
  3. Cluster winner picked by (source tier, summary richness, earliest seen).

Output: news_22apr.xlsx with three sheets:
  - deduped       one row per story, best version
  - clusters      every row with its cluster_id and whether it was the winner
  - source_counts per-source totals on the filtered date + survivors after dedup

Run:
  cd news_scratch && .venv/bin/python dedupe_news.py --date 2026-04-22

Optional IST window (naive IST wall-clock; inclusive bounds):
  .venv/bin/python dedupe_news.py \\
      --ist-window-start "2026-04-26 22:00" --ist-window-end "2026-04-27 08:30" \\
      --date 2026-04-27 --output news_27apr_pre.xlsx

When both --ist-window-start and --ist-window-end are set, rows are kept
where published_at_ist is in [start, end] (inclusive). --date is still used
for logging and default output naming when --output is omitted.

Defaults to yesterday IST if --date is omitted (full-day filter).
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime, date, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

import pandas as pd
from rapidfuzz import fuzz

IST = timezone(timedelta(hours=5, minutes=30))
HERE = Path(__file__).resolve().parent

# ---------- Source quality tiers --------------------------------------------
# Higher tier number == better. When two rows describe the same story,
# the one with the higher tier wins.
def source_tier(src: str) -> int:
    s = src.lower()
    if s.startswith(("mint -", "et -", "bs -", "bl -")):
        return 5                              # direct RSS from a tier-1 business paper
    if s.startswith(("cnbc tv18", "business today")):
        return 4                              # direct RSS, tier-2
    if "(via gn)" in s or "(gn)" in s:
        # publisher-specific Google-News proxy (Moneycontrol, NDTV Profit, …).
        # Usually headline-only but we know which publisher it came from.
        return 3
    if s.startswith("google news -"):
        # Generic GN topical searches. Highest volume, lowest specificity.
        return 1
    return 2


# ---------- URL normalization -----------------------------------------------
_TRACKING_PREFIXES = ("utm_", "fbclid", "gclid", "mc_", "ref_", "ref=")
_TRACKING_EXACT = {"amp", "referrer", "ref", "_ga"}

def normalize_url(url: str) -> str:
    if not url:
        return ""
    try:
        p = urlparse(url.strip())
        host = (p.netloc or "").lower().lstrip("www.")
        # Google News article URLs — keep as-is, they are unique tokens.
        if "news.google.com" in host:
            return url.strip()
        qs = [
            (k, v) for k, v in parse_qsl(p.query, keep_blank_values=False)
            if not any(k.lower().startswith(pfx) for pfx in _TRACKING_PREFIXES)
            and k.lower() not in _TRACKING_EXACT
        ]
        path = p.path.rstrip("/")
        return urlunparse((p.scheme.lower(), host, path, "", urlencode(qs), ""))
    except Exception:
        return url.strip()


# ---------- Title normalization ---------------------------------------------
# Google News appends "<space>-<space>Publisher" to every title.
_GN_SUFFIX = re.compile(r"\s+[-–|]\s+[^-–|]{2,60}$")
_NONWORD = re.compile(r"[^a-z0-9₹%.\s]")
_SPACES = re.compile(r"\s+")
_STOPWORDS = {
    "the", "a", "an", "of", "to", "in", "on", "at", "for", "and", "or",
    "is", "are", "was", "were", "be", "by", "with", "from", "as", "its",
    "this", "that", "these", "those", "it", "has", "have", "had", "will",
    "says", "said",
}

def normalize_title(title: str) -> str:
    if not title:
        return ""
    t = title.strip()
    # drop trailing "- Source" that Google News adds
    t = _GN_SUFFIX.sub("", t)
    t = t.lower()
    t = _NONWORD.sub(" ", t)
    t = _SPACES.sub(" ", t).strip()
    tokens = [w for w in t.split() if w not in _STOPWORDS and len(w) > 1]
    return " ".join(tokens)


# ---------- Junk / placeholder headlines we drop before clustering ----------
# These are boilerplate RSS rows that contain no actual news: publisher
# homepage titles, evergreen SEO pages, and per-ticker "stock price history"
# templates that Economic Times emits for every Nifty-50 stock daily.
_JUNK_PATTERNS = [
    re.compile(r"^\s*[-–|]\s*[^-–|]{2,60}\s*$"),                              # just " - SourceName"
    re.compile(r"^\s*$"),                                                     # empty
    re.compile(r"business\s+news.*economic\s+news", re.I),                    # MC / homepage blob
    re.compile(r"read\s+latest\s+business\s+and\s+finance\s+news", re.I),     # Outlook homepage
    re.compile(r"share\s+price\s+highlights?.*stock\s+price\s+history", re.I),# ET per-ticker templates
    re.compile(r"^(gold|silver)\s+rate(s)?\s+today", re.I),                   # daily gold/silver pages
    re.compile(r"^(top|best)\s+\w+(\s+\w+)?\s+funds?\b", re.I),               # evergreen MF listicles
    re.compile(r"quote\s+of\s+the\s+day", re.I),                              # Mint evergreen column
    re.compile(r"horoscope|rashifal|zodiac", re.I),
    re.compile(r"^live\s+updates?[:\s]|^market\s+live[:\s]|^stock\s+market\s+highlights?[:\s]", re.I),
    re.compile(r"\bnet\s+worth\b.*\bbiography\b", re.I),                      # billionaire profile pages
    re.compile(r"\bbiography\b.*\bnet\s+worth\b", re.I),
    re.compile(r"\bprice\s+in\s+india\b.*moneycontrol", re.I),                # MC crypto/commodity price pages
    re.compile(r"^\s*page\s+\d+\s+of\b", re.I),                               # Reuters pagination titles
    re.compile(r"^[A-Z][a-z]+\s+[A-Z][a-z]+\s*(?:[-–|]\s*bloomberg\.com)\s*$"),# Bloomberg author pages "First Last - Bloomberg.com"
    re.compile(r"^visual\s+stor(y|ies)\b", re.I),
    re.compile(r"recipe|horoscope|astrology", re.I),
]

def is_junk_title(title: str) -> bool:
    if not title or not title.strip():
        return True
    for p in _JUNK_PATTERNS:
        if p.search(title):
            return True
    return False


# ---------- Fuzzy clustering ------------------------------------------------
def cluster_titles(rows: List[Dict[str, Any]], threshold: int = 85) -> List[int]:
    """
    Greedy clustering:
      - rows are assumed pre-sorted (highest quality first)
      - each cluster's canonical title is the title of its first (best) member
      - a row joins a cluster only when BOTH:
          token_set_ratio  >= threshold       (content overlap)
          token_sort_ratio >= threshold - 15  (order/length sanity)
        This avoids merging different stocks that share the same boilerplate
        template (e.g. "<X> Share Price Highlights: <X> Stock Price History").
    Returns list of cluster_ids aligned to rows.
    """
    canon: List[str] = []
    canon_tokens: List[set] = []
    ids: List[int] = []
    sort_threshold = max(threshold - 15, 60)

    for r in rows:
        t = r["_norm_title"]
        tokens = set(t.split())
        placed = False
        if t and tokens:
            for cid, (c, c_tokens) in enumerate(zip(canon, canon_tokens)):
                if not c:
                    continue
                # cheap prefilter: require at least one shared meaningful token
                # (otherwise two totally different stories with similar
                # boilerplate can still sneak past token_set_ratio).
                shared = tokens & c_tokens
                if len(shared) < 2:
                    continue
                set_score = fuzz.token_set_ratio(t, c)
                if set_score < threshold:
                    continue
                sort_score = fuzz.token_sort_ratio(t, c)
                if sort_score < sort_threshold:
                    continue
                ids.append(cid)
                placed = True
                break
        if not placed:
            ids.append(len(canon))
            canon.append(t)
            canon_tokens.append(tokens)
    return ids


# ---------- Row quality score (for ordering before clustering) --------------
def row_score(r: Dict[str, Any]) -> Tuple[int, int, int, float]:
    """
    Higher tuple == better. Used to sort rows so the clustering loop
    encounters the "best candidate" first for each story.
    """
    summary_len = len(str(r.get("summary") or ""))
    has_summary = 1 if summary_len >= 40 else 0
    tier = source_tier(str(r.get("source") or ""))
    # Earlier pub time preferred (breaks scoop ties). Convert to negative epoch.
    ts = r.get("published_at_ist")
    if isinstance(ts, datetime):
        epoch = -ts.timestamp()  # earlier = larger number
    else:
        epoch = 0.0
    return (tier, has_summary, summary_len, epoch)


# ---------- Main ------------------------------------------------------------
def _parse_ist_naive_window(s: str) -> datetime:
    """Parse 'YYYY-MM-DD HH:MM' as naive IST wall time."""
    s = (s or "").strip()
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"Invalid IST window datetime: {s!r} (use YYYY-MM-DD HH:MM)")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    default_date = (datetime.now(IST) - timedelta(days=1)).date().isoformat()
    ap.add_argument("--date", default=default_date,
                    help="IST calendar date label YYYY-MM-DD (default: yesterday IST)")
    ap.add_argument("--input", default=str(HERE / "news_today.xlsx"),
                    help="Input Excel produced by fetch_news.py")
    ap.add_argument("--output", default=None,
                    help="Output Excel (default: news_<ddMMM>.xlsx)")
    ap.add_argument("--ist-window-start", default=None,
                    help="Inclusive naive IST lower bound YYYY-MM-DD HH:MM")
    ap.add_argument("--ist-window-end", default=None,
                    help="Inclusive naive IST upper bound YYYY-MM-DD HH:MM")
    ap.add_argument("--threshold", type=int, default=85,
                    help="rapidfuzz token_set_ratio threshold for clustering")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    target_date = date.fromisoformat(args.date)

    in_path = Path(args.input)
    if not in_path.exists():
        print(f"ERROR: {in_path} not found. Run fetch_news.py first.", file=sys.stderr)
        return 1

    out_path = Path(args.output) if args.output else HERE / (
        f"news_{target_date.strftime('%d%b').lower()}.xlsx"
    )

    print(f"Input  : {in_path}")
    print(f"Date   : {target_date.isoformat()} (IST)")
    print(f"Output : {out_path}\n")

    df = pd.read_excel(in_path, sheet_name="all_news")
    total_rows = len(df)

    df["published_at_ist"] = pd.to_datetime(df["published_at_ist"], errors="coerce")

    ws_raw = getattr(args, "ist_window_start", None)
    we_raw = getattr(args, "ist_window_end", None)
    if (ws_raw and not we_raw) or (we_raw and not ws_raw):
        print("ERROR: pass both --ist-window-start and --ist-window-end, or neither.",
              file=sys.stderr)
        return 1

    if ws_raw and we_raw:
        win_start = _parse_ist_naive_window(ws_raw)
        win_end = _parse_ist_naive_window(we_raw)
        if win_end < win_start:
            print("ERROR: ist-window-end must be >= ist-window-start", file=sys.stderr)
            return 1
        print(f"IST window (inclusive): {win_start}  →  {win_end}")

        def _in_win(d) -> bool:
            if pd.isna(d) or not isinstance(d, (datetime, pd.Timestamp)):
                return False
            if isinstance(d, pd.Timestamp):
                d = d.to_pydatetime()
            # Compare as naive wall times (fetch_news stores IST-naive)
            if d.tzinfo is not None:
                d = d.replace(tzinfo=None)
            return win_start <= d <= win_end

        mask = df["published_at_ist"].apply(_in_win)
        filter_desc = f"IST window {win_start} .. {win_end}"
    else:
        mask = df["published_at_ist"].apply(
            lambda d: isinstance(d, (datetime, pd.Timestamp)) and d.date() == target_date
        )
        filter_desc = f"IST calendar date {target_date}"

    day_df = df.loc[mask].copy()
    print(f"Rows loaded            : {total_rows}")
    print(f"Rows after filter ({filter_desc}) : {len(day_df)}")

    if day_df.empty:
        print("Nothing to do — no rows on that date.")
        return 0

    # Fill NaNs for string cols
    for col in ("source", "title", "summary", "link", "guid", "categories", "author"):
        if col in day_df.columns:
            day_df[col] = day_df[col].fillna("").astype(str)

    # URL + title normalizations
    day_df["_norm_url"] = day_df["link"].apply(normalize_url)
    day_df["_norm_title"] = day_df["title"].apply(normalize_title)
    day_df["_tier"] = day_df["source"].apply(source_tier)
    day_df["_summary_len"] = day_df["summary"].str.len()
    day_df["_is_junk"] = day_df["title"].apply(is_junk_title)

    # Pass 0: split off junk/placeholder titles (homepages, "- Zee Business",
    # ET per-ticker templates, gold/silver rate pages, evergreen listicles).
    junk_df = day_df[day_df["_is_junk"]].copy()
    day_df = day_df[~day_df["_is_junk"]].copy()
    print(f"Junk / placeholder rows: {len(junk_df)}")
    print(f"Candidates for dedup   : {len(day_df)}")

    # Pass 1: exact URL dedup — keep best row per normalized URL
    day_df["_score"] = day_df.apply(lambda r: row_score(r.to_dict()), axis=1)
    day_df = (
        day_df.sort_values("_score", ascending=False)
              .drop_duplicates(subset=["_norm_url"], keep="first")
              .reset_index(drop=True)
    )
    print(f"After URL dedup        : {len(day_df)}")

    # Pass 2: fuzzy title clustering on the already-sorted df
    rows_sorted = day_df.to_dict("records")
    cluster_ids = cluster_titles(rows_sorted, threshold=args.threshold)
    day_df["cluster_id"] = cluster_ids

    # Winner per cluster = first occurrence after the sort (already best)
    day_df["cluster_rank"] = day_df.groupby("cluster_id").cumcount()
    day_df["is_winner"] = day_df["cluster_rank"] == 0
    deduped = day_df[day_df["is_winner"]].copy()
    print(f"After title clustering : {len(deduped)}  (clusters)")

    # Human-friendly output columns
    output_cols = [
        "source", "published_at_ist", "title", "summary",
        "link", "guid", "categories", "author", "raw_pubdate",
    ]
    deduped_out = deduped[output_cols].sort_values(
        "published_at_ist", ascending=False, na_position="last"
    ).reset_index(drop=True)

    # Clusters sheet — shows every row, which cluster, winner flag
    clusters_out = day_df[[
        "cluster_id", "is_winner", "source", "published_at_ist",
        "title", "summary", "link",
    ]].sort_values(["cluster_id", "is_winner"], ascending=[True, False]).reset_index(drop=True)

    # Source counts: raw vs survivors
    raw_counts = day_df.groupby("source").size().rename("items_raw")
    survivor_counts = deduped.groupby("source").size().rename("items_after_dedup")
    source_counts = (
        pd.concat([raw_counts, survivor_counts], axis=1)
          .fillna(0).astype(int)
          .sort_values("items_raw", ascending=False)
          .reset_index()
    )

    # Top-cluster-size report (where is duplication worst?)
    cluster_sizes = (
        day_df.groupby("cluster_id").size().reset_index(name="cluster_size")
    )
    cluster_sizes = cluster_sizes.merge(
        deduped[["cluster_id", "source", "title"]].rename(
            columns={"source": "winning_source", "title": "winning_title"}
        ),
        on="cluster_id",
        how="left",
    ).sort_values("cluster_size", ascending=False).reset_index(drop=True)

    # Strip tz for Excel-friendliness
    for frame in (deduped_out, clusters_out):
        if "published_at_ist" in frame.columns:
            frame["published_at_ist"] = pd.to_datetime(
                frame["published_at_ist"], errors="coerce"
            ).dt.tz_localize(None) if hasattr(
                frame["published_at_ist"].dtype, "tz"
            ) else frame["published_at_ist"]

    junk_out = junk_df[[
        "source", "published_at_ist", "title", "summary", "link",
    ]].sort_values("source").reset_index(drop=True) if not junk_df.empty else junk_df

    with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
        deduped_out.to_excel(xw, sheet_name="deduped", index=False)
        clusters_out.to_excel(xw, sheet_name="clusters", index=False)
        source_counts.to_excel(xw, sheet_name="source_counts", index=False)
        cluster_sizes.head(200).to_excel(xw, sheet_name="top_clusters", index=False)
        if not junk_out.empty:
            junk_out.to_excel(xw, sheet_name="junk_dropped", index=False)

    # Also a CSV for the deduped sheet
    csv_path = out_path.with_suffix(".csv")
    deduped_out.to_csv(csv_path, index=False)

    # Summary
    dupes_dropped = len(day_df) - len(deduped)
    print()
    print("=" * 60)
    print(f"Raw rows matched filter          : {mask.sum():>5}")
    print(f"Junk / placeholder rows dropped    : {len(junk_df):>5}")
    print(f"Candidate rows                     : {len(day_df):>5}")
    print(f"Unique stories after dedup         : {len(deduped):>5}")
    print(f"Duplicates collapsed               : {dupes_dropped:>5} "
          f"({dupes_dropped / max(len(day_df), 1) * 100:.1f}%)")
    if not cluster_sizes.empty:
        print(f"Largest cluster size               : {cluster_sizes['cluster_size'].max()}")
        print(f"Clusters with >=3 members (hot)    : "
              f"{(cluster_sizes['cluster_size'] >= 3).sum()}")
    print()
    print(f"Wrote:\n  {out_path}\n  {csv_path}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
