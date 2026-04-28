"""
Cross-publisher dedup pass on top of news_22apr_v3.xlsx.

We globally cluster all rows by fuzzy title match and keep the richest row
per cluster. Different events about the same stock stay separate because
they won't fuzzy-match each other on title. Same-story-different-publisher
rows collapse even if they sit in different focal-stock buckets (e.g. one
has the focal stock filled, the other is sector-only).

Clustering rule (rapidfuzz, applied only when 2+ shared content tokens):
  fuzz.token_set_ratio  >= threshold            AND
  fuzz.token_sort_ratio >= threshold - 12

Richness (same formula as v1, plus a small focal-stock bonus so that the
version with a named stock wins ties over the sector-only variant):
  score * 100
  + 500 if scrape_status == 'ok' else 0
  + 250 if has focal stock else 0
  + len(ai_summary)
  + 2 * len(key_numbers)
  + len(implication)

Input : news_22apr_v3.xlsx
Output: news_22apr_v4.xlsx
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
from rapidfuzz import fuzz

HERE = Path(__file__).resolve().parent

_SUFFIX_RE = re.compile(
    r"\b(limited|ltd\.?|pvt\.?|private|inc\.?|incorporated|corp\.?|"
    r"corporation|plc|co\.?|company|holdings?|industries|enterprises?)\b",
    re.IGNORECASE,
)
_PAREN_RE = re.compile(r"\(.*?\)")
# Strip trailing " - SourceName" / " | SourceName". Require whitespace on
# BOTH sides of the dash so hyphens inside words like "six-decade-old" are
# not treated as suffix separators. Also require at least 10 chars before it.
_SRC_SUFFIX_RE = re.compile(r"(?<=.{10})\s+[-–|]\s+[^-–|]{2,60}\s*$")
_PUNCT_RE = re.compile(r"[^a-z0-9 ]+")
_WS_RE = re.compile(r"\s+")

_STOPWORDS = {
    "a", "an", "the", "of", "for", "in", "on", "to", "at", "by",
    "and", "or", "is", "are", "was", "were", "be", "been", "with",
    "as", "from", "into", "over", "after", "before", "up", "down",
    "out", "new", "latest", "today", "news", "updates", "update",
    "live", "report", "reports", "says", "said",
}


def norm_stock(name: str) -> str:
    s = name.lower()
    s = _PAREN_RE.sub(" ", s)
    s = _SUFFIX_RE.sub(" ", s)
    s = _PUNCT_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


def norm_title(t: str) -> str:
    s = str(t or "").strip()
    s = _SRC_SUFFIX_RE.sub("", s)  # strip trailing " - Publisher"
    s = s.lower()
    s = _PUNCT_RE.sub(" ", s)
    tokens = [w for w in s.split() if w and w not in _STOPWORDS]
    return " ".join(tokens)


def richness(r: pd.Series) -> int:
    scrape_ok = 500 if str(r.get("scrape_status", "")) == "ok" else 0
    score = int(r.get("score") or 0)
    try:
        n_stk = int(r.get("n_focal_stocks") or 0)
    except (TypeError, ValueError):
        n_stk = 0
    has_stock = 250 if n_stk > 0 else 0
    return (
        score * 100
        + scrape_ok
        + has_stock
        + len(str(r.get("ai_summary") or ""))
        + 2 * len(str(r.get("key_numbers") or ""))
        + len(str(r.get("implication") or ""))
    )


def cluster_rows(
    rows: List[Dict[str, Any]],
    threshold: int = 82,
    min_shared_tokens: int = 2,
) -> List[int]:
    """Union-find clustering by fuzzy title match. Returns cluster_id per row."""
    n = len(rows)
    parent = list(range(n))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    norms = [r["_norm_title"] for r in rows]
    tokens = [set(n_.split()) for n_ in norms]

    for i in range(n):
        ni = norms[i]
        if not ni:
            continue
        for j in range(i + 1, n):
            nj = norms[j]
            if not nj:
                continue
            shared = tokens[i] & tokens[j]
            if len(shared) < min_shared_tokens:
                continue
            set_score = fuzz.token_set_ratio(ni, nj)
            if set_score < threshold:
                continue
            sort_score = fuzz.token_sort_ratio(ni, nj)
            # Accept if order is close (sort_score passes) OR if the titles
            # share a lot of significant tokens (set-based similarity).
            shared_ok = len(shared) >= 4
            if sort_score < threshold - 12 and not shared_ok:
                continue
            union(i, j)
    return [find(i) for i in range(n)]


def dedupe_global(
    df: pd.DataFrame,
    audit: List[Dict[str, Any]],
    threshold: int = 82,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Globally cluster all rows by fuzzy title; keep the richest per cluster.

    Different-event titles have low token overlap so won't merge. This also
    correctly merges cross-bucket duplicates (e.g. one publisher tagged a
    focal stock, another didn't, but both are the same story).
    """
    if len(df) == 0:
        return df, df

    rows = df.to_dict("records")
    cids = cluster_rows(rows, threshold=threshold)
    df = df.assign(_cid=cids)

    kept_frames: List[pd.DataFrame] = []
    dropped_frames: List[pd.DataFrame] = []

    for cid, cgrp in df.groupby("_cid", sort=False):
        if len(cgrp) == 1:
            kept_frames.append(cgrp.drop(columns="_cid"))
            continue
        cgrp_sorted = cgrp.sort_values("_richness", ascending=False)
        kept_row = cgrp_sorted.iloc[[0]].drop(columns="_cid")
        dropped_rows = cgrp_sorted.iloc[1:].drop(columns="_cid")
        kept_frames.append(kept_row)
        dropped_frames.append(dropped_rows)
        for _, r in dropped_rows.iterrows():
            audit.append({
                "kept_title": kept_row.iloc[0]["title"],
                "kept_source": kept_row.iloc[0]["source"],
                "kept_focal_stocks": kept_row.iloc[0]["affected_stocks"],
                "kept_richness": int(kept_row.iloc[0]["_richness"]),
                "kept_score": int(kept_row.iloc[0]["score"] or 0),
                "dropped_title": r["title"],
                "dropped_source": r["source"],
                "dropped_focal_stocks": r["affected_stocks"],
                "dropped_richness": int(r["_richness"]),
                "dropped_score": int(r["score"] or 0),
            })

    kept_df = pd.concat(kept_frames, ignore_index=True) if kept_frames else df.iloc[0:0]
    dropped_df = (
        pd.concat(dropped_frames, ignore_index=True)
        if dropped_frames else df.iloc[0:0]
    )
    return kept_df, dropped_df


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=str(HERE / "news_22apr_v3.xlsx"))
    ap.add_argument("--sheet", default="kept")
    ap.add_argument("--output", default=str(HERE / "news_22apr_v4.xlsx"))
    ap.add_argument("--threshold", type=int, default=82,
                    help="fuzz.token_set_ratio threshold for a match")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    in_path = Path(args.input)
    out_path = Path(args.output)
    if not in_path.exists():
        print(f"ERROR: {in_path} not found")
        return 1

    df = pd.read_excel(in_path, sheet_name=args.sheet).fillna("")
    df = df.reset_index(drop=True)
    total_in = len(df)
    print(f"Loaded {total_in} rows from {in_path.name}::{args.sheet}")

    df["_norm_title"] = df["title"].apply(norm_title)
    df["n_focal_stocks"] = pd.to_numeric(
        df.get("n_focal_stocks", 0), errors="coerce"
    ).fillna(0).astype(int)
    df["_richness"] = df.apply(richness, axis=1)

    audit: List[Dict[str, Any]] = []
    kept, dropped = dedupe_global(df, audit, threshold=args.threshold)
    kept = kept.sort_values(
        ["score", "published_at_ist"], ascending=[False, False]
    ).reset_index(drop=True)

    # For reporting: bucket counts BEFORE and AFTER dedup
    def _bucket_counts(x: pd.DataFrame) -> Dict[str, int]:
        return {
            "single": int((x["n_focal_stocks"] == 1).sum()),
            "pair": int((x["n_focal_stocks"] == 2).sum()),
            "sector": int((x["n_focal_stocks"] == 0).sum()),
        }
    before_counts = _bucket_counts(df)
    after_counts = _bucket_counts(kept)

    helper_cols = {"_norm_title", "_richness", "_stock_keys",
                   "_single_key", "_pair_key", "_sector_key"}
    main_cols = [
        "score", "direction", "category",
        "industry_primary", "affected_industries",
        "affected_stocks", "n_focal_stocks",
        "ai_summary", "implication", "key_numbers",
        "source", "published_at_ist", "title", "link",
        "scrape_status", "scraped_chars",
        "stocks_prior", "stock_focus_reason", "industries_prior",
    ]
    main_cols = [c for c in main_cols if c in kept.columns]

    primary_break = (
        kept.groupby("industry_primary").size().reset_index(name="items")
            .sort_values("items", ascending=False)
            .rename(columns={"industry_primary": "primary_industry"})
    )
    cat_break = (
        kept.groupby("category").size().reset_index(name="items")
            .sort_values("items", ascending=False)
    )
    top_stocks = (
        kept[kept["affected_stocks"] != ""]
            .assign(_s=kept["affected_stocks"].str.split(" | ", regex=False))
            .explode("_s")
            .assign(_s=lambda d: d["_s"].str.strip())
            .query("_s != ''")
            .groupby("_s").size().reset_index(name="items")
            .sort_values("items", ascending=False)
            .rename(columns={"_s": "stock"})
    )
    audit_df = pd.DataFrame(audit)
    stats = pd.DataFrame([
        {"metric": "input_rows", "value": total_in},
        {"metric": "single_before / after",
         "value": f"{before_counts['single']} / {after_counts['single']}"},
        {"metric": "pair_before / after",
         "value": f"{before_counts['pair']} / {after_counts['pair']}"},
        {"metric": "sector_before / after",
         "value": f"{before_counts['sector']} / {after_counts['sector']}"},
        {"metric": "final_kept", "value": int(len(kept))},
        {"metric": "total_dropped", "value": int(len(dropped))},
        {"metric": "fuzz_threshold", "value": args.threshold},
    ])

    with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
        kept[main_cols].to_excel(xw, sheet_name="kept", index=False)
        dropped[main_cols].to_excel(xw, sheet_name="dropped_dupes", index=False)
        if not audit_df.empty:
            audit_df.to_excel(xw, sheet_name="dedup_audit", index=False)
        primary_break.to_excel(xw, sheet_name="by_primary_industry", index=False)
        cat_break.to_excel(xw, sheet_name="by_category", index=False)
        top_stocks.to_excel(xw, sheet_name="top_stocks", index=False)
        stats.to_excel(xw, sheet_name="run_stats", index=False)

    csv_path = out_path.with_suffix(".csv")
    kept[main_cols].to_csv(csv_path, index=False)

    print()
    print("=" * 60)
    print(f"FINAL kept: {len(kept)}  (dropped {len(dropped)} cross-publisher dupes)")
    print()
    print("Bucket change (before -> after):")
    print(f"  single-focal-stock : {before_counts['single']:>3}  ->  {after_counts['single']:>3}")
    print(f"  two-focal-stock    : {before_counts['pair']:>3}  ->  {after_counts['pair']:>3}")
    print(f"  sector-only        : {before_counts['sector']:>3}  ->  {after_counts['sector']:>3}")
    print()
    print("Primary-industry breakdown (final):")
    for _, r in primary_break.iterrows():
        print(f"  {r['primary_industry']:46s} {int(r['items']):>3}")
    print()
    if not audit_df.empty:
        print(f"Dedup decisions ({len(audit_df)} rows merged):")
        for _, r in audit_df.iterrows():
            kept_stk = r["kept_focal_stocks"] or "(sector-only)"
            drop_stk = r["dropped_focal_stocks"] or "(sector-only)"
            print(f"  KEPT    ({r['kept_source']}, r={r['kept_richness']}, "
                  f"stk={kept_stk[:30]}): {str(r['kept_title'])[:80]}")
            print(f"  DROPPED ({r['dropped_source']}, r={r['dropped_richness']}, "
                  f"stk={drop_stk[:30]}): {str(r['dropped_title'])[:80]}")
            print()
    print(f"Wrote:\n  {out_path}\n  {csv_path}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
