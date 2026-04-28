"""
Post-process news_22apr_enriched.xlsx -> news_22apr_v1.xlsx

Rules (per user spec):
  * Drop category == 'order_win' (captured elsewhere via BSE/NSE PDFs).
  * Single-company rows -> group by normalized stock name and keep the
    single richest version (prefers fully-scraped, higher score, longer
    ai_summary / key_numbers / implication).
  * Multi-company rows (2+ affected stocks) and zero-company rows
    (pure sector / macro) -> keep all.

Run:
  cd news_scratch
  .venv/bin/python dedupe_v1.py
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import List

import pandas as pd

HERE = Path(__file__).resolve().parent

_SUFFIX_RE = re.compile(
    r"\b(limited|ltd\.?|pvt\.?|private|inc\.?|incorporated|corp\.?|"
    r"corporation|plc|co\.?|company|holdings?|industries|enterprises?)\b",
    re.IGNORECASE,
)
_PAREN_RE = re.compile(r"\(.*?\)")
_PUNCT_RE = re.compile(r"[^a-z0-9 ]+")
_WS_RE = re.compile(r"\s+")


def split_stocks(val) -> List[str]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return []
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none"):
        return []
    parts = [p.strip() for p in s.split(",") if p.strip()]
    return parts


def norm_stock(name: str) -> str:
    s = name.lower()
    s = _PAREN_RE.sub(" ", s)
    s = _SUFFIX_RE.sub(" ", s)
    s = _PUNCT_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


def richness(row: pd.Series) -> int:
    scrape_ok = 500 if str(row.get("scrape_status", "")) == "ok" else 0
    score = int(row.get("score") or 0)
    ai = str(row.get("ai_summary") or "")
    keyn = str(row.get("key_numbers") or "")
    impl = str(row.get("implication") or "")
    return score * 100 + scrape_ok + len(ai) + 2 * len(keyn) + len(impl)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=str(HERE / "news_22apr_enriched.xlsx"))
    ap.add_argument("--sheet", default="kept")
    ap.add_argument("--output", default=str(HERE / "news_22apr_v1.xlsx"))
    ap.add_argument("--drop-categories", default="order_win",
                    help="Comma-separated categories to drop before dedup")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    in_path = Path(args.input)
    out_path = Path(args.output)
    if not in_path.exists():
        print(f"ERROR: {in_path} not found")
        return 1

    df = pd.read_excel(in_path, sheet_name=args.sheet).fillna("")
    total_in = len(df)

    drop_cats = {c.strip() for c in args.drop_categories.split(",") if c.strip()}
    removed_cat = df[df["category"].isin(drop_cats)].copy()
    df = df[~df["category"].isin(drop_cats)].copy().reset_index(drop=True)
    print(f"Input rows : {total_in}")
    print(f"Dropped categories {sorted(drop_cats)}: {len(removed_cat)}")
    print(f"Remaining  : {len(df)}")

    df["_stocks"] = df["affected_stocks"].apply(split_stocks)
    df["_n_stocks"] = df["_stocks"].apply(len)
    df["_richness"] = df.apply(richness, axis=1)

    single_mask = df["_n_stocks"] == 1
    single = df[single_mask].copy()
    others = df[~single_mask].copy()

    single["_key"] = single["_stocks"].apply(lambda xs: norm_stock(xs[0]))

    # Keep only the richest row per normalized single-stock key
    single_sorted = single.sort_values("_richness", ascending=False)
    single_kept = single_sorted.drop_duplicates(subset=["_key"], keep="first")
    single_dropped = single_sorted[
        ~single_sorted.index.isin(single_kept.index)
    ].copy()

    print(
        f"Single-company rows: {len(single)}  -> kept {len(single_kept)}, "
        f"dropped {len(single_dropped)} duplicates"
    )
    print(f"Multi / zero-company rows kept as-is: {len(others)}")

    kept = pd.concat([single_kept, others], ignore_index=True)
    kept = kept.sort_values(
        ["score", "published_at_ist"], ascending=[False, False]
    ).reset_index(drop=True)

    main_cols = [
        "score", "direction", "category",
        "affected_stocks", "affected_industries",
        "ai_summary", "implication", "key_numbers",
        "source", "published_at_ist", "title", "link",
        "scrape_status", "scraped_chars", "reason",
    ]
    main_cols = [c for c in main_cols if c in kept.columns]

    # Build dedup audit: which stock -> which rows were merged away
    audit_rows = []
    for key, grp in single_sorted.groupby("_key"):
        if len(grp) <= 1:
            continue
        kept_row = grp.iloc[0]
        for _, r in grp.iloc[1:].iterrows():
            audit_rows.append({
                "stock_key": key,
                "kept_title": kept_row["title"],
                "kept_source": kept_row["source"],
                "kept_richness": kept_row["_richness"],
                "kept_score": kept_row["score"],
                "dropped_title": r["title"],
                "dropped_source": r["source"],
                "dropped_richness": r["_richness"],
                "dropped_score": r["score"],
            })
    audit_df = pd.DataFrame(audit_rows)

    cat_break = (
        kept.groupby("category").size().reset_index(name="items")
            .sort_values("items", ascending=False)
    )
    dir_break = (
        kept.groupby("direction").size().reset_index(name="items")
            .sort_values("items", ascending=False)
    )
    ind_break = (
        kept.assign(_s=kept["affected_industries"].fillna("").astype(str).str.split(","))
            .explode("_s")
            .assign(_s=lambda d: d["_s"].str.strip())
            .query("_s != ''")
            .groupby("_s").size().reset_index(name="items")
            .sort_values("items", ascending=False)
            .rename(columns={"_s": "industry"})
    )
    stk_break = (
        kept.assign(_s=kept["affected_stocks"].fillna("").astype(str).str.split(","))
            .explode("_s")
            .assign(_s=lambda d: d["_s"].str.strip())
            .query("_s != ''")
            .groupby("_s").size().reset_index(name="mentions")
            .sort_values("mentions", ascending=False)
            .rename(columns={"_s": "stock"})
    )

    stats = pd.DataFrame([
        {"metric": "input_rows", "value": total_in},
        {"metric": "dropped_categories", "value": ",".join(sorted(drop_cats))},
        {"metric": "dropped_for_category", "value": len(removed_cat)},
        {"metric": "single_company_rows", "value": int(len(single))},
        {"metric": "single_company_kept", "value": int(len(single_kept))},
        {"metric": "single_company_deduped_away", "value": int(len(single_dropped))},
        {"metric": "multi_or_zero_company_kept", "value": int(len(others))},
        {"metric": "final_kept", "value": int(len(kept))},
    ])

    with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
        kept[main_cols].to_excel(xw, sheet_name="kept", index=False)
        single_dropped[main_cols].to_excel(
            xw, sheet_name="dropped_dupes", index=False
        )
        removed_cat[main_cols].to_excel(
            xw, sheet_name="dropped_order_wins", index=False
        )
        if not audit_df.empty:
            audit_df.to_excel(xw, sheet_name="dedup_audit", index=False)
        cat_break.to_excel(xw, sheet_name="by_category", index=False)
        dir_break.to_excel(xw, sheet_name="by_direction", index=False)
        ind_break.to_excel(xw, sheet_name="by_industry", index=False)
        stk_break.to_excel(xw, sheet_name="top_stocks", index=False)
        stats.to_excel(xw, sheet_name="run_stats", index=False)

    csv_path = out_path.with_suffix(".csv")
    kept[main_cols].to_csv(csv_path, index=False)

    print()
    print("=" * 60)
    print(f"FINAL kept: {len(kept)}")
    print()
    print("Category breakdown (final):")
    for _, r in cat_break.iterrows():
        print(f"  {r['category']:22s} {int(r['items']):>3}")
    print()
    if not audit_df.empty:
        print("Examples of single-stock dedup (kept vs dropped):")
        for _, r in audit_df.head(8).iterrows():
            print(f"  [{r['stock_key']}]")
            print(f"    KEPT    ({r['kept_source']}, r={r['kept_richness']}): "
                  f"{str(r['kept_title'])[:90]}")
            print(f"    DROPPED ({r['dropped_source']}, r={r['dropped_richness']}): "
                  f"{str(r['dropped_title'])[:90]}")
    print()
    print(f"Wrote:\n  {out_path}\n  {csv_path}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
