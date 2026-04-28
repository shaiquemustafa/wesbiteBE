"""
Refocus `affected_stocks` so it contains only the company the news is
genuinely ABOUT, not every name mentioned.

Rules the LLM is asked to follow:
  • 0 companies  -> pure sector / policy / macro / industry story,
                    OR a multi-company listicle / "top gainers" piece.
  • 1 company    -> the story is clearly about ONE Indian listed firm;
                    peers/competitors that are only referenced do not count.
  • 2 companies  -> ONLY when two listed parties are both focal
                    (e.g. a binding JV / M&A between them, a direct
                    legal/commercial dispute). Use sparingly.
  • Indian LISTED company only. Drop foreign and private companies.
  • When in doubt, prefer empty.

Input : news_22apr_v2.xlsx  (kept sheet)
Output: news_22apr_v3.xlsx

Run:
  cd news_scratch
  .venv/bin/python focus_stocks.py
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import truststore  # type: ignore
    truststore.inject_into_ssl()
except Exception:
    pass

import pandas as pd
from dotenv import load_dotenv

HERE = Path(__file__).resolve().parent
for cand in [HERE / ".env", HERE.parent / "wesbiteBE" / ".env"]:
    if cand.exists():
        load_dotenv(cand)
        break

from openai import OpenAI  # noqa: E402

API_KEY = os.environ.get("OPENAI_API_KEY")
if not API_KEY:
    sys.exit("ERROR: OPENAI_API_KEY is not set")
client = OpenAI(api_key=API_KEY)

PRICE_IN = {"gpt-4.1-nano": 0.10, "gpt-4.1-mini": 0.40}
PRICE_OUT = {"gpt-4.1-nano": 0.40, "gpt-4.1-mini": 1.60}

SYSTEM_PROMPT = """You extract the FOCAL INDIAN LISTED COMPANY from a financial news item.

The goal is to answer: "If I were alerting a user about this news, whose stock \
ticker is this really about?"

STRICT RULES:
  1. Output is a LIST of 0, 1, or at most 2 Indian-listed-company names.
  2. Output 0 companies (empty list) when the story is PRIMARILY about:
       - a sector / industry trend  (e.g. "Crude oil output falls for 11th year")
       - a government policy / regulation / rule  (e.g. "RBI draft PPI norms",
         "Online Gaming Rules from May 1")
       - macro or commodity data  (e.g. "Bank credit growth to ease to 12%",
         "Petrol prices may rise Rs 25-28 per litre")
       - a listicle / multi-company market wrap  (e.g. "5 stocks to watch")
     Do NOT guess. Do NOT sprinkle in peer names. Leave empty.
  3. Output 1 company when the story is clearly about ONE Indian listed firm.
     Mentions of competitors or peers DO NOT make them focal.
     Example: "HCLTech shares fall 11%, worst single day in 11 years" ->
       ["HCL Technologies"]   (never add Infosys, TCS, Wipro even if compared)
     Example: "Vedanta vs Adani over Jaypee: NCLAT reserves judgment on
       Vedanta plea" -> ["Vedanta"]   (Adani is the counterparty; the plea
       filed is Vedanta's, which is the spirit of the news)
  4. Output 2 companies ONLY when both are focal parties to the event:
       - binding JV / merger / acquisition between two LISTED Indian firms
       - direct commercial deal or dispute where BOTH stocks should move
     Example: "Jio Financial, Allianz formalise 50:50 JV" -> ["Jio Financial
       Services"]   (Allianz is foreign, dropped)
     Example: "TCS, Infosys in three-way JV for..." -> ["TCS", "Infosys"]
  5. DROP non-Indian / non-listed names:
       - foreign parents / counterparties (Shell, Allianz, Naver, Karex Bhd,
         KKR, Temasek, Google, Apple, Samsung, etc.)
       - private / unlisted Indian entities
       - government bodies, ministries, regulators
  6. Use the CURRENT affected_stocks field as a STARTING POINT but DROP any
     name that is a peer / background mention / foreign / unlisted.
  7. Preserve the company name as commonly used on Indian exchanges (e.g.
     "HCL Technologies", "Reliance Industries", "Jio Financial Services",
     "Vedanta", "Tata Motors"). Do NOT invent tickers.

Return a JSON OBJECT with key "focus" whose value is a JSON ARRAY.
Each element: {"id": <int>, "stocks": ["<name>", ...], "reason": "<<=15 words>"}

Return one entry per input item, preserving ids. No extra keys, no prose.
"""


def _format_batch(batch: List[Dict[str, Any]]) -> str:
    lines = ["ITEMS:"]
    for it in batch:
        lines.append(
            f"- id: {it['id']}\n"
            f"  title: {it['title']}\n"
            f"  summary: {it['summary']}\n"
            f"  current_stocks: {it['current_stocks']}\n"
            f"  industry: {it['industry']}\n"
            f"  category: {it['category']}"
        )
    return "\n".join(lines)


def _call_llm(
    model: str,
    batch: List[Dict[str, Any]],
    max_retries: int = 3,
) -> tuple[List[Dict[str, Any]], int, int]:
    user_msg = _format_batch(batch)
    last_err: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            resp = client.chat.completions.create(
                model=model,
                temperature=0.0,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_msg},
                ],
                max_tokens=1400,
            )
            content = resp.choices[0].message.content or "{}"
            data = json.loads(content)
            verdicts = data.get("focus") or data.get("items") or []
            usage = resp.usage
            return (
                verdicts,
                getattr(usage, "prompt_tokens", 0) or 0,
                getattr(usage, "completion_tokens", 0) or 0,
            )
        except Exception as e:
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"LLM failed after {max_retries} retries: {last_err}")


def _clean_stocks(stocks: Any) -> List[str]:
    if not isinstance(stocks, list):
        return []
    out: List[str] = []
    seen = set()
    for s in stocks:
        if not isinstance(s, str):
            continue
        name = s.strip().strip(",").strip()
        if not name:
            continue
        # drop parenthetical disclaimers like "(if listed)", "(private)"
        if "(" in name:
            name = name.split("(")[0].strip()
        key = name.lower()
        if key and key not in seen:
            seen.add(key)
            out.append(name)
    return out[:2]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=str(HERE / "news_22apr_v2.xlsx"))
    ap.add_argument("--sheet", default="kept")
    ap.add_argument("--output", default=str(HERE / "news_22apr_v3.xlsx"))
    ap.add_argument("--model", default="gpt-4.1-mini")
    ap.add_argument("--batch-size", type=int, default=12)
    ap.add_argument("--workers", type=int, default=4)
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
    df["_id"] = df.index.astype(int)
    print(f"Loaded {len(df)} rows from {in_path.name}::{args.sheet}")

    items: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        items.append({
            "id": int(r["_id"]),
            "title": str(r.get("title") or "")[:220],
            "summary": str(r.get("ai_summary") or "")[:700],
            "current_stocks": str(r.get("affected_stocks") or "")[:250],
            "industry": str(r.get("industry_primary") or r.get("affected_industries") or "")[:120],
            "category": str(r.get("category") or ""),
        })

    batches = [
        items[i:i + args.batch_size]
        for i in range(0, len(items), args.batch_size)
    ]
    print(f"Prepared {len(batches)} batches of up to {args.batch_size}, "
          f"running {args.workers} in parallel with {args.model}")

    results: Dict[int, Dict[str, Any]] = {}
    tot_in = 0
    tot_out = 0
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = {
            pool.submit(_call_llm, args.model, b): idx
            for idx, b in enumerate(batches)
        }
        done = 0
        for fut in as_completed(futs):
            done += 1
            try:
                verdicts, ti, to = fut.result()
                tot_in += ti
                tot_out += to
                for v in verdicts:
                    try:
                        rid = int(v.get("id"))
                    except (TypeError, ValueError):
                        continue
                    results[rid] = {
                        "stocks": _clean_stocks(v.get("stocks")),
                        "reason": str(v.get("reason") or "")[:120],
                    }
            except Exception as e:
                print(f"  batch {futs[fut]} failed: {e}")
            if done % 2 == 0 or done == len(batches):
                print(f"  {done}/{len(batches)}  tok in/out={tot_in}/{tot_out}  "
                      f"elapsed={time.time() - t0:.1f}s")

    # Apply back
    df["stocks_prior"] = df["affected_stocks"]
    df["affected_stocks"] = df["_id"].apply(
        lambda rid: " | ".join(results.get(rid, {}).get("stocks", []))
    )
    df["stock_focus_reason"] = df["_id"].apply(
        lambda rid: results.get(rid, {}).get("reason", "")
    )
    df["n_focal_stocks"] = df["_id"].apply(
        lambda rid: len(results.get(rid, {}).get("stocks", []))
    )

    # Diagnostics: how did the stock field change?
    def _prior_count(s: str) -> int:
        if not isinstance(s, str) or not s.strip():
            return 0
        return len([x for x in s.split(",") if x.strip()])

    df["n_prior_stocks"] = df["stocks_prior"].apply(_prior_count)

    # Counts
    n_emptied = int(((df["n_prior_stocks"] > 0) & (df["n_focal_stocks"] == 0)).sum())
    n_narrowed = int(
        ((df["n_prior_stocks"] > df["n_focal_stocks"]) &
         (df["n_focal_stocks"] > 0)).sum()
    )
    n_unchanged_single = int(
        ((df["n_prior_stocks"] == 1) & (df["n_focal_stocks"] == 1)).sum()
    )
    n_always_empty = int(
        ((df["n_prior_stocks"] == 0) & (df["n_focal_stocks"] == 0)).sum()
    )

    cat_focus = (
        df.groupby(["n_focal_stocks"]).size().reset_index(name="rows")
          .rename(columns={"n_focal_stocks": "num_focal_stocks"})
    )
    top_stocks = (
        df[df["affected_stocks"] != ""]
          .assign(_s=df["affected_stocks"].str.split(" | ", regex=False))
          .explode("_s")
          .assign(_s=lambda d: d["_s"].str.strip())
          .query("_s != ''")
          .groupby("_s").size().reset_index(name="items")
          .sort_values("items", ascending=False)
          .rename(columns={"_s": "stock"})
    )
    empty_stock_by_cat = (
        df[df["n_focal_stocks"] == 0]
          .groupby("category").size().reset_index(name="rows")
          .sort_values("rows", ascending=False)
    )
    empty_stock_by_ind = (
        df[df["n_focal_stocks"] == 0]
          .groupby("industry_primary").size().reset_index(name="rows")
          .sort_values("rows", ascending=False)
    )

    main_cols = [
        "score", "direction", "category",
        "industry_primary", "affected_industries",
        "affected_stocks", "n_focal_stocks",
        "ai_summary", "implication", "key_numbers",
        "source", "published_at_ist", "title", "link",
        "scrape_status", "scraped_chars",
        "stocks_prior", "stock_focus_reason",
        "industries_prior",
    ]
    main_cols = [c for c in main_cols if c in df.columns]

    inp_cost = tot_in / 1_000_000 * PRICE_IN.get(args.model, 0.0)
    out_cost = tot_out / 1_000_000 * PRICE_OUT.get(args.model, 0.0)
    stats = pd.DataFrame([
        {"metric": "model", "value": args.model},
        {"metric": "rows", "value": int(len(df))},
        {"metric": "emptied_(multi_guess_to_none)", "value": n_emptied},
        {"metric": "narrowed_(kept_subset)", "value": n_narrowed},
        {"metric": "unchanged_single_stock", "value": n_unchanged_single},
        {"metric": "already_empty", "value": n_always_empty},
        {"metric": "final_with_stock",
         "value": int((df["n_focal_stocks"] > 0).sum())},
        {"metric": "final_without_stock",
         "value": int((df["n_focal_stocks"] == 0).sum())},
        {"metric": "input_tokens", "value": tot_in},
        {"metric": "output_tokens", "value": tot_out},
        {"metric": "estimated_cost_usd", "value": f"${inp_cost + out_cost:.4f}"},
        {"metric": "elapsed_sec", "value": round(time.time() - t0, 1)},
    ])

    with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
        df[main_cols].sort_values(
            ["score", "published_at_ist"], ascending=[False, False]
        ).to_excel(xw, sheet_name="kept", index=False)
        cat_focus.to_excel(xw, sheet_name="focal_stock_counts", index=False)
        top_stocks.to_excel(xw, sheet_name="top_stocks", index=False)
        empty_stock_by_cat.to_excel(xw, sheet_name="sector_only_by_category", index=False)
        empty_stock_by_ind.to_excel(xw, sheet_name="sector_only_by_industry", index=False)
        stats.to_excel(xw, sheet_name="run_stats", index=False)

    csv_path = out_path.with_suffix(".csv")
    df[main_cols].to_csv(csv_path, index=False)

    print()
    print("=" * 60)
    print(f"Rows: {len(df)}")
    print(f"  emptied (multi-guess -> none)  : {n_emptied}")
    print(f"  narrowed (kept subset)         : {n_narrowed}")
    print(f"  unchanged single stock          : {n_unchanged_single}")
    print(f"  already empty, still empty      : {n_always_empty}")
    print(f"  final with 1+ stock             : {int((df['n_focal_stocks'] > 0).sum())}")
    print(f"  final pure sector (no stock)    : {int((df['n_focal_stocks'] == 0).sum())}")
    print(f"Tokens in/out: {tot_in:,} / {tot_out:,}   cost ${inp_cost + out_cost:.4f}")
    print(f"Elapsed: {time.time() - t0:.1f}s")
    print()
    print("Focal stock count distribution:")
    for _, r in cat_focus.iterrows():
        print(f"  {int(r['num_focal_stocks'])} stock(s): {int(r['rows'])} rows")
    print()
    print("Top 15 focal stocks:")
    for _, r in top_stocks.head(15).iterrows():
        print(f"  {int(r['items']):>2}  {r['stock']}")
    print()
    print(f"Wrote:\n  {out_path}\n  {csv_path}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
