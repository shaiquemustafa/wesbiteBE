"""
Stage-2 refinement with gpt-4.1-mini.

Takes the survivors from stage-1 (cheapest model) and re-screens them with
a sharper model, stricter criteria, and a richer schema:

  - one of ~12 news categories
  - affected_stocks (Indian-listed, if explicit in title/summary)
  - affected_industries
  - rich_summary (1-2 sentences, preserves numbers)
  - keep/drop with a harsher threshold

Run:
  cd news_scratch
  .venv/bin/python refine_news.py --input news_22apr_cheapest_model.xlsx
  .venv/bin/python refine_news.py --input news_22apr_cheapest_model.xlsx --model gpt-4.1-mini --min-score 7
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

for candidate in [HERE / ".env", HERE.parent / "wesbiteBE" / ".env"]:
    if candidate.exists():
        load_dotenv(candidate)
        break

from openai import OpenAI  # noqa: E402

API_KEY = os.environ.get("OPENAI_API_KEY")
if not API_KEY:
    sys.exit(
        "ERROR: OPENAI_API_KEY is not set.\n"
        "  Fix: export OPENAI_API_KEY=sk-... in this shell,\n"
        "  or create news_scratch/.env with OPENAI_API_KEY=sk-..."
    )
client = OpenAI(api_key=API_KEY)

PRICE_PER_1M_INPUT = {"gpt-4.1-nano": 0.10, "gpt-4.1-mini": 0.40, "gpt-4o-mini": 0.15}
PRICE_PER_1M_OUTPUT = {"gpt-4.1-nano": 0.40, "gpt-4.1-mini": 1.60, "gpt-4o-mini": 0.60}

# ---------------------------------------------------------------------------
# Fixed category taxonomy (keep model consistent across runs)
# ---------------------------------------------------------------------------
CATEGORIES = [
    "earnings",              # quarterly / annual results of a listed co
    "order_win",             # specific contract / order won
    "deal_mna",              # M&A, JV, stake sale, strategic tie-up
    "analyst_call",          # broker target / upgrade / downgrade on a named stock
    "product_capex",         # new product, capacity expansion, new plant, R&D
    "regulation_sector",     # SEBI/RBI/CCI/IRDAI/sectoral regulator action
    "govt_spending",         # budget allocations, PLI schemes, infra spend, subsidies
    "macro_to_sector",       # commodity / FX / global macro with clear sector read-through
    "demand_trend",          # consumption, sales data, demand shifts in a sector
    "legal_dispute",         # NCLT/NCLAT/courts/tribunals/bankruptcy/IBC
    "management",            # CEO/CFO/board/promoter changes
    "capital_raise_rating",  # IPO/QIP/bonds/credit-rating changes
    "other",                 # genuinely important but doesn't fit — use sparingly
]

SYSTEM_PROMPT = f"""You are a sharp Indian equity-research analyst running \
SECOND-PASS triage. Input items already passed a relevance pre-screen. \
Your job is to be BRUTAL: only keep items that would meaningfully inform a \
BUY / SELL / HOLD decision on an Indian listed stock or a clearly-defined \
sector view.

DROP (keep=false) unless you have a specific reason to keep:
  • Daily market wraps ("Sensex up 200 pts", "Nifty closes flat", \
    "Investors lose Rs X crore", "top gainers/losers") — DROP.
  • Intraday Reuters SNAPSHOTs and pre-market previews — DROP.
  • Generic "stocks to watch today" or "buzzing stocks" lists — DROP.
  • Opinion columns, podcasts, editorials without a concrete stock event — DROP.
  • Foreign corporate / political news without a CLEAR read-through to a \
    named Indian company or sector — DROP.
  • Personal finance tips, SIP guides, tax explainers — DROP.
  • Speculation ("what to expect from Q4") unless it contains concrete \
    guidance, management commentary, or numbers — DROP.
  • Duplicate angles on the same story (we already deduped, but some slip \
    through) — DROP the weaker one.

KEEP (keep=true) when the item delivers a concrete signal:
  • Specific earnings numbers, guidance, margin moves.
  • Order wins / contracts with a value.
  • M&A, JV, stake sales with parties named.
  • Analyst target / rating changes on a specific stock.
  • Product launches, capacity additions, plant commissioning.
  • Sector-targeted regulation / policy with material impact.
  • Commodity / FX moves with clear Indian sector read-through (crude up → \
    OMCs / aviation / paints; steel down → autos input cost).
  • Demand / consumption data affecting a sector.
  • Credit-rating / fundraising events.

FOR EACH ITEM, RETURN:

  category: EXACTLY ONE of: {", ".join(CATEGORIES)}
  score:    0-10 (be strict; 8+ means concrete, stock-moving; 6-7 \
            directional but smaller; <6 → keep=false)
  keep:     true ONLY if score >= 6 AND the stricter criteria are met
  affected_stocks:     list of INDIAN-LISTED company names actually \
                       mentioned in the title/summary (not guessed). \
                       Prefer the common short name (e.g. "HCL Tech", \
                       "Tata Motors", "Reliance Industries"). Empty list \
                       if none explicitly mentioned.
  affected_industries: list (1-4) of affected industry/sector tags \
                       lowercased (e.g. "it services", "oil & gas", \
                       "cement", "autos", "banks", "pharma", \
                       "renewables", "defence", "telecom", "fmcg", \
                       "metals", "real estate", "chemicals", "capital \
                       goods", "power", "insurance", "retail", \
                       "agri", "logistics", "media", "consumer tech"). \
                       Always fill this even if no specific stock.
  rich_summary: 1-2 sentences, MUST preserve key numbers (profit change \
                %, order value Rs cr, target price, guidance, etc.). \
                Factual, no adjectives. Max ~40 words.
  reason: <= 20 words explaining keep/drop.

Return a JSON OBJECT with a single key "verdicts" = JSON ARRAY, one entry \
per input item in the same order:
{{
  "i": <int>, "keep": <bool>, "score": <int>,
  "category": "<tag>",
  "affected_stocks": ["..."],
  "affected_industries": ["..."],
  "rich_summary": "...",
  "reason": "..."
}}
Nothing outside the JSON object. No markdown.
"""


def _format_batch(batch: List[Dict[str, Any]]) -> str:
    lines = []
    for row in batch:
        title = str(row.get("title") or "").strip()
        summary = str(row.get("summary") or "").strip()
        source = str(row.get("source") or "").strip()
        if len(summary) > 500:
            summary = summary[:500] + "…"
        lines.append(
            f"[i={row['i']}] ({source}) {title}\n    summary: {summary or '(none)'}"
        )
    return "\n".join(lines)


def _call_llm(
    model: str,
    batch: List[Dict[str, Any]],
    max_retries: int = 3,
) -> tuple[List[Dict[str, Any]], int, int]:
    user_msg = (
        f"Refine the following {len(batch)} items. Return exactly "
        f"{len(batch)} verdicts, same order.\n\n" + _format_batch(batch)
    )
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
                max_tokens=1800,  # richer output per item
            )
            content = resp.choices[0].message.content or "{}"
            data = json.loads(content)
            verdicts = data.get("verdicts") or data.get("results") or []
            if not isinstance(verdicts, list):
                raise ValueError(f"bad shape: {type(verdicts)}")
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


def refine_all(
    items: List[Dict[str, Any]],
    model: str,
    batch_size: int,
    workers: int,
) -> tuple[List[Dict[str, Any]], Dict[str, int]]:
    batches = [items[i:i + batch_size] for i in range(0, len(items), batch_size)]
    verdicts_by_i: Dict[int, Dict[str, Any]] = {}
    totals = {"input_tokens": 0, "output_tokens": 0, "failed_batches": 0}
    t0 = time.time()

    def _work(batch):
        vs, inp, out = _call_llm(model, batch)
        return batch, vs, inp, out

    print(f"Refining {len(items)} items  model={model}  batches={len(batches)}  "
          f"batch_size={batch_size}  workers={workers}")

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(_work, b): b for b in batches}
        done = 0
        for fut in as_completed(futs):
            done += 1
            try:
                batch, verdicts, inp_tok, out_tok = fut.result()
                totals["input_tokens"] += inp_tok
                totals["output_tokens"] += out_tok
                indices_in_batch = {r["i"] for r in batch}
                for v in verdicts:
                    i = v.get("i")
                    if i in indices_in_batch and i not in verdicts_by_i:
                        verdicts_by_i[i] = v
                for r in batch:
                    if r["i"] not in verdicts_by_i:
                        verdicts_by_i[r["i"]] = {
                            "i": r["i"], "keep": False, "score": 0,
                            "category": "other",
                            "affected_stocks": [], "affected_industries": [],
                            "rich_summary": "", "reason": "LLM missing verdict",
                        }
                if done % 5 == 0 or done == len(batches):
                    print(f"  batches: {done}/{len(batches)}  "
                          f"in/out: {totals['input_tokens']}/{totals['output_tokens']}  "
                          f"elapsed: {time.time() - t0:.1f}s")
            except Exception as e:
                totals["failed_batches"] += 1
                batch = futs[fut]
                for r in batch:
                    verdicts_by_i[r["i"]] = {
                        "i": r["i"], "keep": False, "score": 0,
                        "category": "other",
                        "affected_stocks": [], "affected_industries": [],
                        "rich_summary": "", "reason": f"batch_error: {e}",
                    }
                print(f"  [BATCH FAIL] {e}")

    verdicts = [verdicts_by_i[i] for i in sorted(verdicts_by_i)]
    totals["elapsed_sec"] = round(time.time() - t0, 1)
    return verdicts, totals


def _listify(x) -> List[str]:
    if isinstance(x, list):
        return [str(v).strip() for v in x if str(v).strip()]
    if isinstance(x, str):
        return [v.strip() for v in x.split(",") if v.strip()]
    return []


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=str(HERE / "news_22apr_cheapest_model.xlsx"),
                    help="Input Excel — must have a 'kept' sheet.")
    ap.add_argument("--sheet", default="kept")
    ap.add_argument("--output", default=None)
    ap.add_argument("--model", default="gpt-4.1-mini")
    ap.add_argument("--batch-size", type=int, default=6,
                    help="Fewer items per call because output is richer.")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--min-score", type=int, default=7,
                    help="Final cut (score>=this AND keep=true).")
    ap.add_argument("--limit", type=int, default=0)
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    in_path = Path(args.input)
    if not in_path.exists():
        print(f"ERROR: {in_path} not found.")
        return 1
    model_tag = {
        "gpt-4.1-nano": "cheapest_model",
        "gpt-4o-mini":  "cheap_model",
        "gpt-4.1-mini": "mini_model",
    }.get(args.model, args.model.replace("-", "_"))
    out_path = Path(args.output) if args.output else in_path.with_name(
        in_path.stem.replace("_cheapest_model", "").replace("_screened", "")
        + f"_{model_tag}_refined.xlsx"
    )

    df = pd.read_excel(in_path, sheet_name=args.sheet).fillna("")
    if args.limit:
        df = df.head(args.limit).copy()
    df = df.reset_index(drop=True)
    df["i"] = df.index.astype(int)

    # Rename any existing stage-1 columns so we can preserve them as "prior_*"
    for col in ("score", "keep", "reason", "sectors", "companies"):
        if col in df.columns:
            df = df.rename(columns={col: f"prior_{col}"})

    items: List[Dict[str, Any]] = df[["i", "source", "title", "summary"]].to_dict("records")
    print(f"Input rows: {len(items)}  from {in_path.name}::{args.sheet}")

    verdicts, totals = refine_all(
        items, model=args.model, batch_size=args.batch_size, workers=args.workers,
    )

    vdf = pd.DataFrame(verdicts)
    vdf["affected_stocks_list"] = vdf.get("affected_stocks", pd.Series([[]] * len(vdf))).apply(_listify)
    vdf["affected_industries_list"] = vdf.get("affected_industries", pd.Series([[]] * len(vdf))).apply(_listify)
    vdf["affected_stocks"] = vdf["affected_stocks_list"].apply(lambda xs: ", ".join(xs))
    vdf["affected_industries"] = vdf["affected_industries_list"].apply(lambda xs: ", ".join(xs))
    for col, default in (("keep", False), ("score", 0), ("category", "other"),
                         ("rich_summary", ""), ("reason", "")):
        if col not in vdf.columns:
            vdf[col] = default
    vdf["score"] = pd.to_numeric(vdf["score"], errors="coerce").fillna(0).astype(int)
    vdf["keep"] = vdf["keep"].fillna(False).astype(bool)

    # Force category to taxonomy — any stray value falls back to "other"
    vdf["category"] = vdf["category"].apply(
        lambda c: str(c).strip().lower().replace(" ", "_") if c else "other"
    )
    vdf["category"] = vdf["category"].where(vdf["category"].isin(CATEGORIES), "other")

    merged = df.merge(
        vdf[["i", "keep", "score", "category",
             "affected_stocks", "affected_industries",
             "affected_stocks_list", "affected_industries_list",
             "rich_summary", "reason"]],
        on="i", how="left",
    )

    kept = merged[(merged["keep"]) & (merged["score"] >= args.min_score)].copy()
    dropped = merged[~((merged["keep"]) & (merged["score"] >= args.min_score))].copy()

    kept = kept.sort_values(["score", "published_at_ist"], ascending=[False, False])
    dropped = dropped.sort_values(["score", "published_at_ist"], ascending=[False, False])

    out_cols_main = [
        "score", "category", "affected_stocks", "affected_industries",
        "rich_summary", "source", "published_at_ist", "title", "summary",
        "link", "reason",
    ]
    # For prior-stage audit
    prior_cols = [c for c in ("prior_score", "prior_reason", "prior_sectors",
                              "prior_companies") if c in merged.columns]
    out_cols_all = out_cols_main + ["keep"] + prior_cols
    out_cols_main = [c for c in out_cols_main if c in merged.columns]
    out_cols_all = [c for c in out_cols_all if c in merged.columns]

    # Cost estimate
    inp_cost = totals["input_tokens"] / 1_000_000 * PRICE_PER_1M_INPUT.get(args.model, 0.0)
    out_cost = totals["output_tokens"] / 1_000_000 * PRICE_PER_1M_OUTPUT.get(args.model, 0.0)
    total_cost = inp_cost + out_cost

    # Breakdowns
    cat_breakdown = (
        kept.groupby("category").size().reset_index(name="items")
            .sort_values("items", ascending=False)
    )
    ind_breakdown = (
        kept.explode("affected_industries_list")
            .assign(ind=lambda d: d["affected_industries_list"].str.strip())
            .query("ind != '' and ind == ind")
            .groupby("ind").size().reset_index(name="items")
            .sort_values("items", ascending=False)
            .rename(columns={"ind": "industry"})
    )
    stock_breakdown = (
        kept.explode("affected_stocks_list")
            .assign(stk=lambda d: d["affected_stocks_list"].str.strip())
            .query("stk != '' and stk == stk")
            .groupby("stk").size().reset_index(name="mentions")
            .sort_values("mentions", ascending=False)
            .rename(columns={"stk": "stock"})
    )

    stats_df = pd.DataFrame([
        {"metric": "model",            "value": args.model},
        {"metric": "input_rows",       "value": len(items)},
        {"metric": "kept",             "value": int(len(kept))},
        {"metric": "dropped",          "value": int(len(dropped))},
        {"metric": "keep_threshold",   "value": f"keep=true AND score>={args.min_score}"},
        {"metric": "batches",          "value": int((len(items) + args.batch_size - 1) // args.batch_size)},
        {"metric": "failed_batches",   "value": totals["failed_batches"]},
        {"metric": "input_tokens",     "value": totals["input_tokens"]},
        {"metric": "output_tokens",    "value": totals["output_tokens"]},
        {"metric": "estimated_cost_usd", "value": f"${total_cost:.4f}"},
        {"metric": "elapsed_sec",      "value": totals["elapsed_sec"]},
    ])

    with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
        kept[out_cols_main].to_excel(xw, sheet_name="kept", index=False)
        dropped[out_cols_main].to_excel(xw, sheet_name="dropped", index=False)
        merged[out_cols_all].to_excel(xw, sheet_name="all_with_verdict", index=False)
        cat_breakdown.to_excel(xw, sheet_name="by_category", index=False)
        ind_breakdown.to_excel(xw, sheet_name="by_industry", index=False)
        stock_breakdown.to_excel(xw, sheet_name="top_stocks", index=False)
        stats_df.to_excel(xw, sheet_name="run_stats", index=False)

    csv_path = out_path.with_suffix(".csv")
    kept[out_cols_main].to_csv(csv_path, index=False)

    print()
    print("=" * 60)
    print(f"Input rows         : {len(items)}")
    print(f"Kept (score>={args.min_score})    : {len(kept)}  "
          f"({len(kept) / max(len(items), 1) * 100:.1f}%)")
    print(f"Dropped            : {len(dropped)}")
    print(f"Failed batches     : {totals['failed_batches']}")
    print(f"Tokens (in/out)    : {totals['input_tokens']:,} / {totals['output_tokens']:,}")
    print(f"Cost estimate      : ${total_cost:.4f}  ({args.model})")
    print(f"Elapsed            : {totals['elapsed_sec']}s")
    print()
    print("Category breakdown (kept):")
    for _, r in cat_breakdown.iterrows():
        print(f"  {r['category']:22s} {int(r['items']):>4}")
    print()
    print("Top 15 kept:")
    for _, r in kept.head(15).iterrows():
        stocks = str(r.get("affected_stocks") or "")[:40]
        inds = str(r.get("affected_industries") or "")[:30]
        print(f"  s={int(r['score']):>2}  {str(r.get('category') or ''):20s}  "
              f"stocks={stocks:40s}  inds={inds:30s}  | {str(r.get('title') or '')[:70]}")
    print()
    print(f"Wrote:\n  {out_path}\n  {csv_path}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
