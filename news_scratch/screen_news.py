"""
Stage-1 relevance screen over the deduped Excel using the cheapest OpenAI
model (gpt-4.1-nano by default).

Goal: keep items that could move a stock / sector / industry view, drop
generic macro, political, lifestyle and opinion noise.

Run:
  cd news_scratch
  .venv/bin/python screen_news.py --input news_22apr.xlsx
  .venv/bin/python screen_news.py --input news_22apr.xlsx --model gpt-4.1-nano --min-score 6

Loads OPENAI_API_KEY from:
  1. process env
  2. news_scratch/.env
  3. wesbiteBE/.env
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

# Trust macOS keychain for SSL intercepting proxies
try:
    import truststore  # type: ignore
    truststore.inject_into_ssl()
except Exception:
    pass

import pandas as pd
from dotenv import load_dotenv

HERE = Path(__file__).resolve().parent

# Try local .env, then wesbiteBE/.env
for candidate in [HERE / ".env", HERE.parent / "wesbiteBE" / ".env"]:
    if candidate.exists():
        load_dotenv(candidate)
        break

from openai import OpenAI  # noqa: E402

API_KEY = os.environ.get("OPENAI_API_KEY")
if not API_KEY:
    sys.exit(
        "ERROR: OPENAI_API_KEY is not set.\n"
        "  Fix: either   export OPENAI_API_KEY=sk-...   in this shell,\n"
        "  or create     news_scratch/.env             with OPENAI_API_KEY=sk-..."
    )

client = OpenAI(api_key=API_KEY)

# Approximate per-1M-token prices for cost estimates. These change; keep as
# rough guide only.
PRICE_PER_1M_INPUT = {
    "gpt-4.1-nano":  0.10,
    "gpt-4.1-mini":  0.40,
    "gpt-4o-mini":   0.15,
}
PRICE_PER_1M_OUTPUT = {
    "gpt-4.1-nano":  0.40,
    "gpt-4.1-mini":  1.60,
    "gpt-4o-mini":   0.60,
}

# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """You are an equity-research triage assistant for Indian \
investors. You receive a batch of news items (title + short summary) and \
decide which ones are worth reading for stock/sector investment decisions.

KEEP (return keep=true) when an item is about ANY of the following:
  • A specific listed company or business group (earnings, orders, deals, \
    M&A, product launches, leadership changes, regulatory action, \
    credit-rating changes, capacity expansion, layoffs, legal disputes).
  • A specific sector or industry moving (auto, banking, IT, pharma, FMCG, \
    energy, telecom, defence, infra, real estate, chemicals, cement, \
    metals, agri, renewable, retail, consumer tech, etc.) — including \
    demand/consumption trends, production data, input-cost changes, \
    export/import shifts, capex cycles.
  • Government / regulator / court action targeting a specific sector or \
    company (e.g., GST cut on autos, new telecom spectrum rules, SEBI \
    action on NBFCs, oil & gas pricing policy).
  • Commodity or currency moves that clearly flow into an Indian industry \
    (crude → paints/airlines/OMCs, steel → autos/infra, rupee → IT/pharma).
  • Foreign news that has a clear, direct read-through to a listed Indian \
    company or sector (e.g., China export curbs on APIs → Indian pharma, \
    US tariffs on Indian textiles).

DROP (return keep=false) for:
  • Pure macro with no sector tie (headline GDP, CPI, IIP, unemployment, \
    fiscal deficit) unless the article clearly points at an industry.
  • Generic daily market wraps ("Sensex ends 200 pts higher", "Nifty \
    closes flat", "FII/DII activity" summaries, "top gainers/losers").
  • Personal-finance tips, SIP guides, "top 5 mutual funds" listicles, \
    tax-saving columns, credit-card explainers.
  • Opinion / editorial / columns without a concrete stock or sector event.
  • Politics, crime, sports, entertainment, celebrity, lifestyle, visual \
    stories, horoscopes.
  • Foreign news with no India linkage (US elections, European politics, \
    purely international corporate moves, foreign sports).
  • Crypto / Web3 price moves (unless explicit Indian regulatory action).
  • Pre-market/opening-bell "stocks to watch" lists and daily intraday \
    technical calls — too ephemeral for investment decisions.

Scoring (0–10):
  10  Definitively stock/sector moving (earnings surprise, major order \
      win, material M&A, policy shock).
   7–9  Important sector / company signal; investor would want to read.
   4–6  Mildly relevant; good context but not urgent.
   1–3  Tangentially relevant.
   0   Noise.

Return a JSON OBJECT with a single key "verdicts" whose value is a JSON \
ARRAY, one entry per input item, in the same order, of the form:
{
  "i": <int index you were given>,
  "keep": <bool>,
  "score": <int 0-10>,
  "sectors": ["<short sector tag>", ...],    // may be empty
  "companies": ["<company name>", ...],      // Indian-listed names if \
identifiable, else []
  "reason": "<<= 20 words why keep or drop>"
}
Return NOTHING outside the JSON object. No markdown.
"""


# ---------------------------------------------------------------------------
# Batch screening
# ---------------------------------------------------------------------------
def _format_batch(batch: List[Dict[str, Any]]) -> str:
    lines = []
    for row in batch:
        title = str(row.get("title") or "").strip()
        summary = str(row.get("summary") or "").strip()
        source = str(row.get("source") or "").strip()
        # Title + summary; trim summary to stay cheap.
        if len(summary) > 400:
            summary = summary[:400] + "…"
        lines.append(
            f"[i={row['i']}] ({source}) {title}\n    summary: {summary or '(none)'}"
        )
    return "\n".join(lines)


def _call_llm(
    model: str,
    batch: List[Dict[str, Any]],
    max_retries: int = 3,
) -> tuple[List[Dict[str, Any]], int, int]:
    """Return (verdicts, input_tokens, output_tokens)."""
    user_msg = (
        f"Screen the following {len(batch)} items. Return exactly "
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
                max_tokens=900,
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
            wait = 1.5 * (attempt + 1)
            time.sleep(wait)
    raise RuntimeError(f"LLM failed after {max_retries} retries: {last_err}")


def screen_all(
    items: List[Dict[str, Any]],
    model: str,
    batch_size: int = 10,
    workers: int = 8,
) -> tuple[List[Dict[str, Any]], Dict[str, int]]:
    batches = [items[i:i + batch_size] for i in range(0, len(items), batch_size)]
    verdicts_by_i: Dict[int, Dict[str, Any]] = {}
    totals = {"input_tokens": 0, "output_tokens": 0, "failed_batches": 0}
    t0 = time.time()

    def _work(batch: List[Dict[str, Any]]):
        vs, inp, out = _call_llm(model, batch)
        return batch, vs, inp, out

    print(f"Screening {len(items)} items  model={model}  batches={len(batches)}  "
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
                # Merge verdicts back by their `i` index
                indices_in_batch = {r["i"] for r in batch}
                seen = 0
                for v in verdicts:
                    i = v.get("i")
                    if i in indices_in_batch and i not in verdicts_by_i:
                        verdicts_by_i[i] = v
                        seen += 1
                # If model didn't return one per item, fill gaps as "unknown"
                for r in batch:
                    if r["i"] not in verdicts_by_i:
                        verdicts_by_i[r["i"]] = {
                            "i": r["i"], "keep": False, "score": 0,
                            "sectors": [], "companies": [],
                            "reason": "LLM response missing for this index",
                        }
                if done % 10 == 0 or done == len(batches):
                    print(f"  batches done: {done}/{len(batches)}  "
                          f"in/out tokens: {totals['input_tokens']}/{totals['output_tokens']}  "
                          f"elapsed: {time.time() - t0:.1f}s")
            except Exception as e:
                totals["failed_batches"] += 1
                batch = futs[fut]
                for r in batch:
                    verdicts_by_i[r["i"]] = {
                        "i": r["i"], "keep": False, "score": 0,
                        "sectors": [], "companies": [],
                        "reason": f"batch_error: {type(e).__name__}: {e}",
                    }
                print(f"  [BATCH FAIL] {e}")

    verdicts = [verdicts_by_i[i] for i in sorted(verdicts_by_i)]
    totals["elapsed_sec"] = round(time.time() - t0, 1)
    return verdicts, totals


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=str(HERE / "news_22apr.xlsx"),
                    help="Input Excel — must have a 'deduped' sheet.")
    ap.add_argument("--sheet", default="deduped")
    ap.add_argument("--output", default=None)
    ap.add_argument("--model", default="gpt-4.1-nano")
    ap.add_argument("--batch-size", type=int, default=10)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--min-score", type=int, default=6,
                    help="Final cut threshold on top of keep=true")
    ap.add_argument("--limit", type=int, default=0,
                    help="Only screen the first N rows (for quick testing)")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    in_path = Path(args.input)
    if not in_path.exists():
        print(f"ERROR: {in_path} not found.")
        return 1
    # Default output name encodes the model family so back-to-back runs with
    # different models don't overwrite each other (e.g. _cheapest_model.xlsx
    # for nano, _refined.xlsx if we later run mini on the survivors).
    model_tag = {
        "gpt-4.1-nano": "cheapest_model",
        "gpt-4o-mini":  "cheap_model",
        "gpt-4.1-mini": "mini_model",
    }.get(args.model, args.model.replace("-", "_"))
    out_path = Path(args.output) if args.output else in_path.with_name(
        f"{in_path.stem}_{model_tag}.xlsx"
    )

    df = pd.read_excel(in_path, sheet_name=args.sheet).fillna("")
    if args.limit:
        df = df.head(args.limit).copy()
    df = df.reset_index(drop=True)
    df["i"] = df.index.astype(int)

    items: List[Dict[str, Any]] = df[["i", "source", "title", "summary"]].to_dict("records")
    print(f"Input rows: {len(items)}  from {in_path.name}::{args.sheet}")

    verdicts, totals = screen_all(
        items, model=args.model, batch_size=args.batch_size, workers=args.workers,
    )

    vdf = pd.DataFrame(verdicts)
    # Normalize shape
    for col in ("sectors", "companies"):
        if col in vdf.columns:
            vdf[col] = vdf[col].apply(
                lambda x: ", ".join(x) if isinstance(x, list) else (str(x) if x else "")
            )
        else:
            vdf[col] = ""
    for col in ("keep", "score", "reason"):
        if col not in vdf.columns:
            vdf[col] = False if col == "keep" else (0 if col == "score" else "")

    merged = df.merge(vdf, on="i", how="left")
    merged["score"] = pd.to_numeric(merged["score"], errors="coerce").fillna(0).astype(int)
    merged["keep"] = merged["keep"].fillna(False).astype(bool)

    kept = merged[(merged["keep"]) & (merged["score"] >= args.min_score)].copy()
    dropped = merged[~((merged["keep"]) & (merged["score"] >= args.min_score))].copy()

    kept = kept.sort_values(["score", "published_at_ist"], ascending=[False, False])
    dropped = dropped.sort_values(["score", "published_at_ist"], ascending=[False, False])

    out_cols = [
        "score", "keep", "sectors", "companies", "reason",
        "source", "published_at_ist", "title", "summary", "link",
    ]
    # Column subset (only existing)
    out_cols = [c for c in out_cols if c in merged.columns]

    # Cost estimate
    inp_cost = totals["input_tokens"] / 1_000_000 * PRICE_PER_1M_INPUT.get(args.model, 0.0)
    out_cost = totals["output_tokens"] / 1_000_000 * PRICE_PER_1M_OUTPUT.get(args.model, 0.0)
    total_cost = inp_cost + out_cost

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

    sector_breakdown = (
        kept.assign(_s=kept["sectors"].fillna("").str.split(","))
            .explode("_s")
            .assign(_s=lambda d: d["_s"].str.strip())
            .query("_s != ''")
            .groupby("_s").size().sort_values(ascending=False)
            .reset_index().rename(columns={"_s": "sector", 0: "items"})
    )
    if sector_breakdown.empty:
        sector_breakdown = pd.DataFrame(columns=["sector", "items"])

    with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
        kept[out_cols].to_excel(xw, sheet_name="kept", index=False)
        dropped[out_cols].to_excel(xw, sheet_name="dropped", index=False)
        merged[out_cols].to_excel(xw, sheet_name="all_with_verdict", index=False)
        sector_breakdown.to_excel(xw, sheet_name="kept_by_sector", index=False)
        stats_df.to_excel(xw, sheet_name="run_stats", index=False)

    csv_path = out_path.with_suffix(".csv")
    kept[out_cols].to_csv(csv_path, index=False)

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
    print("Top 10 kept:")
    for _, r in kept.head(10).iterrows():
        print(f"  score={int(r['score']):>2}  {str(r.get('source',''))[:22]:22s}  "
              f"{str(r.get('title',''))[:85]}")
    print()
    print(f"Wrote:\n  {out_path}\n  {csv_path}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
