"""
Stage-3 enrichment: drop earnings, scrape article bodies, regenerate
user-ready summaries with gpt-4.1-mini.

Pipeline:
  read news_22apr_mini_model_refined.xlsx::kept
  drop category == 'earnings'  (user handles those separately)
  for each remaining row:
      fetch article HTML (browser UA, follow redirects, truststore SSL)
      trafilatura.extract -> plain-text article body
      if scrape fails / < 300 chars -> fall back to title+RSS summary
      call gpt-4.1-mini with strict keep/drop prompt + rich schema:
          keep, score, category, affected_stocks, affected_industries,
          ai_summary (user-ready, 2-4 sentences, with numbers),
          key_numbers, implication, direction, reason
  write news_22apr_enriched.xlsx

Run:
  cd news_scratch
  .venv/bin/python enrich_news.py
  .venv/bin/python enrich_news.py --input news_22apr_mini_model_refined.xlsx --min-score 7
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

try:
    import truststore  # type: ignore
    truststore.inject_into_ssl()
except Exception:
    pass

import pandas as pd
import requests
import trafilatura
from dotenv import load_dotenv

HERE = Path(__file__).resolve().parent

for candidate in [HERE / ".env", HERE.parent / "wesbiteBE" / ".env"]:
    if candidate.exists():
        load_dotenv(candidate)
        break

from openai import OpenAI  # noqa: E402

API_KEY = os.environ.get("OPENAI_API_KEY")
if not API_KEY:
    sys.exit("ERROR: OPENAI_API_KEY is not set (export or put in news_scratch/.env)")
client = OpenAI(api_key=API_KEY)

PRICE_PER_1M_INPUT = {"gpt-4.1-nano": 0.10, "gpt-4.1-mini": 0.40, "gpt-4o-mini": 0.15}
PRICE_PER_1M_OUTPUT = {"gpt-4.1-nano": 0.40, "gpt-4.1-mini": 1.60, "gpt-4o-mini": 0.60}

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

CATEGORIES = [
    "order_win", "deal_mna", "analyst_call", "product_capex",
    "regulation_sector", "govt_spending", "macro_to_sector",
    "demand_trend", "legal_dispute", "management",
    "capital_raise_rating", "other",
]

# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------
_SESSION = requests.Session()
_SESSION.headers.update({
    "User-Agent": UA,
    "Accept-Language": "en-IN,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.google.com/",
})

_MIN_ARTICLE_CHARS = 300

# Sometimes Google News landing pages contain a <meta http-equiv="refresh">
# or a c-wiz data-n-au attribute with the destination. Try to extract it.
_META_REFRESH = re.compile(
    r'<meta[^>]*http-equiv=["\']?refresh["\']?[^>]*url=([^"\'>]+)',
    re.IGNORECASE,
)
_DATA_N_AU = re.compile(r'data-n-au="(https?://[^"]+)"', re.IGNORECASE)


def _resolve_google_news(url: str, html: str) -> Optional[str]:
    """Return the real publisher URL embedded in a Google News landing page, or None."""
    m = _META_REFRESH.search(html)
    if m:
        return m.group(1).strip()
    m = _DATA_N_AU.search(html)
    if m:
        return m.group(1).strip()
    return None


def fetch_article(link: str, timeout: int = 20) -> Dict[str, Any]:
    """Return {text, status, final_url, chars}. Never raises."""
    if not link:
        return {"text": "", "status": "no_link", "final_url": "", "chars": 0}
    try:
        resp = _SESSION.get(link, timeout=timeout, allow_redirects=True)
    except requests.RequestException as e:
        return {"text": "", "status": f"req_err_{type(e).__name__}",
                "final_url": link, "chars": 0}

    final_url = resp.url

    # Google News landing page — try once to follow to real publisher.
    if "news.google.com" in (urlparse(final_url).netloc or "").lower():
        real = _resolve_google_news(final_url, resp.text or "")
        if real and real != final_url:
            try:
                resp2 = _SESSION.get(real, timeout=timeout, allow_redirects=True)
                if resp2.status_code == 200 and resp2.text:
                    final_url = resp2.url
                    resp = resp2
            except requests.RequestException:
                pass

    status_code = resp.status_code
    if status_code != 200:
        return {"text": "", "status": f"http_{status_code}",
                "final_url": final_url, "chars": 0}

    html = resp.text or ""
    if not html:
        return {"text": "", "status": "empty_body",
                "final_url": final_url, "chars": 0}

    text = trafilatura.extract(
        html,
        include_comments=False,
        include_tables=False,
        favor_precision=True,
        deduplicate=True,
    ) or ""
    text = text.strip()

    if len(text) < _MIN_ARTICLE_CHARS:
        return {"text": text, "status": "short",
                "final_url": final_url, "chars": len(text)}

    # Cap to keep token spend bounded (~1500 tokens of input).
    if len(text) > 6000:
        text = text[:6000] + "…"
    return {"text": text, "status": "ok",
            "final_url": final_url, "chars": len(text)}


# ---------------------------------------------------------------------------
# LLM
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = f"""You are a senior Indian equity-research analyst \
preparing user-ready news alerts. A news item has already been triaged \
twice; you now receive its title, source, RSS summary, a prior short \
summary, and (usually) the full article body. Your job is FINAL triage.

BE BRUTAL. Only KEEP items that are likely to:
  • move an Indian listed stock by at least a few percent, OR
  • materially change the view on a specific sector / industry, OR
  • represent a genuinely important macro-to-sector development \
    (e.g. policy, commodity / currency shock with clear read-through).

FORCED DROP (always keep=false):
  • Anything whose core story is quarterly or annual earnings of a \
    listed company — user already handles earnings separately. \
    Reason: "earnings handled elsewhere".
  • Generic market wraps, index moves, FII/DII flows, "top gainers".
  • Soft corporate PR (forms committee, signs MoU without numbers, \
    generic launch, award announcement) with no material impact.
  • Opinion / editorial / podcast / column / visual story.
  • Foreign corporate or political news without a clear, direct \
    Indian-stock or Indian-sector read-through.
  • Stock-pick lists, technical-call shows, "buy-sell-hold" daily calls \
    without a specific broker target.
  • Personal-finance explainers, tax how-tos, SIP guides.
  • Pure macro prints (GDP, CPI, IIP, unemployment) without a sector tie.
  • Speculation / preview ("what to expect from X") without guidance \
    or concrete numbers.

KEEP only when ALL of the following hold:
  • The article states a CONCRETE event (order win / deal / JV / \
    regulatory action / capex / product / management change / legal \
    ruling / demand shift / policy move / rating or target change).
  • There is a LIKELY, IDENTIFIABLE stock or sector impact.
  • You have enough detail to write an accurate 2–4 sentence summary \
    with at least one or two concrete numbers (value, %, rating, date).

CATEGORIES (choose exactly one; do NOT use "earnings"):
  {", ".join(CATEGORIES)}

Return a JSON OBJECT with these fields (no markdown, nothing outside):
{{
  "keep": <bool>,
  "score": <int 0-10>,             // 10 = clearly stock-moving; <6 => drop
  "category": "<one of above>",
  "affected_stocks": ["<Indian listed co>", ...],
  "affected_industries": ["<lowercased sector tag>", ...],
  "ai_summary": "<2-4 sentences, preserves key numbers, names parties, \
dates, values — write as a user-facing alert: clear, direct, useful>",
  "key_numbers": "<comma-separated list of important numbers: \
e.g. 'Rs 350.23 cr, +9.6%, FY26'>",
  "implication": "<one sentence — what this means for the stock/sector. \
State direction and rough magnitude.>",
  "direction": "<positive|negative|neutral|mixed>",
  "reason": "<<= 20 words why keep or drop>"
}}
"""


def _build_user_msg(row: Dict[str, Any], article_text: str) -> str:
    title = str(row.get("title") or "").strip()
    source = str(row.get("source") or "").strip()
    pub = str(row.get("published_at_ist") or "").strip()
    rss_summary = str(row.get("summary") or "").strip()
    prior = str(row.get("rich_summary") or "").strip()

    if article_text and len(article_text) >= _MIN_ARTICLE_CHARS:
        article_block = f"ARTICLE BODY:\n{article_text}"
    else:
        article_block = (
            "(Article body unavailable or too short. Use title + RSS "
            "summary + prior summary below.)"
        )

    return (
        f"TITLE: {title}\n"
        f"SOURCE: {source}\n"
        f"PUBLISHED: {pub}\n"
        f"RSS SUMMARY: {rss_summary or '(none)'}\n"
        f"PRIOR SHORT SUMMARY: {prior or '(none)'}\n\n"
        f"{article_block}"
    )


def _call_llm(
    model: str,
    row: Dict[str, Any],
    article_text: str,
    max_retries: int = 3,
) -> tuple[Dict[str, Any], int, int]:
    user_msg = _build_user_msg(row, article_text)
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
                max_tokens=700,
            )
            content = resp.choices[0].message.content or "{}"
            data = json.loads(content)
            usage = resp.usage
            return (
                data,
                getattr(usage, "prompt_tokens", 0) or 0,
                getattr(usage, "completion_tokens", 0) or 0,
            )
        except Exception as e:
            last_err = e
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"LLM failed after {max_retries} retries: {last_err}")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def _listify(x) -> List[str]:
    if isinstance(x, list):
        return [str(v).strip() for v in x if str(v).strip()]
    if isinstance(x, str):
        return [v.strip() for v in x.split(",") if v.strip()]
    return []


def process_one(
    row: Dict[str, Any],
    model: str,
) -> Dict[str, Any]:
    link = row.get("link", "")
    fetch = fetch_article(link)
    try:
        verdict, inp_tok, out_tok = _call_llm(model, row, fetch["text"])
    except Exception as e:
        return {
            **row,
            "scrape_status": fetch["status"],
            "scraped_chars": fetch["chars"],
            "final_url": fetch["final_url"],
            "keep": False,
            "score": 0,
            "category": "other",
            "affected_stocks": "",
            "affected_industries": "",
            "ai_summary": "",
            "key_numbers": "",
            "implication": "",
            "direction": "",
            "reason": f"llm_err: {type(e).__name__}: {e}",
            "input_tokens": 0,
            "output_tokens": 0,
        }

    stocks = _listify(verdict.get("affected_stocks"))
    industries = _listify(verdict.get("affected_industries"))
    cat = str(verdict.get("category") or "other").strip().lower().replace(" ", "_")
    if cat not in CATEGORIES:
        cat = "other"

    return {
        **row,
        "scrape_status": fetch["status"],
        "scraped_chars": fetch["chars"],
        "final_url": fetch["final_url"],
        "keep": bool(verdict.get("keep")),
        "score": int(verdict.get("score") or 0),
        "category": cat,
        "affected_stocks": ", ".join(stocks),
        "affected_industries": ", ".join(industries),
        "ai_summary": str(verdict.get("ai_summary") or "").strip(),
        "key_numbers": str(verdict.get("key_numbers") or "").strip(),
        "implication": str(verdict.get("implication") or "").strip(),
        "direction": str(verdict.get("direction") or "").strip().lower(),
        "reason": str(verdict.get("reason") or "").strip(),
        "input_tokens": inp_tok,
        "output_tokens": out_tok,
    }


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=str(HERE / "news_22apr_mini_model_refined.xlsx"))
    ap.add_argument("--sheet", default="kept")
    ap.add_argument("--output", default=None)
    ap.add_argument("--model", default="gpt-4.1-mini")
    ap.add_argument("--workers", type=int, default=8,
                    help="Concurrent fetch+LLM workers (one end-to-end pipeline per worker)")
    ap.add_argument("--min-score", type=int, default=7,
                    help="Final cut (keep=true AND score>=this)")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--drop-category", default="earnings",
                    help="Comma-separated list of categories to drop before processing")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    in_path = Path(args.input)
    if not in_path.exists():
        print(f"ERROR: {in_path} not found.")
        return 1
    out_path = Path(args.output) if args.output else in_path.with_name(
        in_path.stem.replace("_mini_model_refined", "") + "_enriched.xlsx"
    )

    df = pd.read_excel(in_path, sheet_name=args.sheet).fillna("")
    total_input = len(df)

    drop_cats = {c.strip() for c in args.drop_category.split(",") if c.strip()}
    if drop_cats and "category" in df.columns:
        before = len(df)
        df = df[~df["category"].isin(drop_cats)].copy()
        print(f"Dropped categories {sorted(drop_cats)}: {before - len(df)} rows removed")

    df = df.reset_index(drop=True)
    df["i"] = df.index.astype(int)

    # Preserve prior LLM fields under prior_* to compare before/after
    rename = {}
    for c in ("score", "keep", "reason", "category", "affected_stocks",
              "affected_industries", "rich_summary"):
        if c in df.columns:
            rename[c] = f"prior_{c}"
    if rename:
        df = df.rename(columns=rename)

    if args.limit:
        df = df.head(args.limit).copy()

    rows = df.to_dict("records")
    print(f"Input rows (post-filter): {len(rows)}  from {in_path.name}::{args.sheet}")
    print(f"Model: {args.model}   workers: {args.workers}")
    print(f"Scraping + LLM (one call per row)…\n")

    results: List[Dict[str, Any]] = []
    totals = {"input_tokens": 0, "output_tokens": 0, "scrape_ok": 0,
              "scrape_short": 0, "scrape_blocked": 0, "scrape_err": 0,
              "llm_err": 0}
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = {pool.submit(process_one, r, args.model): r for r in rows}
        done = 0
        for fut in as_completed(futs):
            done += 1
            res = fut.result()
            results.append(res)
            totals["input_tokens"] += res.get("input_tokens", 0) or 0
            totals["output_tokens"] += res.get("output_tokens", 0) or 0
            st = str(res.get("scrape_status", ""))
            if st == "ok":
                totals["scrape_ok"] += 1
            elif st == "short":
                totals["scrape_short"] += 1
            elif st.startswith("http_4") or st.startswith("http_5"):
                totals["scrape_blocked"] += 1
            elif st.startswith("req_err") or st == "empty_body":
                totals["scrape_err"] += 1
            if "llm_err" in str(res.get("reason", "")):
                totals["llm_err"] += 1
            if done % 10 == 0 or done == len(rows):
                elapsed = time.time() - t0
                print(f"  {done}/{len(rows)}  ok={totals['scrape_ok']} "
                      f"short={totals['scrape_short']} blocked={totals['scrape_blocked']} "
                      f"err={totals['scrape_err']}  "
                      f"tok in/out={totals['input_tokens']}/{totals['output_tokens']}  "
                      f"elapsed={elapsed:.1f}s")

    rdf = pd.DataFrame(results)
    rdf["score"] = pd.to_numeric(rdf["score"], errors="coerce").fillna(0).astype(int)
    rdf["keep"] = rdf["keep"].fillna(False).astype(bool)

    kept = rdf[(rdf["keep"]) & (rdf["score"] >= args.min_score)].copy()
    dropped = rdf[~((rdf["keep"]) & (rdf["score"] >= args.min_score))].copy()

    kept = kept.sort_values(["score", "published_at_ist"], ascending=[False, False])
    dropped = dropped.sort_values(["score", "published_at_ist"], ascending=[False, False])

    main_cols = [
        "score", "direction", "category",
        "affected_stocks", "affected_industries",
        "ai_summary", "implication", "key_numbers",
        "source", "published_at_ist", "title", "link",
        "scrape_status", "scraped_chars", "reason",
    ]
    audit_cols = [c for c in ("prior_score", "prior_category", "prior_rich_summary")
                  if c in rdf.columns]
    all_cols = main_cols + ["keep"] + audit_cols
    main_cols = [c for c in main_cols if c in rdf.columns]
    all_cols = [c for c in all_cols if c in rdf.columns]

    # Breakdowns
    cat_breakdown = (
        kept.groupby("category").size().reset_index(name="items")
            .sort_values("items", ascending=False)
    )
    dir_breakdown = (
        kept.groupby("direction").size().reset_index(name="items")
            .sort_values("items", ascending=False)
    )
    ind_breakdown = (
        kept.assign(_s=kept["affected_industries"].fillna("").str.split(","))
            .explode("_s")
            .assign(_s=lambda d: d["_s"].str.strip())
            .query("_s != ''")
            .groupby("_s").size().reset_index(name="items")
            .sort_values("items", ascending=False)
            .rename(columns={"_s": "industry"})
    )
    stock_breakdown = (
        kept.assign(_s=kept["affected_stocks"].fillna("").str.split(","))
            .explode("_s")
            .assign(_s=lambda d: d["_s"].str.strip())
            .query("_s != ''")
            .groupby("_s").size().reset_index(name="mentions")
            .sort_values("mentions", ascending=False)
            .rename(columns={"_s": "stock"})
    )
    scrape_breakdown = (
        rdf.groupby("scrape_status").size().reset_index(name="rows")
           .sort_values("rows", ascending=False)
    )

    inp_cost = totals["input_tokens"] / 1_000_000 * PRICE_PER_1M_INPUT.get(args.model, 0.0)
    out_cost = totals["output_tokens"] / 1_000_000 * PRICE_PER_1M_OUTPUT.get(args.model, 0.0)
    total_cost = inp_cost + out_cost

    stats_df = pd.DataFrame([
        {"metric": "model", "value": args.model},
        {"metric": "input_rows_before_category_drop", "value": total_input},
        {"metric": "input_rows_after_category_drop", "value": len(rdf)},
        {"metric": "dropped_categories", "value": ",".join(sorted(drop_cats))},
        {"metric": "kept", "value": int(len(kept))},
        {"metric": "dropped", "value": int(len(dropped))},
        {"metric": "keep_threshold", "value": f"keep=true AND score>={args.min_score}"},
        {"metric": "scrape_ok", "value": totals["scrape_ok"]},
        {"metric": "scrape_short", "value": totals["scrape_short"]},
        {"metric": "scrape_blocked", "value": totals["scrape_blocked"]},
        {"metric": "scrape_err", "value": totals["scrape_err"]},
        {"metric": "llm_err", "value": totals["llm_err"]},
        {"metric": "input_tokens", "value": totals["input_tokens"]},
        {"metric": "output_tokens", "value": totals["output_tokens"]},
        {"metric": "estimated_cost_usd", "value": f"${total_cost:.4f}"},
        {"metric": "elapsed_sec", "value": round(time.time() - t0, 1)},
    ])

    with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
        kept[main_cols].to_excel(xw, sheet_name="kept", index=False)
        dropped[main_cols].to_excel(xw, sheet_name="dropped", index=False)
        rdf[all_cols].to_excel(xw, sheet_name="all_with_verdict", index=False)
        cat_breakdown.to_excel(xw, sheet_name="by_category", index=False)
        ind_breakdown.to_excel(xw, sheet_name="by_industry", index=False)
        stock_breakdown.to_excel(xw, sheet_name="top_stocks", index=False)
        dir_breakdown.to_excel(xw, sheet_name="by_direction", index=False)
        scrape_breakdown.to_excel(xw, sheet_name="scrape_health", index=False)
        stats_df.to_excel(xw, sheet_name="run_stats", index=False)

    csv_path = out_path.with_suffix(".csv")
    kept[main_cols].to_csv(csv_path, index=False)

    elapsed = time.time() - t0

    print()
    print("=" * 60)
    print(f"Input rows (post category drop): {len(rdf)}")
    print(f"Kept (score>={args.min_score})              : {len(kept)}  "
          f"({len(kept) / max(len(rdf), 1) * 100:.1f}%)")
    print(f"Scrape: ok={totals['scrape_ok']}  short={totals['scrape_short']}  "
          f"blocked={totals['scrape_blocked']}  err={totals['scrape_err']}")
    print(f"Tokens in/out: {totals['input_tokens']:,} / {totals['output_tokens']:,}")
    print(f"Cost estimate: ${total_cost:.4f}")
    print(f"Elapsed: {elapsed:.1f}s")
    print()
    print("Category breakdown (kept):")
    for _, r in cat_breakdown.iterrows():
        print(f"  {r['category']:22s} {int(r['items']):>3}")
    print()
    print("Top 10 kept:")
    for _, r in kept.head(10).iterrows():
        stocks = (r.get("affected_stocks") or "")[:35]
        inds = (r.get("affected_industries") or "")[:25]
        dirn = str(r.get("direction") or "")[:4]
        print(f"  s={int(r['score']):>2} {dirn:4s} {str(r.get('category') or ''):18s} "
              f"stk={stocks:35s}  | {str(r.get('title') or '')[:65]}")
    print()
    print(f"Wrote:\n  {out_path}\n  {csv_path}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
