"""
Remap free-form `affected_industries` to a FIXED 24-industry taxonomy.

Input : news_22apr_v1.xlsx  (kept sheet)
Output: news_22apr_v2.xlsx  (same structure, affected_industries rewritten
                             to use canonical labels; prior values saved
                             in `industries_prior`)

Uses gpt-4.1-nano with batched JSON mode. Cost << $0.02.

Run:
  cd news_scratch
  .venv/bin/python remap_industries.py
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
from rapidfuzz import fuzz, process

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

# ---------------------------------------------------------------------------
# Fixed taxonomy (exact strings — LLM must return these verbatim)
# ---------------------------------------------------------------------------
TAXONOMY: List[Dict[str, str]] = [
    ("Information Technology & Software",
     "software, IT services, cloud, SaaS, enterprise tech, digital platforms, AI, cybersecurity, semiconductors where tech-led"),
    ("Telecommunications & Connectivity",
     "telecom operators, broadband, wireless, towers, network infrastructure, satellite communications"),
    ("Media, Entertainment & Gaming",
     "TV, streaming, content, music, publishing, esports, online gaming, digital entertainment"),
    ("Financial Services",
     "banks, NBFCs, lending, payments, fintech, investment firms, exchanges, brokers, wealth management, asset management"),
    ("Insurance",
     "life insurance, general insurance, health insurance, reinsurance"),
    ("Real Estate & Property Services",
     "developers, commercial and residential real estate, RE services, property management"),
    ("Infrastructure & Construction",
     "EPC, civil construction, roads, rail infra, ports infra, urban infra, construction services"),
    ("Industrial Manufacturing & Engineering",
     "capital goods, machinery, industrial equipment, engineering services, automation, electrical equipment, factory systems"),
    ("Automobiles & Mobility",
     "passenger vehicles, commercial vehicles, two-wheelers, auto components, EVs, mobility platforms, transport equipment"),
    ("Aerospace & Defence",
     "defence manufacturing, weapons systems, aerospace, aviation systems, military electronics, strategic technology"),
    ("Transportation & Logistics",
     "shipping, ports, freight, logistics, warehousing, rail transport, cargo handling, supply-chain services"),
    ("Aviation & Travel",
     "airlines, airports, aviation services, travel services, tourism, hospitality-linked travel businesses"),
    ("Energy",
     "oil & gas, refining, fuel retail, coal, power generation, utilities, renewable energy, biofuels, energy storage, hydrogen"),
    ("Chemicals & Materials",
     "specialty chemicals, industrial chemicals, petrochemicals, industrial gases, fertilizers, paints, coatings, advanced materials"),
    ("Metals & Mining",
     "steel, aluminium, copper, ferrous/non-ferrous metals, mining, mineral extraction, metal processing"),
    ("Building Materials",
     "cement, ceramics, glass, cables where construction-led, pipes, insulation, tiles, other construction materials"),
    ("Agriculture & Agribusiness",
     "farming, seeds, crop inputs, agri services, plantations, sugar, ethanol, seafood, marine products, agri exports"),
    ("Food, Beverage & Consumer Staples",
     "dairy, packaged food, beverages, alcohol, household staples, food processing, daily-use products"),
    ("Consumer Durables & Discretionary",
     "electronics, white goods, appliances, batteries for consumer use, home products, long-life discretionary goods"),
    ("Retail & E-Commerce",
     "consumer retail, online commerce, marketplaces, omnichannel commerce, specialty retail"),
    ("Healthcare & Life Sciences",
     "pharmaceuticals, biotech, hospitals, diagnostics, medical devices, healthcare services"),
    ("Textiles, Apparel & Luxury Goods",
     "textiles, garments, apparel, footwear, leather, fashion manufacturing, related exports"),
    ("Environmental & Waste Services",
     "recycling, water treatment, waste management, pollution control, environmental solutions"),
    ("Diversified Conglomerates / Holding Companies",
     "companies spanning multiple unrelated sectors where no single industry is dominant"),
]
CANON_NAMES = [t[0] for t in TAXONOMY]
CANON_SET = set(CANON_NAMES)

TAXONOMY_TEXT = "\n".join(f"  - {name} — {desc}" for name, desc in TAXONOMY)

# Keyword-to-canonical map (covers the most common free-form variants
# the LLM tends to return). The key is lower-cased and searched as a
# substring in the incoming string; longer keys win.
_KEYWORDS: List[tuple[str, str]] = [
    # IT & Software
    ("information technology", "Information Technology & Software"),
    ("software", "Information Technology & Software"),
    ("it services", "Information Technology & Software"),
    ("cloud", "Information Technology & Software"),
    ("saas", "Information Technology & Software"),
    ("semiconductor", "Information Technology & Software"),
    ("cybersecurity", "Information Technology & Software"),
    ("artificial intelligence", "Information Technology & Software"),
    (" ai ", "Information Technology & Software"),
    # Telecom
    ("telecom", "Telecommunications & Connectivity"),
    ("broadband", "Telecommunications & Connectivity"),
    ("wireless", "Telecommunications & Connectivity"),
    ("5g", "Telecommunications & Connectivity"),
    ("tower", "Telecommunications & Connectivity"),
    # Media / entertainment / gaming
    ("media", "Media, Entertainment & Gaming"),
    ("entertainment", "Media, Entertainment & Gaming"),
    ("gaming", "Media, Entertainment & Gaming"),
    ("esports", "Media, Entertainment & Gaming"),
    ("e-sports", "Media, Entertainment & Gaming"),
    ("streaming", "Media, Entertainment & Gaming"),
    ("publishing", "Media, Entertainment & Gaming"),
    # Financial Services
    ("financial services", "Financial Services"),
    ("banking", "Financial Services"),
    ("bank ", "Financial Services"),
    ("nbfc", "Financial Services"),
    ("fintech", "Financial Services"),
    ("payments", "Financial Services"),
    ("asset management", "Financial Services"),
    ("wealth management", "Financial Services"),
    ("broker", "Financial Services"),
    ("exchange", "Financial Services"),
    # Insurance
    ("insurance", "Insurance"),
    ("reinsurance", "Insurance"),
    # Real Estate
    ("real estate", "Real Estate & Property Services"),
    ("property", "Real Estate & Property Services"),
    ("developer", "Real Estate & Property Services"),
    # Infrastructure
    ("infrastructure", "Infrastructure & Construction"),
    ("construction", "Infrastructure & Construction"),
    ("epc", "Infrastructure & Construction"),
    ("roads", "Infrastructure & Construction"),
    # Industrial
    ("industrial", "Industrial Manufacturing & Engineering"),
    ("machinery", "Industrial Manufacturing & Engineering"),
    ("capital goods", "Industrial Manufacturing & Engineering"),
    ("engineering", "Industrial Manufacturing & Engineering"),
    ("automation", "Industrial Manufacturing & Engineering"),
    ("electrical equipment", "Industrial Manufacturing & Engineering"),
    # Auto
    ("automobile", "Automobiles & Mobility"),
    ("auto component", "Automobiles & Mobility"),
    ("two-wheeler", "Automobiles & Mobility"),
    ("two wheeler", "Automobiles & Mobility"),
    ("passenger vehicle", "Automobiles & Mobility"),
    ("commercial vehicle", "Automobiles & Mobility"),
    ("electric vehicle", "Automobiles & Mobility"),
    ("ev ", "Automobiles & Mobility"),
    # Defence
    ("defence", "Aerospace & Defence"),
    ("defense", "Aerospace & Defence"),
    ("aerospace", "Aerospace & Defence"),
    ("weapons", "Aerospace & Defence"),
    ("military", "Aerospace & Defence"),
    # Transport / Logistics
    ("logistics", "Transportation & Logistics"),
    ("shipping", "Transportation & Logistics"),
    ("port ", "Transportation & Logistics"),
    ("ports", "Transportation & Logistics"),
    ("freight", "Transportation & Logistics"),
    ("warehous", "Transportation & Logistics"),
    ("supply chain", "Transportation & Logistics"),
    # Aviation / Travel
    ("airline", "Aviation & Travel"),
    ("airport", "Aviation & Travel"),
    ("aviation", "Aviation & Travel"),
    ("travel", "Aviation & Travel"),
    ("tourism", "Aviation & Travel"),
    ("hospitality", "Aviation & Travel"),
    # Energy
    ("oil and gas", "Energy"),
    ("oil & gas", "Energy"),
    ("refining", "Energy"),
    ("fuel retail", "Energy"),
    ("petroleum", "Energy"),
    ("coal", "Energy"),
    ("power generation", "Energy"),
    ("utilit", "Energy"),
    ("renewable", "Energy"),
    ("biofuel", "Energy"),
    ("ethanol blending", "Energy"),
    ("hydrogen", "Energy"),
    ("energy storage", "Energy"),
    ("solar", "Energy"),
    ("wind power", "Energy"),
    ("energy", "Energy"),
    # Chemicals & Materials
    ("chemicals", "Chemicals & Materials"),
    ("petrochemical", "Chemicals & Materials"),
    ("fertilizer", "Chemicals & Materials"),
    ("fertiliser", "Chemicals & Materials"),
    ("paint", "Chemicals & Materials"),
    ("coating", "Chemicals & Materials"),
    ("industrial gas", "Chemicals & Materials"),
    ("advanced materials", "Chemicals & Materials"),
    # Metals & Mining
    ("steel", "Metals & Mining"),
    ("aluminium", "Metals & Mining"),
    ("aluminum", "Metals & Mining"),
    ("copper", "Metals & Mining"),
    ("mining", "Metals & Mining"),
    ("metal", "Metals & Mining"),
    ("ferrous", "Metals & Mining"),
    # Building Materials
    ("cement", "Building Materials"),
    ("ceramic", "Building Materials"),
    ("glass", "Building Materials"),
    ("tiles", "Building Materials"),
    ("pipes", "Building Materials"),
    ("cables", "Building Materials"),
    ("insulation", "Building Materials"),
    ("building material", "Building Materials"),
    # Agriculture / Agribusiness
    ("agriculture", "Agriculture & Agribusiness"),
    ("agri ", "Agriculture & Agribusiness"),
    ("farming", "Agriculture & Agribusiness"),
    ("seeds", "Agriculture & Agribusiness"),
    ("sugar", "Agriculture & Agribusiness"),
    ("ethanol", "Agriculture & Agribusiness"),
    ("seafood", "Agriculture & Agribusiness"),
    ("marine product", "Agriculture & Agribusiness"),
    ("plantation", "Agriculture & Agribusiness"),
    # Food / Beverage / Consumer Staples
    ("dairy", "Food, Beverage & Consumer Staples"),
    ("packaged food", "Food, Beverage & Consumer Staples"),
    ("beverage", "Food, Beverage & Consumer Staples"),
    ("alcohol", "Food, Beverage & Consumer Staples"),
    ("fmcg", "Food, Beverage & Consumer Staples"),
    ("consumer staples", "Food, Beverage & Consumer Staples"),
    ("food processing", "Food, Beverage & Consumer Staples"),
    ("food ", "Food, Beverage & Consumer Staples"),
    # Consumer Durables & Discretionary
    ("consumer durable", "Consumer Durables & Discretionary"),
    ("white goods", "Consumer Durables & Discretionary"),
    ("appliance", "Consumer Durables & Discretionary"),
    ("consumer electronic", "Consumer Durables & Discretionary"),
    ("refurbished electronic", "Consumer Durables & Discretionary"),
    ("batteries", "Consumer Durables & Discretionary"),
    ("discretionary", "Consumer Durables & Discretionary"),
    # Retail / E-Commerce
    ("e-commerce", "Retail & E-Commerce"),
    ("ecommerce", "Retail & E-Commerce"),
    ("marketplace", "Retail & E-Commerce"),
    ("retail", "Retail & E-Commerce"),
    # Healthcare
    ("pharma", "Healthcare & Life Sciences"),
    ("biotech", "Healthcare & Life Sciences"),
    ("hospital", "Healthcare & Life Sciences"),
    ("diagnostic", "Healthcare & Life Sciences"),
    ("medical device", "Healthcare & Life Sciences"),
    ("healthcare", "Healthcare & Life Sciences"),
    ("life science", "Healthcare & Life Sciences"),
    # Textiles
    ("textile", "Textiles, Apparel & Luxury Goods"),
    ("apparel", "Textiles, Apparel & Luxury Goods"),
    ("garment", "Textiles, Apparel & Luxury Goods"),
    ("footwear", "Textiles, Apparel & Luxury Goods"),
    ("leather", "Textiles, Apparel & Luxury Goods"),
    ("luxury", "Textiles, Apparel & Luxury Goods"),
    # Environmental
    ("recycling", "Environmental & Waste Services"),
    ("water treatment", "Environmental & Waste Services"),
    ("waste management", "Environmental & Waste Services"),
    ("pollution", "Environmental & Waste Services"),
    ("environmental", "Environmental & Waste Services"),
    # Conglomerates
    ("conglomerate", "Diversified Conglomerates / Holding Companies"),
    ("holding company", "Diversified Conglomerates / Holding Companies"),
    ("holdings", "Diversified Conglomerates / Holding Companies"),
]
# Sort so longer keywords (more specific) are checked first.
_KEYWORDS.sort(key=lambda kv: len(kv[0]), reverse=True)

_CANON_BLOB = {
    name: (name + " " + desc).lower()
    for name, desc in TAXONOMY
}


def _keyword_match(raw: str) -> Optional[str]:
    s = f" {raw.lower()} "
    for kw, canon in _KEYWORDS:
        if kw in s:
            return canon
    return None


def _fuzzy_match(raw: str, min_score: int = 72) -> Optional[str]:
    s = raw.lower().strip()
    if not s:
        return None
    # Try match against canonical name first
    best = process.extractOne(
        s, CANON_NAMES, scorer=fuzz.token_set_ratio
    )
    if best and best[1] >= 85:
        return best[0]
    # Fall back to matching against (name + description) blob; return name
    name_blob = list(_CANON_BLOB.items())
    scored = [
        (name, fuzz.token_set_ratio(s, blob))
        for name, blob in name_blob
    ]
    scored.sort(key=lambda x: x[1], reverse=True)
    if scored and scored[0][1] >= min_score:
        return scored[0][0]
    return None


def resolve_label(raw: str) -> Optional[str]:
    """Resolve a single free-form label to a canonical one (or None)."""
    if not isinstance(raw, str):
        return None
    s = raw.strip()
    if not s:
        return None
    if s in CANON_SET:
        return s
    low = s.lower()
    for n in CANON_NAMES:
        if n.lower() == low:
            return n
    kw = _keyword_match(s)
    if kw:
        return kw
    return _fuzzy_match(s)

SYSTEM_PROMPT = f"""You classify Indian financial news items into a FIXED industry taxonomy.

TAXONOMY (use the EXACT label strings; any other string is invalid):
{TAXONOMY_TEXT}

Rules:
  - For each item, pick 1 to 3 industries, ORDERED by relevance (most relevant first).
  - Prefer the most specific industry over general ones.
  - Use "Diversified Conglomerates / Holding Companies" ONLY for holding companies / multi-sector plays where no single industry dominates.
  - For pure macro items with no clear sector read-through, still pick the closest 1-2 labels; do NOT leave empty unless truly impossible.
  - NEVER invent new labels. NEVER return a label that is not in the list above.
  - Return ONLY the canonical strings verbatim.

Output: a JSON object with key "classifications" whose value is an ARRAY. \
Each element must be an object with:
  {{"id": <int>, "industries": ["<canonical label>", ...]}}

Return one entry per input item, preserving ids. No extra keys, no prose.
"""


def _format_batch(batch: List[Dict[str, Any]]) -> str:
    lines = ["ITEMS:"]
    for it in batch:
        lines.append(
            f"- id: {it['id']}\n"
            f"  title: {it['title']}\n"
            f"  summary: {it['summary']}\n"
            f"  prior_industries: {it['prior_industries']}\n"
            f"  stocks: {it['stocks']}"
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
                max_tokens=1200,
            )
            content = resp.choices[0].message.content or "{}"
            data = json.loads(content)
            verdicts = data.get("classifications") or data.get("items") or []
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


def _sanitize_labels(labels: List[str]) -> List[str]:
    """Resolve each label to a canonical one; dedupe, preserve order, cap at 3."""
    clean: List[str] = []
    for lab in labels:
        canon = resolve_label(lab) if isinstance(lab, str) else None
        if canon:
            clean.append(canon)
    seen = set()
    out = []
    for c in clean:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out[:3]


def _fallback_from_prior(prior: str, title: str, category: str) -> List[str]:
    """When the LLM returns nothing usable, resolve from the prior free-form
    industries + title + category."""
    picks: List[str] = []
    if prior:
        # prior was comma-separated free-form
        for tok in prior.split(","):
            canon = resolve_label(tok)
            if canon and canon not in picks:
                picks.append(canon)
                if len(picks) == 3:
                    return picks
    if not picks and title:
        canon = _keyword_match(title) or _fuzzy_match(title, min_score=60)
        if canon:
            picks.append(canon)
    # Category-level hints for pure-macro items
    CAT_HINTS = {
        "macro_to_sector": "",          # too generic on its own
        "govt_spending": "Infrastructure & Construction",
        "regulation_sector": "",
    }
    if not picks:
        hint = CAT_HINTS.get(str(category or "").lower(), "")
        if hint:
            picks.append(hint)
    return picks


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default=str(HERE / "news_22apr_v1.xlsx"))
    ap.add_argument("--sheet", default="kept")
    ap.add_argument("--output", default=str(HERE / "news_22apr_v2.xlsx"))
    ap.add_argument("--model", default="gpt-4.1-nano")
    ap.add_argument("--batch-size", type=int, default=15)
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
            "title": str(r.get("title") or "")[:200],
            "summary": str(r.get("ai_summary") or "")[:600],
            "prior_industries": str(r.get("affected_industries") or ""),
            "stocks": str(r.get("affected_stocks") or "")[:200],
        })

    batches: List[List[Dict[str, Any]]] = [
        items[i:i + args.batch_size]
        for i in range(0, len(items), args.batch_size)
    ]
    print(f"Prepared {len(batches)} batches of up to {args.batch_size}, "
          f"running {args.workers} in parallel with {args.model}")

    verdicts_by_id: Dict[int, List[str]] = {}
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
                    vid = v.get("id")
                    try:
                        vid = int(vid)
                    except (TypeError, ValueError):
                        continue
                    labels = v.get("industries") or v.get("labels") or []
                    verdicts_by_id[vid] = _sanitize_labels(list(labels))
            except Exception as e:
                print(f"  batch {futs[fut]} failed: {e}")
            if done % 2 == 0 or done == len(batches):
                print(f"  {done}/{len(batches)}  tok in/out={tot_in}/{tot_out}  "
                      f"elapsed={time.time() - t0:.1f}s")

    # Apply back to dataframe
    df["industries_prior"] = df["affected_industries"]

    def _resolve_row(row: pd.Series) -> str:
        rid = int(row["_id"])
        labs = verdicts_by_id.get(rid, [])
        if not labs:
            labs = _fallback_from_prior(
                str(row.get("industries_prior") or ""),
                str(row.get("title") or ""),
                str(row.get("category") or ""),
            )
        return " | ".join(labs)

    df["affected_industries"] = df.apply(_resolve_row, axis=1)
    df["industry_primary"] = df["affected_industries"].apply(
        lambda s: s.split(" | ")[0].strip() if isinstance(s, str) and s else ""
    )

    missing = df[df["affected_industries"] == ""]
    if len(missing):
        print(f"  Note: {len(missing)} rows still have no industry after LLM + fallback.")

    # Breakdowns using canonical taxonomy (split on " | ", safe for labels with commas)
    ind_break = (
        df.assign(_s=df["affected_industries"].str.split(" | ", regex=False))
          .explode("_s")
          .assign(_s=lambda d: d["_s"].str.strip())
          .query("_s != ''")
          .groupby("_s").size().reset_index(name="items")
          .sort_values("items", ascending=False)
          .rename(columns={"_s": "industry"})
    )
    primary_break = (
        df.groupby("industry_primary").size().reset_index(name="items")
          .sort_values("items", ascending=False)
          .rename(columns={"industry_primary": "primary_industry"})
    )

    main_cols = [
        "score", "direction", "category",
        "affected_stocks", "industry_primary", "affected_industries",
        "ai_summary", "implication", "key_numbers",
        "source", "published_at_ist", "title", "link",
        "scrape_status", "scraped_chars", "reason",
        "industries_prior",
    ]
    main_cols = [c for c in main_cols if c in df.columns]

    inp_cost = tot_in / 1_000_000 * PRICE_IN.get(args.model, 0.0)
    out_cost = tot_out / 1_000_000 * PRICE_OUT.get(args.model, 0.0)
    stats = pd.DataFrame([
        {"metric": "model", "value": args.model},
        {"metric": "rows", "value": int(len(df))},
        {"metric": "input_tokens", "value": tot_in},
        {"metric": "output_tokens", "value": tot_out},
        {"metric": "estimated_cost_usd", "value": f"${inp_cost + out_cost:.4f}"},
        {"metric": "elapsed_sec", "value": round(time.time() - t0, 1)},
        {"metric": "rows_with_no_industry", "value": int(len(missing))},
    ])
    taxonomy_df = pd.DataFrame(
        [{"industry": n, "description": d} for n, d in TAXONOMY]
    )

    with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
        df[main_cols].sort_values(
            ["score", "published_at_ist"], ascending=[False, False]
        ).to_excel(xw, sheet_name="kept", index=False)
        primary_break.to_excel(xw, sheet_name="by_primary_industry", index=False)
        ind_break.to_excel(xw, sheet_name="by_industry_all", index=False)
        taxonomy_df.to_excel(xw, sheet_name="taxonomy", index=False)
        stats.to_excel(xw, sheet_name="run_stats", index=False)

    csv_path = out_path.with_suffix(".csv")
    df[main_cols].to_csv(csv_path, index=False)

    print()
    print("=" * 60)
    print(f"Rows classified: {len(df) - len(missing)}/{len(df)}")
    print(f"Tokens in/out: {tot_in:,} / {tot_out:,}   cost ${inp_cost + out_cost:.4f}")
    print()
    print("Primary-industry breakdown:")
    for _, r in primary_break.iterrows():
        label = r["primary_industry"] or "(none)"
        print(f"  {label:46s} {int(r['items']):>3}")
    print()
    print(f"Wrote:\n  {out_path}\n  {csv_path}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
