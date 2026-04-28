"""
add_stock_codes.py
==================

Enriches briefing `stock_news` and `all_items_by_industry` with BSE scrip
code, NSE symbol, ISIN, and market cap for every focal stock.

`build_final.py` calls `insert_exchange_columns_after_stocks()` automatically,
so new briefings already include these columns. This script remains useful to
re-enrich an older `.xlsx` or to refresh caps without re-running the LLM funnel.

Resolution chain (each step only runs if the previous one missed):

  1. company_master (via /api/stocks/search on Render) — tried first for speed
     and to avoid Indian API rate limits (429). No ISIN in this payload.
  2. Indian API  ( https://stock.indianapi.in/stock?name=<query> )
     Throttled (~0.35s between calls by default). Returns BSE, NSE, cap, ISIN.
  3. Yahoo Finance Search  -> canonical NSE symbol, then Indian API again.
  4. Yahoo-only row (symbol but no cap) if Indian API still misses.

  Validation: resolved companyName MUST share its FIRST significant token
  with the query, otherwise we discard the match. This kills false
  positives like "Air India" -> "Tenneco Clean Air India".

Lookups are cached per stock name within a single run.

Usage:
    .venv/bin/python add_stock_codes.py --input outputs/2026-04-25_briefing.xlsx
    .venv/bin/python add_stock_codes.py --all   # every *_briefing.xlsx in outputs/

A backup copy <name>.bak.xlsx is written before any in-place overwrite.
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import truststore  # noqa: F401  -- must be imported before requests for SSL
truststore.inject_into_ssl()

import pandas as pd
import requests

HERE = Path(__file__).resolve().parent
OUTPUTS_DIR = HERE / "outputs"

API_BASE = "https://wesbitebe.onrender.com"
SEARCH_URL = f"{API_BASE}/api/stocks/search"
YAHOO_SEARCH_URL = "https://query1.finance.yahoo.com/v1/finance/search"
INDIAN_API_URL = "https://stock.indianapi.in/stock"
INDIAN_API_KEY = "sk-live-3cCypamO0AsP0fDs1xUwGZCYLTzwnTD5Qhl9Ma2L"
TIMEOUT = 20

# Indian API rate-limits hard; spacing requests avoids 429 + multi-second backoffs.
_INDIAN_MIN_GAP_S = float(os.environ.get("INDIAN_API_MIN_GAP_S", "0.35"))
_indian_last_call_at: float = 0.0


def _throttle_indian() -> None:
    global _indian_last_call_at
    delta = time.monotonic() - _indian_last_call_at
    if delta < _INDIAN_MIN_GAP_S:
        time.sleep(_INDIAN_MIN_GAP_S - delta)
    _indian_last_call_at = time.monotonic()

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "rito-news-scratch/1.0"})

# Indian API session (Bearer-style x-api-key header).
INDIAN_SESSION = requests.Session()
INDIAN_SESSION.headers.update(
    {
        "x-api-key": INDIAN_API_KEY,
        "User-Agent": "rito-news-scratch/1.0",
        "Accept": "application/json",
    }
)

# Yahoo's API is friendlier with a real browser UA.
YAHOO_SESSION = requests.Session()
YAHOO_SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
    }
)

# In-process cache so repeated stock names don't re-hit any API.
_CACHE: Dict[str, Optional[dict]] = {}

# Things the user already told us aren't listed — short-circuit so we don't
# even hit Yahoo. (Yahoo correctly returns nothing for these too, so this is
# purely a perf optimisation.)
_KNOWN_UNLISTED = {
    "nse",
    "national stock exchange",
    "air india",
    "canara hsbc life insurance",
    "raheja developers",
    "aditya birla renewables",
    "haier electronics india",
    "axis finance",
    "renew power",  # NASDAQ-listed (RNW), not on NSE/BSE
}

# Hand-curated aliases: when the raw stock name doesn't resolve via Indian API
# under any obvious variant, map it directly to the NSE symbol so we can query
# the API by symbol (which is bulletproof). Keys must be _normalize()d strings.
_ALIASES: Dict[str, str] = {
    "adani energy solutions": "ADANIENSOL",
    "adani energy solutions limited": "ADANIENSOL",
    "adani energy solutions ltd": "ADANIENSOL",
    "one97 communications": "PAYTM",
    "one 97 communications": "PAYTM",
    "associated alcohols breweries": "ASALCBR",
    "associated alcohols & breweries": "ASALCBR",
    "associated alcohols & breweries limited": "ASALCBR",
}

# Generic words to drop when comparing names for validation, and when
# shrinking queries.
_NOISE_WORDS = {
    "ltd", "ltd.", "limited", "corp", "corp.", "corporation", "inc", "inc.",
    "company", "co", "co.", "the", "industries", "industry", "india", "indian",
    "and", "&", "of", "group", "holdings", "holding", "plc",
}


def _strip_suffix(name: str) -> str:
    n = (name or "").strip().rstrip(".").strip()
    parts = n.split()
    while parts and parts[-1].lower().rstrip(".") in _NOISE_WORDS:
        parts.pop()
    return " ".join(parts)


def _normalize(name: str) -> str:
    return _strip_suffix(name).lower().strip()


def _significant_tokens(name: str) -> List[str]:
    """Lowercased word LIST (preserves order) with noise/short tokens removed."""
    out: List[str] = []
    for raw in (name or "").lower().replace(".", " ").replace(",", " ").split():
        w = raw.strip("()[]'\"`")
        if len(w) >= 3 and w not in _NOISE_WORDS:
            out.append(w)
    return out


def _name_matches(query: str, candidate_name: str) -> bool:
    """
    True if `candidate_name` is plausibly the same entity as `query`.

    Rules (both must hold):
      (a) the FIRST significant token of the query must equal the first
          significant token of the candidate. This blocks
          "Air India" -> "Tenneco Clean Air India Ltd".
      (b) at least min(2, len(q)) significant query tokens must appear in
          the candidate. This blocks shrunk-query false positives like
          "Adani Energy" -> "Adani Green Energy" where only the first
          token matches.
    """
    q = _significant_tokens(query)
    c = _significant_tokens(candidate_name)
    if not q or not c:
        return False
    if q[0] != c[0]:
        return False
    qset = set(q)
    cset = set(c)
    needed = min(2, len(qset))
    return len(qset & cset) >= needed


# ── Step 1: Indian API  ─────────────────────────────────────────────────────
def _coerce_float(v) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def indian_api_lookup(query: str, retries: int = 2) -> Optional[dict]:
    """
    Hit https://stock.indianapi.in/stock?name=<query>.
    Returns {bse_scrip_code, nse_symbol, company_name, mkt_cap_cr,
    isin, industry} or None if the API returns {'error': ...} or
    if the validator rejects the match.
    """
    last_err: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            _throttle_indian()
            resp = INDIAN_SESSION.get(
                INDIAN_API_URL,
                params={"name": query},
                timeout=TIMEOUT,
            )
            if resp.status_code == 429:
                wait = 2 ** (attempt + 1)
                print(
                    f"  ! indian-api 429 for '{query}', backing off {wait}s",
                    file=sys.stderr,
                )
                time.sleep(wait)
                continue
            if resp.status_code != 200:
                last_err = RuntimeError(f"HTTP {resp.status_code}")
                if attempt < retries:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                return None
            data = resp.json() or {}
            if "error" in data:
                return None  # genuine "no such stock"
            cp = data.get("companyProfile") or {}
            cn = data.get("companyName") or cp.get("companyName") or ""
            bse = cp.get("exchangeCodeBse")
            nse = cp.get("exchangeCodeNse")
            if not (bse or nse):
                return None
            mc = (data.get("stockDetailsReusableData") or {}).get("marketCap")
            return {
                "bse_scrip_code": str(bse) if bse else None,
                "nse_symbol": (nse or "").upper() or None,
                "company_name": cn,
                "mkt_cap_cr": _coerce_float(mc),
                "isin": cp.get("isInId"),
                "industry": data.get("industry"),
                "source": "indian_api",
            }
        except Exception as exc:
            last_err = exc
            if attempt < retries:
                time.sleep(0.5 * (attempt + 1))
                continue
    if last_err:
        print(f"  ! indian_api lookup failed for '{query}': {last_err}", file=sys.stderr)
    return None


# ── Step 2: Yahoo Finance Search  ──────────────────────────────────────────

# Yahoo rate-limits aggressively (~10 req/min from a single IP). We throttle
# to stay under that even when stocks repeat across rows.
_YAHOO_MIN_GAP_S = 1.5
_yahoo_last_call_at: float = 0.0


def _throttle_yahoo() -> None:
    global _yahoo_last_call_at
    delta = time.monotonic() - _yahoo_last_call_at
    if delta < _YAHOO_MIN_GAP_S:
        time.sleep(_YAHOO_MIN_GAP_S - delta)
    _yahoo_last_call_at = time.monotonic()


def yahoo_search(query: str, retries: int = 2) -> Optional[dict]:
    """
    Search Yahoo Finance for an Indian-listed equity matching `query`.
    Returns {'name', 'nse_symbol', 'bse_symbol'} or None.
    """
    last_err: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            _throttle_yahoo()
            resp = YAHOO_SESSION.get(
                YAHOO_SEARCH_URL,
                params={"q": query, "quotesCount": 10, "newsCount": 0},
                timeout=TIMEOUT,
            )
            if resp.status_code == 429:
                # Yahoo rate-limit: exponential backoff (2s, 4s, 8s, 16s).
                wait = 2 ** (attempt + 1)
                print(
                    f"  ! yahoo 429 for '{query}', backing off {wait}s "
                    f"(attempt {attempt + 1}/{retries})",
                    file=sys.stderr,
                )
                time.sleep(wait)
                continue
            if resp.status_code != 200:
                last_err = RuntimeError(f"HTTP {resp.status_code}")
                if attempt < retries:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                return None
            quotes = (resp.json() or {}).get("quotes") or []
            # Filter to equities on Indian exchanges (BSE/NSI).
            in_quotes = [
                q for q in quotes
                if (q.get("exchange") in ("BSE", "NSI"))
                and (q.get("quoteType") in (None, "EQUITY"))
                and q.get("symbol")
            ]
            if not in_quotes:
                return None

            # Prefer NSE (.NS / exchange=NSI) since most news names track NSE.
            in_quotes.sort(key=lambda q: 0 if q.get("exchange") == "NSI" else 1)
            for cand in in_quotes:
                name = cand.get("longname") or cand.get("shortname") or ""
                if not _name_matches(query, name):
                    continue
                sym_raw = cand.get("symbol", "")
                # NSE symbol = symbol without ".NS"; BSE symbol = without ".BO"
                nse_sym = sym_raw[:-3] if sym_raw.endswith(".NS") else (
                    sym_raw[:-3] if (
                        cand.get("exchange") == "NSI" and sym_raw.endswith(".NS")
                    ) else None
                )
                # If we got a .BO, look in the rest for a paired .NS:
                if not nse_sym:
                    base = sym_raw.split(".")[0]
                    pair = next(
                        (q for q in in_quotes
                         if q.get("symbol") == f"{base}.NS"),
                        None,
                    )
                    if pair:
                        nse_sym = base
                bse_sym = None
                base = sym_raw.split(".")[0]
                pair_bo = next(
                    (q for q in in_quotes
                     if q.get("symbol") == f"{base}.BO"),
                    None,
                )
                if pair_bo or sym_raw.endswith(".BO"):
                    bse_sym = base
                return {
                    "name": name,
                    "nse_symbol": (nse_sym or "").upper() or None,
                    "bse_symbol": (bse_sym or "").upper() or None,
                }
            return None
        except Exception as exc:
            last_err = exc
            if attempt < retries:
                time.sleep(0.5 * (attempt + 1))
                continue
    if last_err:
        print(f"  ! yahoo lookup failed for '{query}': {last_err}", file=sys.stderr)
    return None


# ── Step 2/3: company_master via prod backend  ──────────────────────────────
def master_search(query: str, retries: int = 2) -> List[dict]:
    """Hit our backend's company_master search and return the raw list."""
    last_err: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            resp = SESSION.get(
                SEARCH_URL,
                params={"q": query, "limit": 10},
                timeout=TIMEOUT,
            )
            if resp.status_code != 200:
                last_err = RuntimeError(f"HTTP {resp.status_code}")
                if attempt < retries:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                return []
            return (resp.json() or {}).get("results") or []
        except Exception as exc:
            last_err = exc
            if attempt < retries:
                time.sleep(0.5 * (attempt + 1))
                continue
    if last_err:
        print(f"  ! master lookup failed for '{query}': {last_err}", file=sys.stderr)
    return []


def master_by_nse_symbol(nse_sym: str) -> Optional[dict]:
    """Lookup company_master by exact NSE symbol."""
    if not nse_sym:
        return None
    for row in master_search(nse_sym):
        if (row.get("nse_symbol") or "").upper() == nse_sym.upper():
            return row
    return None


def master_by_name(raw_name: str) -> Optional[dict]:
    """
    Fallback fuzzy lookup by company name. Tries the original name and then
    progressively shorter prefixes. Validates that the matched company name
    shares a token with the query.
    """
    stripped = _strip_suffix(raw_name)
    queries = [raw_name.strip(), stripped]
    words = stripped.split()
    if len(words) >= 3:
        queries.append(" ".join(words[:3]))
    if len(words) >= 2:
        queries.append(" ".join(words[:2]))

    seen_q: set[str] = set()
    for q in queries:
        if not q or q.lower() in seen_q:
            continue
        seen_q.add(q.lower())
        for row in master_search(q):
            if _name_matches(raw_name, row.get("company_name") or ""):
                return row
    return None


# ── Public API ──────────────────────────────────────────────────────────────
def lookup_stock(raw_name: str) -> Optional[dict]:
    """
    Resolve `raw_name` to {'bse_scrip_code', 'nse_symbol', 'company_name',
    'mkt_cap_cr', 'isin', 'industry', 'source'} or None. `source` indicates
    which step matched.

    Order of attempts:
      (1) company_master fuzzy name search (Render DB — fast, no Indian API
          quota; ISIN usually left blank)
      (2) Indian API by raw name
      (3) Indian API by suffix-stripped name (drops Ltd / Limited / Corp ...)
      (4) Yahoo Finance Search -> NSE symbol -> Indian API by NSE symbol
      (5) Yahoo Finance only (returns NSE symbol but no BSE code, used for
          stocks Indian API doesn't have under any name we know)
    """
    if not raw_name or not raw_name.strip():
        return None

    key = _normalize(raw_name)
    if key in _CACHE:
        return _CACHE[key]

    if key in _KNOWN_UNLISTED:
        _CACHE[key] = None
        return None

    # (0) Hand-curated alias -> NSE symbol -> Indian API by symbol.
    alias_sym = _ALIASES.get(key) or _ALIASES.get(_normalize(_strip_suffix(raw_name)))
    if alias_sym:
        hit = indian_api_lookup(alias_sym)
        if hit:
            _CACHE[key] = hit
            return hit

    # (1) company_master first — same resolver as the old tail fallback, but
    # here it avoids dozens of Indian API + Yahoo calls per briefing row.
    master_row = master_by_name(raw_name)
    if master_row:
        out = {
            "bse_scrip_code": master_row.get("bse_scrip_code"),
            "nse_symbol": master_row.get("nse_symbol"),
            "company_name": master_row.get("company_name"),
            "mkt_cap_cr": master_row.get("mkt_cap_cr"),
            "isin": None,
            "industry": None,
            "source": "master-fuzzy",
        }
        _CACHE[key] = out
        return out

    # (2) and (3): Indian API with raw name + suffix-stripped variant.
    candidate_queries: List[str] = []
    seen: set[str] = set()
    for q in (raw_name.strip(), _strip_suffix(raw_name)):
        if q and q.lower() not in seen:
            candidate_queries.append(q)
            seen.add(q.lower())

    raw_norm = re.sub(r"[^a-z0-9]+", "", raw_name.lower())
    for q in candidate_queries:
        hit = indian_api_lookup(q)
        if not hit:
            continue
        # Accept if name matches OR if the query string itself matches
        # the resolved NSE/BSE symbol (catches aliases: 'Paytm'->PAYTM,
        # 'Pine Labs'->PINELABS, 'PINELABS'->PINELABS).
        nse = (hit.get("nse_symbol") or "").lower()
        bse = (hit.get("bse_scrip_code") or "").lower()
        if (
            _name_matches(raw_name, hit.get("company_name", ""))
            or (nse and raw_norm == nse)
            or (bse and raw_norm == bse)
        ):
            _CACHE[key] = hit
            return hit

    # (4): Yahoo to discover the canonical NSE symbol, then use it as the
    # Indian-API query (which accepts NSE symbols directly).
    yahoo = yahoo_search(raw_name)
    if yahoo and yahoo.get("nse_symbol"):
        hit = indian_api_lookup(yahoo["nse_symbol"])
        if hit:
            # Validate against the Yahoo-canonical name rather than the raw
            # query, since this branch handles renames where raw_name and
            # the master/API name diverge intentionally.
            if _name_matches(yahoo["name"], hit.get("company_name", "")):
                _CACHE[key] = hit
                return hit
        # (5): Indian API didn't have it under NSE either (rare).
        out = {
            "bse_scrip_code": None,
            "nse_symbol": yahoo["nse_symbol"],
            "company_name": yahoo["name"],
            "mkt_cap_cr": None,
            "isin": None,
            "industry": None,
            "source": "yahoo-only",
        }
        _CACHE[key] = out
        return out

    _CACHE[key] = None
    return None


def split_stocks(cell: object) -> List[str]:
    """Split a `affected_stocks` cell on '|' or ',' and clean up."""
    if cell is None or (isinstance(cell, float) and pd.isna(cell)):
        return []
    s = str(cell).strip()
    if not s:
        return []
    # We standardised on ' | ' as separator; comma may sneak in via legacy data.
    parts = [p.strip() for p in s.replace(",", "|").split("|")]
    return [p for p in parts if p]


def enrich_stock_row(stocks_cell: object) -> Tuple[str, str, str, str]:
    """Return (bse_codes, nse_symbols, market_caps_cr, isins) for a row, '|'-separated."""
    names = split_stocks(stocks_cell)
    if not names:
        return ("", "", "", "")

    bse_codes: List[str] = []
    nse_syms: List[str] = []
    mcaps: List[str] = []
    isins: List[str] = []
    for n in names:
        hit = lookup_stock(n)
        if hit:
            bse_codes.append(str(hit.get("bse_scrip_code") or ""))
            nse_syms.append(str(hit.get("nse_symbol") or ""))
            mc = hit.get("mkt_cap_cr")
            mcaps.append(f"{mc:,.0f}" if isinstance(mc, (int, float)) and mc else "")
            isins.append(str(hit.get("isin") or ""))
        else:
            bse_codes.append("")
            nse_syms.append("")
            mcaps.append("")
            isins.append("")
    return (
        " | ".join(bse_codes).strip(" |"),
        " | ".join(nse_syms).strip(" |"),
        " | ".join(mcaps).strip(" |"),
        " | ".join(isins).strip(" |"),
    )


def insert_exchange_columns_after_stocks(
    stock_df: pd.DataFrame,
    *,
    progress_label: str = "rows",
    progress_every: int = 5,
) -> pd.DataFrame:
    """
    Insert bse_scrip_code, nse_symbol, isin, market_cap_cr immediately after
    `affected_stocks` (same layout as the standalone post-process step).

    Rows with empty `affected_stocks` get blank exchange fields. Used by
    `build_final.py` so every briefing is investor-ready without a second script.
    """
    if "affected_stocks" not in stock_df.columns:
        return stock_df

    out = stock_df.copy()
    n = len(out)
    bse_list: List[str] = []
    nse_list: List[str] = []
    mc_list: List[str] = []
    isin_list: List[str] = []
    for i, val in enumerate(out["affected_stocks"].tolist(), 1):
        bse, nse, mc, isin = enrich_stock_row(val)
        bse_list.append(bse)
        nse_list.append(nse)
        mc_list.append(mc)
        isin_list.append(isin)
        if progress_every and (i % progress_every == 0 or i == n):
            print(f"    {progress_label}: {i}/{n} (cache {len(_CACHE)})")

    insert_at = list(out.columns).index("affected_stocks") + 1
    for col_name, col_vals in [
        ("market_cap_cr", mc_list),
        ("isin", isin_list),
        ("nse_symbol", nse_list),
        ("bse_scrip_code", bse_list),
    ]:
        if col_name in out.columns:
            out.drop(columns=[col_name], inplace=True)
        out.insert(insert_at, col_name, col_vals)

    return out


def process_file(in_path: Path, out_path: Path) -> dict:
    """Read briefing, enrich stock_news, write new briefing keeping all sheets."""
    if not in_path.exists():
        raise FileNotFoundError(in_path)

    print(f"Reading: {in_path}")
    sheets: Dict[str, pd.DataFrame] = pd.read_excel(in_path, sheet_name=None)
    if "stock_news" not in sheets:
        raise RuntimeError(f"{in_path.name} has no 'stock_news' sheet")

    stock_df = sheets["stock_news"].copy()
    print(f"  stock_news rows: {len(stock_df)}")
    sheets["stock_news"] = insert_exchange_columns_after_stocks(
        stock_df, progress_label="stock_news", progress_every=5
    )

    if "all_items_by_industry" in sheets:
        all_df = sheets["all_items_by_industry"].copy()
        if "affected_stocks" in all_df.columns:
            print("  enriching all_items_by_industry too...")
            sheets["all_items_by_industry"] = insert_exchange_columns_after_stocks(
                all_df, progress_label="all_items", progress_every=25
            )

    # Backup original if we're overwriting in place.
    if in_path.resolve() == out_path.resolve():
        backup = in_path.with_suffix(".bak.xlsx")
        shutil.copy2(in_path, backup)
        print(f"  backup written: {backup.name}")

    # Preserve sheet order from the original file.
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    sn = sheets["stock_news"]
    n_filled = (
        int(sn["bse_scrip_code"].astype(str).str.strip().ne("").sum())
        if "bse_scrip_code" in sn.columns
        else 0
    )
    print(f"Wrote: {out_path}")
    print(f"  filled BSE codes: {n_filled}/{len(sn)} stock_news rows")
    return {
        "rows": len(sn),
        "filled": n_filled,
        "cache": len(_CACHE),
    }


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", help="Path to one briefing xlsx")
    ap.add_argument("--output", default=None, help="Output xlsx (default: overwrite input)")
    ap.add_argument("--all", action="store_true", help="Process every *.xlsx in outputs/")
    return ap.parse_args()


def main() -> int:
    args = parse_args()

    if args.all:
        files = sorted(
            p for p in OUTPUTS_DIR.glob("*_briefing.xlsx")
            if not p.name.startswith("~$") and not p.name.endswith(".bak.xlsx")
        )
        if not files:
            print(f"No *_briefing.xlsx files in {OUTPUTS_DIR}")
            return 1
        for f in files:
            print()
            process_file(f, f)
        return 0

    if not args.input:
        print("Provide --input <file> or --all", file=sys.stderr)
        return 2

    in_path = Path(args.input).resolve()
    out_path = Path(args.output).resolve() if args.output else in_path
    process_file(in_path, out_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
