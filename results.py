# results.py – PDF analysis & ranking pipeline
import os
import re
import warnings
import io
import pandas as pd
import openai
import pdfplumber
from httpx import ReadTimeout, ConnectError
from datetime import datetime
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

warnings.filterwarnings("ignore", message=r"Cannot set gray non-stroke color")

load_dotenv()

API_KEY = os.environ.get("OPENAI_API_KEY")
if not API_KEY:
    raise ValueError("OPENAI_API_KEY not found. Please set it in your .env file.")

MODEL_SCREEN = "gpt-4.1-mini"
BASE_URL = "https://www.bseindia.com/xml-data/corpfiling/AttachLive/"

client = openai.OpenAI(api_key=API_KEY)

# ---------------------------------------------------------------------------
# Full screening prompt (from notebook – handles edge cases properly)
# ---------------------------------------------------------------------------
PROMPT_SCREEN = (
    "You are assessing the incremental price impact of this specific PDF filing.\n"
    "Return ONE tab-separated line only:\n"
    "Company<TAB>Impact tag<TAB>≤30-word summary<TAB>Price-move range<TAB>≤20-word rationale\n"
    "Impact tag = STRONGLY POSITIVE / POSITIVE / NEUTRAL / NEGATIVE / STRONGLY NEGATIVE / "
    "BEAT / MATCHED / MISSED; use N/A if immaterial.\n"
    "\n"
    "Treat the filing as immaterial and set Impact tag=N/A, Price-move range=0–1% if:\n"
    "- It is mainly a transcript/notes/proceedings/recording link of an event (results call, analyst meet,\n"
    "  conference, AGM) that already happened, OR\n"
    "- It mostly repeats earlier company announcements without meaningful new numbers/guidance/contracts/\n"
    "  regulatory actions, OR\n"
    "- It is a newspaper clipping/media article just reporting results/events already public.\n"
    "\n"
    "For financial results, compare both YoY and QoQ for key metrics (revenue, EBITDA, PAT, EPS):\n"
    "- Only give clearly positive tags (POSITIVE/STRONGLY POSITIVE/BEAT) and a large price-move range when\n"
    "  MOST key metrics improve on BOTH YoY AND QoQ.\n"
    "- Give clearly negative tags (NEGATIVE/STRONGLY NEGATIVE/MISSED) when MOST key metrics are weak on\n"
    "  BOTH YoY AND QoQ.\n"
    "- If YoY and QoQ are mixed (some better, some worse), choose a balanced tag (often NEUTRAL or modest\n"
    "  POSITIVE/NEGATIVE) and keep the price-move range moderate.\n"
    "- If you cannot reliably infer BOTH YoY and QoQ performance from the PDF, do NOT give a strong\n"
    "  positive/negative tag or large move; keep impact low or N/A.\n"
    "\n"
    "Only use a non-trivial price-move range when this PDF itself introduces materially new information\n"
    "versus what the market already knows. If unsure, treat impact as immaterial.\n"
    "Output exactly one TSV line and nothing else."
)

# ---------------------------------------------------------------------------
# Category classification
# ---------------------------------------------------------------------------
CATEGORY_LABELS = [
    "Financial Results/Announcement",
    "Leadership Changes",
    "Raising Money & Changing Shares",
    "Dividends/Shareholder Rewards",
    "Mergers, Acquisitions & Partnerships",
    "New Orders & Business Wins",
    "Operations Expansions",
    "Legal, Compliance & Credit",
    "General Investor Info & Clarifications",
]

PROMPT_CATEGORY = (
    "Read the content and return ONLY one of the following exact labels:\n"
    + "\n".join(CATEGORY_LABELS)
    + "\nReturn just the label, no other text."
)

KEYWORDS = {
    "Financial Results/Announcement": [
        "quarter", "q1", "q2", "q3", "q4", "annual", "results", "revenue",
        "profit", "ebitda", "earnings", "auditor", "conference call", "call transcript"
    ],
    "Leadership Changes": [
        "ceo", "cfo", "cto", "coo", "chairman", "director", "resignation",
        "appoint", "appointment", "board"
    ],
    "Raising Money & Changing Shares": [
        "qip", "preferential", "rights issue", "fpo", "ipo", "equity shares",
        "warrants", "esop", "allotment", "bonus issue of warrants", "private placement"
    ],
    "Dividends/Shareholder Rewards": [
        "dividend", "buyback", "bonus share", "split", "stock split",
        "record date", "ex-dividend"
    ],
    "Mergers, Acquisitions & Partnerships": [
        "merger", "amalgamation", "scheme of arrangement", "acquisition",
        "stake buy", "takeover", "mou", "strategic partnership", "joint venture",
        "tie-up", "collaboration"
    ],
    "New Orders & Business Wins": [
        "order", "purchase order", "loi", "letter of intent", "loa",
        "contract", "work order", "award"
    ],
    "Operations Expansions": [
        "capacity", "capex", "greenfield", "brownfield", "plant", "factory",
        "commissioning", "commercial production", "expansion", "facility",
        "new unit", "trial run", "scale-up"
    ],
    "Legal, Compliance & Credit": [
        "litigation", "writ", "suit", "arbitration", "legal", "penalty",
        "show cause", "rbi", "sebi", "nclt", "tribunal", "insolvency",
        "pledge", "release of pledge", "credit rating", "crisil",
        "care ratings", "icra", "downgrade", "upgrade", "lender", "default"
    ],
    "General Investor Info & Clarifications": [
        "clarification", "media", "rumour", "press release", "investor",
        "analyst meet", "intimation", "update", "change in registered office",
        "change in name", "intimation of", "general information"
    ],
}


def _guess_category(text: str) -> str:
    t = text.lower()
    priority = [
        "Financial Results/Announcement",
        "New Orders & Business Wins",
        "Mergers, Acquisitions & Partnerships",
        "Dividends/Shareholder Rewards",
        "Raising Money & Changing Shares",
        "Leadership Changes",
        "Operations Expansions",
        "Legal, Compliance & Credit",
        "General Investor Info & Clarifications",
    ]
    for cat in priority:
        if any(k in t for k in KEYWORDS.get(cat, [])):
            return cat
    return "General Investor Info & Clarifications"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_split_line = lambda l: l.split("\t") if l.count("\t") == 4 else re.split(r"\s*\|\s*", l)

_price_mid = lambda s: [float(x) for x in re.findall(r"-?\d+\.?\d*", s)]

_impact_map = {
    "STRONGLY POSITIVE": 5, "BEAT": 5,
    "POSITIVE": 4,
    "NEUTRAL": 3, "MATCHED": 3,
    "NEGATIVE": 2,
    "STRONGLY NEGATIVE": 1, "MISSED": 1,
}


def _extract_text(pdf_bytes: bytes, max_pages=5, max_chars=12_000) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    txt = ""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for i, p in enumerate(pdf.pages):
                if i >= max_pages or len(txt) > max_chars:
                    break
                txt += (p.extract_text() or "") + "\n"
    except Exception as e:
        print(f"  [extract] Error: {e}")
        return ""
    return txt[:max_chars]


def _call_llm(prompt, user, retries=3, max_tokens=400, temperature=0.3):
    """Call OpenAI with retries."""
    for attempt in range(retries):
        try:
            return client.chat.completions.create(
                model=MODEL_SCREEN,
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": user},
                ],
                max_tokens=max_tokens,
                temperature=temperature,
            ).choices[0].message.content.strip()
        except (openai.APIConnectionError, ReadTimeout, ConnectError) as e:
            if attempt == retries - 1:
                print(f"  [llm] Connection error (final attempt): {e}")
                return None
            print(f"  [llm] Connection error (attempt {attempt+1}): {e}")
        except Exception as e:
            print(f"  [llm] Unexpected error: {e}")
            return None
    return None


def _process_single_pdf(pdf_row: pd.Series) -> Optional[dict]:
    """
    Process one PDF: extract text → screen with LLM → classify category.
    Returns a dict ready for the predictions DataFrame, or None on failure.
    """
    scrip_cd = str(pdf_row.get("SCRIP_CD", ""))
    pdf_link = str(pdf_row.get("ATTACHMENTNAME", ""))
    news_sub_dt = pdf_row.get("News_submission_dt")
    company_name_hint = str(pdf_row.get("SLONGNAME", ""))

    text = _extract_text(pdf_row["pdf_content"])
    if not text or len(text) < 100:
        print(f"  [skip] SCRIP {scrip_cd}: PDF text too short ({len(text)} chars)")
        return None

    # 1) SCREENING
    resp = _call_llm(PROMPT_SCREEN, text + "\nReturn one line only.")
    if not resp:
        print(f"  [skip] SCRIP {scrip_cd}: LLM returned nothing")
        return None

    parts = _split_line(resp)
    if len(parts) != 5:
        print(f"  [skip] SCRIP {scrip_cd}: LLM returned {len(parts)} fields instead of 5")
        return None

    company, imp_tag, summary, price_range, rationale = [p.strip() for p in parts]
    imp_tag = imp_tag.upper()

    # Skip immaterial filings
    if imp_tag == "N/A":
        print(f"  [skip] SCRIP {scrip_cd}: Impact = N/A (immaterial)")
        return None

    # 2) CATEGORY (independent – does not affect prediction)
    cat_resp = _call_llm(PROMPT_CATEGORY, text, max_tokens=16, temperature=0.0)
    if not cat_resp or cat_resp not in CATEGORY_LABELS:
        cat_resp = _guess_category(text)

    return {
        "File": scrip_cd,
        "PDF_Link": pdf_link,
        "Company": company or company_name_hint,
        "SCRIP_CD": scrip_cd,
        "Impact": imp_tag,
        "Summary": summary,
        "Price_Range": price_range,
        "Rationale": rationale,
        "Category": cat_resp,
        "News_submission_dt": news_sub_dt,  # carried through from raw data
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def analyze_pdfs_from_dataframe(pdf_df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyse every PDF in *pdf_df*, rank them, return the top 30.
    """
    if pdf_df.empty:
        print("No PDFs to analyse.")
        return pd.DataFrame()

    pdf_tasks = [row for _, row in pdf_df.iterrows()]
    print(f"Starting analysis of {len(pdf_tasks)} PDFs ...")

    rows = []
    with ThreadPoolExecutor(max_workers=2) as pool:
        future_map = {pool.submit(_process_single_pdf, task): task.get("SCRIP_CD") for task in pdf_tasks}
        for i, future in enumerate(as_completed(future_map), 1):
            result = future.result()
            status = "OK" if result else "skipped"
            print(f"  [{i}/{len(pdf_tasks)}] SCRIP {future_map[future]}: {status}")
            if result:
                rows.append(result)

    if not rows:
        print("No valid PDFs produced predictions.")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Derived metrics
    df["Impact_Score"] = df["Impact"].apply(lambda t: _impact_map.get(t.upper(), 0))
    df["Mid_%"] = df["Price_Range"].apply(
        lambda r: (sum(_price_mid(r)) / len(_price_mid(r))) if _price_mid(r) else 0.0
    )

    # Rank
    df.sort_values(["Impact_Score", "Mid_%"], ascending=[False, False], inplace=True)
    df = df.head(30).copy()
    df.reset_index(drop=True, inplace=True)
    df.insert(0, "Rank", df.index + 1)
    df["SCRIP_CD"] = df["SCRIP_CD"].astype(str)

    print(f"Analysis complete. {len(df)} predictions ranked.")
    return df
