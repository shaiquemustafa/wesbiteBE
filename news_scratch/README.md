# news_scratch

End-to-end pipeline that turns Indian financial RSS feeds into a daily, user-ready briefing (stock-level news + industry-level insights), with an Excel report as the single deliverable per day.

---

## Folder layout

```
news_scratch/
├── outputs/                          ← the only thing you ever need to open
│   ├── 2026-04-22_briefing.xlsx     ← finished report for 22 Apr
│   └── 2026-04-25_briefing.xlsx     ← finished report for 25 Apr
│
├── fetch_news.py                     ← stage 1: pull RSS
├── dedupe_news.py                    ← stage 2: filter to date + dedup titles
├── screen_news.py                    ← stage 3: cheap LLM relevance screen (nano)
├── refine_news.py                    ← stage 4: LLM categorize + score (mini)
├── enrich_news.py                    ← stage 5: scrape article + rich summary (mini)
├── dedupe_v1.py                      ← stage 6: drop order_win + per-stock dedup
├── remap_industries.py               ← stage 7: snap to fixed 24-industry taxonomy
├── focus_stocks.py                   ← stage 8: pick 0/1/2 focal Indian-listed stocks
├── dedupe_v4.py                      ← stage 9: global title dedup across publishers
├── build_final.py                    ← stage 10: industry insights + final Excel (+ BSE/NSE/mcap on stock rows)
├── add_stock_codes.py                ← optional: re-enrich an existing briefing xlsx in-place (writes .bak)
│
├── explore_news.ipynb                ← scratch notebook
├── README.md                         ← this file
├── .env                              ← OPENAI_API_KEY (gitignored)
├── .gitignore
└── .venv/                            ← Python env (gitignored)
```

All intermediate files (`news_today.*`, `news_<date>.*`, `news_<date>_v1..v4.*`, `_enriched`, `_refined`, `_cheapest_model`) are **disposable** — they get re-created from RSS in ~3 minutes. Only `outputs/<date>_briefing.xlsx` is meant to be kept.

---

## What's inside each `<date>_briefing.xlsx`

Open it in Excel. Sheets, in order:

| Sheet | What it is |
| --- | --- |
| `stock_news` | One row per individual stock-focused news item. After `affected_stocks`: `bse_scrip_code`, `nse_symbol`, `isin`, `market_cap_cr` (from Indian API + fallbacks), then score, category, direction, summaries, source URL. |
| `industry_insights` | LLM-generated multi-bullet briefings for industries with ≥5 items that day (e.g. Financial Services, Energy, IT). Each industry gets a headline + 5–10 bullets covering the main developments. |
| `industry_small` | Industries with 1–4 items that didn't qualify for an LLM briefing — listed so nothing is lost. |
| `all_items_by_industry` | Every surviving item grouped by primary industry — the master view. |
| `overview` | Industry manifest: counts, dominant direction, sample headlines. |
| `run_stats` | Funnel stats + LLM token spend for the day. |
| `README` | Embedded mini-readme. |

---

## How to run for a new date

Today's briefing (auto-detects yesterday IST):

```bash
cd news_scratch
.venv/bin/python fetch_news.py
.venv/bin/python dedupe_news.py                     # defaults to yesterday IST
.venv/bin/python screen_news.py     --input news_<ddmmm>.xlsx
.venv/bin/python refine_news.py     --input news_<ddmmm>_cheapest_model.xlsx
.venv/bin/python enrich_news.py     --input news_<ddmmm>_mini_model_refined.xlsx
.venv/bin/python dedupe_v1.py       --input news_<ddmmm>_enriched.xlsx --output news_<ddmmm>_v1.xlsx
.venv/bin/python remap_industries.py --input news_<ddmmm>_v1.xlsx --output news_<ddmmm>_v2.xlsx --model gpt-4.1-mini
.venv/bin/python focus_stocks.py    --input news_<ddmmm>_v2.xlsx --output news_<ddmmm>_v3.xlsx
.venv/bin/python dedupe_v4.py       --input news_<ddmmm>_v3.xlsx --output news_<ddmmm>_v4.xlsx
.venv/bin/python build_final.py     --input news_<ddmmm>_v4.xlsx --output outputs/<YYYY-MM-DD>_briefing.xlsx
```

Replace `<ddmmm>` with e.g. `25apr` and `<YYYY-MM-DD>` with `2026-04-25`. Then delete every `news_*` file at the project root — only `outputs/<YYYY-MM-DD>_briefing.xlsx` should remain.

`build_final.py` already adds BSE/NSE/ISIN and `market_cap_cr` on stock rows (same resolver as `add_stock_codes.py`). Run `add_stock_codes.py --input outputs/<date>_briefing.xlsx` only to refresh an older workbook without re-running the LLM funnel; it writes `<name>.bak.xlsx` before overwriting.

For a specific past date, pass `--date 2026-04-25` to `dedupe_news.py`. RSS only stores the last ~20–100 items per feed, so going back more than 2–3 days will return very few rows.

End-to-end runtime: ~3 minutes. End-to-end LLM cost: ~$0.20–$0.40 per day depending on news volume.

---

## Setup (only if `.venv` ever needs rebuilding)

```bash
cd news_scratch
python3 -m venv .venv
.venv/bin/pip install feedparser requests pandas openpyxl python-dateutil \
                     truststore openai python-dotenv trafilatura ftfy rapidfuzz
```

`truststore` is required so Python trusts the macOS keychain (AV/proxy roots).

`.env` must contain:

```
OPENAI_API_KEY=sk-...
```

---

## Sources

~42 RSS feeds, pulled in parallel:

- **Direct publisher RSS** — Mint, Economic Times, Business Standard, Hindu BusinessLine, CNBC TV18, Business Today.
- **Via Google News site-scoped RSS** — Moneycontrol, NDTV Profit, Zee Business, Financial Express, Bloomberg India, Reuters India, Outlook Business, Forbes India, The Ken, Inc42, YourStory, Entrackr (these block direct RSS).

---

## Pipeline funnel (typical day)

| Stage | Items | Why we lose rows |
| --- | --- | --- |
| RSS pull | ~2,500 | All dates, all sources |
| Filter to one IST date | ~1,200 | |
| URL + junk + fuzzy title dedup | ~950 | Same headline reprinted across publishers |
| Nano relevance screen (score ≥ 6) | ~300 | Politics, lifestyle, generic macro dropped |
| Mini refine (score ≥ 7) | ~150 | Stricter relevance + categorization |
| Enrich + drop earnings + scrape article | ~85 | Earnings handled separately by BSE pipeline |
| Drop order_win + per-stock dedup | ~70 | Order wins handled separately too |
| Industry remap + focal stock + global dedup | ~65–70 | Cross-publisher duplicates collapsed |
| **Final briefing** | **~60–70** | One row per real story |

---

## Known issues & next steps

- `build_final.py` writes its CSV side-files with a hardcoded `news_22apr_final_*` prefix — these CSVs duplicate the Excel sheets so we delete them on cleanup. Fix when consolidating the pipeline.
- We discussed collapsing the 10 stages into 5 (single LLM call for refine+enrich+industry+stock, embedding-based semantic dedup at the end). See chat history for the proposed plan.
