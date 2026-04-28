# News pipeline — what each step does + how long

Run everything in order (same as `run_daily_pipeline.sh`):

| Step | Script | What it does | ~Time |
|:---:|:---|:---|:---:|
| **1** | `fetch_news.py` | Downloads **all RSS feeds** (~40); writes `news_today.xlsx` (mixed dates). | **10–20 s** |
| **2** | `dedupe_news.py --date YYYY-MM-DD` | Keeps **one calendar day (IST)**; drops junk; URL + fuzzy **title dedup** → `news_<ddmmm>.xlsx`. | **2–10 s** |
| **3** | `screen_news.py` | **Smaller model (nano)**: cheap relevance score; drops weak rows → `*_cheapest_model.xlsx`. | **2–5 min** |
| **4** | `refine_news.py` | **Larger model (mini)**: category, direction, tighter score → `*_mini_model_refined.xlsx`. | **2–4 min** |
| **5** | `enrich_news.py` | **Mini** again: **fetch article HTML**, richer summary; trims earnings-type noise → `*_enriched.xlsx`. | **2–4 min** |
| **6** | `dedupe_v1.py` | Rule **dedupe**: order-win overlap, per-stock story collapse → `*_v1.xlsx`. | **2–10 s** |
| **7** | `remap_industries.py` | **Mini**: map text industries → **24 canonical** labels → `*_v2.xlsx`. | **20–90 s** |
| **8** | `focus_stocks.py` | **Mini**: attach **0–2 focal** Indian listed names per row → `*_v3.xlsx`. | **30–90 s** |
| **9** | `dedupe_v4.py` | **Global** cross-publisher **title dedup** → `*_v4.xlsx`. | **2–10 s** |
| **10** | `build_final.py` | **Mini**: per-industry bullet briefings + assemble **final workbook**; adds **BSE, NSE, ISIN, market_cap_cr** on stock rows (same resolver as step 11). | **1–4 min** |
| **11** | `add_stock_codes.py` | **Optional**: re-resolve tickers/mcaps; copies current file to **`.bak.xlsx`** then overwrites main. | **1–6 min** (API rate limits vary) |

**Total end-to-end:** about **10–20 minutes** when APIs behave; longer if Indian API/Yahoo return many **429**s.

---

## One command (after `chmod +x`)

```bash
cd news_scratch
chmod +x run_daily_pipeline.sh
ISO_DATE=2026-04-27 TAG=27apr ./run_daily_pipeline.sh
```

For another day, set `ISO_DATE` and `TAG` (e.g. `28apr`).

---

## Cleanup

Intermediate `news_*` files are disposable. Keep `outputs/<ISO_DATE>_briefing.xlsx` (and `.bak` if you use step 11).
