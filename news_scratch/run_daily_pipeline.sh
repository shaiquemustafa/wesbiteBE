#!/usr/bin/env bash
# Daily news → final Excel. Edit ISO_DATE + TAG below for each run.
set -euo pipefail
cd "$(dirname "$0")"

ISO_DATE="${ISO_DATE:-2026-04-27}"   # YYYY-MM-DD (IST calendar day for dedupe)
TAG="${TAG:-27apr}"                  # must match dedupe_news output: news_<TAG>.xlsx
OUT="outputs/${ISO_DATE}_briefing.xlsx"
PY="${PY:-.venv/bin/python}"

step() {
  local n="$1" name="$2" est="$3"
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "STEP $n — $name"
  echo "  Rough time: $est"
  echo "  Started: $(date -Iseconds)"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

step_end() {
  echo "  Finished: $(date -Iseconds)"
}

step 1 "Fetch all RSS feeds → news_today.xlsx / .csv" "~10s"
$PY fetch_news.py
step_end

step 2 "Keep one IST day + dedupe URLs/titles → news_${TAG}.xlsx" "~5s"
$PY dedupe_news.py --date "$ISO_DATE"
step_end

step 3 "LLM screen (gpt-4.1-nano) relevance → news_${TAG}_cheapest_model.xlsx" "~2–4 min"
$PY screen_news.py --input "news_${TAG}.xlsx"
step_end

step 4 "LLM refine (mini) categories + scores → news_${TAG}_mini_model_refined.xlsx" "~2–3 min"
$PY refine_news.py --input "news_${TAG}_cheapest_model.xlsx"
step_end

step 5 "Scrape articles + LLM enrich (mini) → news_${TAG}_enriched.xlsx" "~2–4 min"
$PY enrich_news.py --input "news_${TAG}_mini_model_refined.xlsx"
step_end

step 6 "Dedupe v1 (order wins / per-stock) → news_${TAG}_v1.xlsx" "~5s"
$PY dedupe_v1.py --input "news_${TAG}_enriched.xlsx" --output "news_${TAG}_v1.xlsx"
step_end

step 7 "Remap to 24 industries (mini) → news_${TAG}_v2.xlsx" "~20–60s"
$PY remap_industries.py --input "news_${TAG}_v1.xlsx" --output "news_${TAG}_v2.xlsx" --model gpt-4.1-mini
step_end

step 8 "Pick focal stocks (mini) → news_${TAG}_v3.xlsx" "~30–90s"
$PY focus_stocks.py --input "news_${TAG}_v2.xlsx" --output "news_${TAG}_v3.xlsx"
step_end

step 9 "Dedupe v4 (global title collapse) → news_${TAG}_v4.xlsx" "~5s"
$PY dedupe_v4.py --input "news_${TAG}_v3.xlsx" --output "news_${TAG}_v4.xlsx"
step_end

step 10 "Final Excel: industry briefings + BSE/NSE/ISIN/mcap on stock rows → $OUT" "~1–4 min"
$PY build_final.py --input "news_${TAG}_v4.xlsx" --output "$OUT"
step_end

step 11 "Optional: refresh lookups + write ${OUT%.xlsx}.bak.xlsx before overwrite" "~1–5 min"
$PY add_stock_codes.py --input "$OUT"
step_end

echo ""
echo "ALL STEPS COMPLETE."
echo "  Deliverable: $OUT"
ls -la "$OUT" "${OUT%.xlsx}.bak.xlsx" 2>/dev/null || ls -la "$OUT"
