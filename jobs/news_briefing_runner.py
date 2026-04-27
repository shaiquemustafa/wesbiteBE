"""
Run the news_scratch RSS → Excel pipeline for one IST window (pre / during / post)
and ingest the final workbook into Postgres.

Uses news_scratch/.venv/bin/python when present, else the current interpreter.
"""
from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

IST = timezone(timedelta(hours=5, minutes=30))

logger = logging.getLogger("uvicorn.error")

# This file lives at wesbiteBE/jobs/… — repo root is one level above wesbiteBE.
_WESBITE_BE = Path(__file__).resolve().parent.parent
_REPO_ROOT = _WESBITE_BE.parent
SCRATCH = Path(os.getenv("NEWS_SCRATCH_DIR", str(_REPO_ROOT / "news_scratch"))).resolve()

CYCLE_TAGS = {
    "pre_market": "pre",
    "during_market": "mid",
    "post_market": "post",
}


def _python_exe() -> str:
    vpy = SCRATCH / ".venv" / "bin" / "python"
    if vpy.exists():
        return str(vpy)
    return sys.executable


def _now_ist_naive() -> datetime:
    return datetime.now(IST).replace(tzinfo=None)


def compute_ist_window(cycle: str, now_ist: Optional[datetime] = None) -> Tuple[datetime, datetime, date]:
    """
    Inclusive naive-IST bounds [start, end] and briefing_date_ist (trading session date).
    """
    now_ist = now_ist or _now_ist_naive()
    d = now_ist.date()

    def combine(day: date, hh: int, mm: int) -> datetime:
        return datetime(
            day.year, day.month, day.day, hh, mm, 0, 0,
        )

    if cycle == "pre_market":
        start = combine(d - timedelta(days=1), 22, 0)
        end = combine(d, 8, 30)
    elif cycle == "during_market":
        start = combine(d, 8, 30)
        end = combine(d, 15, 30)
    elif cycle == "post_market":
        start = combine(d, 15, 30)
        end = combine(d, 22, 0)
    else:
        raise ValueError(f"Unknown cycle {cycle!r}")

    return start, end, d


def _run(cmd: List[str], *, timeout: int = 3600) -> None:
    env = os.environ.copy()
    env.setdefault("PYTHONUTF8", "1")
    logger.info("news_briefing subprocess: %s", " ".join(cmd))
    subprocess.run(
        cmd,
        cwd=str(SCRATCH),
        env=env,
        check=True,
        timeout=timeout,
    )


def run_pipeline_and_ingest(cycle: str) -> Dict[str, Any]:
    """
    Full fetch → dedupe (IST window) → … → build_final → DB ingest.
    `cycle`: pre_market | during_market | post_market
    """
    if cycle not in CYCLE_TAGS:
        raise ValueError(f"Invalid cycle {cycle!r}")

    py = _python_exe()
    win_start, win_end, briefing_d = compute_ist_window(cycle)
    run_at = _now_ist_naive()

    ws = win_start.strftime("%Y-%m-%d %H:%M")
    we = win_end.strftime("%Y-%m-%d %H:%M")
    iso_d = briefing_d.isoformat()
    tag = f"{briefing_d.strftime('%d%b').lower()}_{CYCLE_TAGS[cycle]}"

    out_final = SCRATCH / "outputs" / f"{iso_d}_briefing_{cycle}.xlsx"
    out_final.parent.mkdir(parents=True, exist_ok=True)

    # 1 fetch
    _run([py, "fetch_news.py"], timeout=120)
    # 2 dedupe window
    _run(
        [
            py,
            "dedupe_news.py",
            "--date",
            iso_d,
            "--ist-window-start",
            ws,
            "--ist-window-end",
            we,
            "--output",
            f"news_{tag}.xlsx",
        ],
        timeout=120,
    )
    stem = f"news_{tag}"
    # 3 screen
    _run([py, "screen_news.py", "--input", f"{stem}.xlsx"], timeout=3600)
    # 4 refine
    _run(
        [py, "refine_news.py", "--input", f"{stem}_cheapest_model.xlsx"],
        timeout=3600,
    )
    # 5 enrich
    _run(
        [py, "enrich_news.py", "--input", f"{stem}_mini_model_refined.xlsx"],
        timeout=3600,
    )
    # 6 v1
    _run(
        [
            py,
            "dedupe_v1.py",
            "--input",
            f"{stem}_enriched.xlsx",
            "--output",
            f"{stem}_v1.xlsx",
        ],
        timeout=300,
    )
    # 7 remap
    _run(
        [
            py,
            "remap_industries.py",
            "--input",
            f"{stem}_v1.xlsx",
            "--output",
            f"{stem}_v2.xlsx",
            "--model",
            "gpt-4.1-mini",
        ],
        timeout=1800,
    )
    # 8 focus
    _run(
        [
            py,
            "focus_stocks.py",
            "--input",
            f"{stem}_v2.xlsx",
            "--output",
            f"{stem}_v3.xlsx",
        ],
        timeout=1800,
    )
    # 9 v4
    _run(
        [
            py,
            "dedupe_v4.py",
            "--input",
            f"{stem}_v3.xlsx",
            "--output",
            f"{stem}_v4.xlsx",
        ],
        timeout=300,
    )
    # 10 final
    _run(
        [
            py,
            "build_final.py",
            "--input",
            f"{stem}_v4.xlsx",
            "--output",
            str(out_final),
        ],
        timeout=3600,
    )

    from service.news_briefing_service import ingest_briefing_file

    meta = ingest_briefing_file(
        xlsx_path=out_final,
        cycle=cycle,
        briefing_date_ist=briefing_d,
        run_at_ist=run_at,
        window_start_ist=win_start,
        window_end_ist=win_end,
    )
    meta.update(
        {
            "cycle": cycle,
            "briefing_date_ist": iso_d,
            "window_start_ist": ws,
            "window_end_ist": we,
            "excel_path": str(out_final),
        }
    )
    logger.info("news_briefing ingest OK: %s", meta)
    return meta
