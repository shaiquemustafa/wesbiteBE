"""
Persist news_scratch final Excel (three sheets) into Postgres.

All wall-clock columns are naive IST (TIMESTAMP WITHOUT TIME ZONE), matching
fetch_news / dedupe_news conventions.
"""
from __future__ import annotations

import hashlib
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from psycopg2.extras import execute_batch

from database import get_conn

IST = timezone(timedelta(hours=5, minutes=30))


def story_fingerprint(link: Any, title: Any) -> str:
    s = f"{str(link or '').strip().lower()}|{str(title or '').strip().lower()}"
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def dedupe_fingerprint(story_fp: str, scope: str) -> str:
    """
    Scope dedupe keys by logical bucket ("stock_news" vs "all_items_by_industry")
    so one sheet cannot suppress inserts from the other.
    """
    seed = f"{scope}|{story_fp}"
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def _cell(v: Any) -> Any:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime().replace(tzinfo=None)
    return v


def prune_seen_stories_older_than(conn, days: int = 14) -> int:
    """Drop cross-cycle dedupe keys older than `days` (IST calendar dates)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM news_briefing_seen_stories
            WHERE briefing_date_ist < (timezone('Asia/Kolkata', now()))::date - %s::integer
            """,
            (days,),
        )
        return cur.rowcount


def delete_runs_for_cycle_dates(
    conn,
    cycle: str,
    briefing_date_ist: date,
) -> None:
    """
    Remove this cycle's run for `briefing_date_ist` (same-day retry) and for the
    prior IST calendar day (yesterday same slot). CASCADE deletes child rows;
    we also remove seen_stories rows keyed to those runs so fingerprints can
    reappear on a retry.
    """
    prev = briefing_date_ist - timedelta(days=1)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id FROM news_briefing_runs
            WHERE cycle = %s AND briefing_date_ist IN (%s, %s)
            """,
            (cycle, briefing_date_ist, prev),
        )
        ids = [row[0] for row in cur.fetchall()]
        if ids:
            cur.execute(
                "DELETE FROM news_briefing_seen_stories WHERE first_seen_run_id = ANY(%s)",
                (ids,),
            )
        cur.execute(
            """
            DELETE FROM news_briefing_runs
            WHERE cycle = %s AND briefing_date_ist IN (%s, %s)
            """,
            (cycle, briefing_date_ist, prev),
        )


def insert_run_row(
    conn,
    *,
    cycle: str,
    briefing_date_ist: date,
    run_at_ist: datetime,
    window_start_ist: datetime,
    window_end_ist: datetime,
    status: str = "completed",
    error_message: Optional[str] = None,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO news_briefing_runs (
                cycle, briefing_date_ist, run_at_ist,
                window_start_ist, window_end_ist, status, error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                cycle,
                briefing_date_ist,
                run_at_ist,
                window_start_ist,
                window_end_ist,
                status,
                error_message,
            ),
        )
        rid = cur.fetchone()[0]
    return int(rid)


def update_run_counts(
    conn,
    run_id: int,
    *,
    stock_news_rows: int,
    industry_insight_rows: int,
    all_items_rows: int,
    cross_cycle_skipped: int,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE news_briefing_runs SET
                stock_news_rows = %s,
                industry_insight_rows = %s,
                all_items_rows = %s,
                cross_cycle_skipped = %s
            WHERE id = %s
            """,
            (
                stock_news_rows,
                industry_insight_rows,
                all_items_rows,
                cross_cycle_skipped,
                run_id,
            ),
        )


def mark_run_failed(conn, run_id: int, msg: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE news_briefing_runs
            SET status = 'failed', error_message = %s
            WHERE id = %s
            """,
            (msg[:8000], run_id),
        )


def ingest_excel(
    conn,
    xlsx_path: Path,
    *,
    run_id: int,
    cycle: str,
    briefing_date_ist: date,
) -> Tuple[int, int, int, int]:
    """
    Load industry_insights + industry_small + stock_news + all_items_by_industry.
    Returns (n_insights, n_stock, n_all, n_skipped_duplicates).
    """
    xlsx_path = Path(xlsx_path)
    if not xlsx_path.exists():
        raise FileNotFoundError(str(xlsx_path))

    xl = pd.ExcelFile(xlsx_path)
    skipped = 0

    # --- Industry (full + small) ---
    insight_rows: List[Tuple[Any, ...]] = []
    so = 0
    for tier, sheet in (("full", "industry_insights"), ("small", "industry_small")):
        if sheet not in xl.sheet_names:
            continue
        df = pd.read_excel(xlsx_path, sheet_name=sheet).fillna("")
        for _, r in df.iterrows():
            insight_rows.append(
                (
                    run_id,
                    tier,
                    so,
                    str(r.get("industry") or "")[:500],
                    int(r["n_items"]) if pd.notna(r.get("n_items")) else None,
                    str(r.get("direction") or "")[:50],
                    str(r.get("headline") or "")[:4000],
                    str(r.get("bullets") or ""),
                    str(r.get("key_numbers") or "")[:2000],
                    str(r.get("key_stocks") or "")[:2000],
                    str(r.get("key_themes") or "")[:2000],
                )
            )
            so += 1

    with conn.cursor() as cur:
        if insight_rows:
            execute_batch(
                cur,
                """
                INSERT INTO news_briefing_industry_insights (
                    run_id, insight_tier, sort_order, industry, n_items, direction,
                    headline, bullets, key_numbers, key_stocks, key_themes
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                insight_rows,
                page_size=200,
            )

    # --- stock_news with cross-cycle dedupe ---
    sn_rows: List[Tuple[Any, ...]] = []
    if "stock_news" in xl.sheet_names:
        sdf = pd.read_excel(xlsx_path, sheet_name="stock_news").fillna("")
    else:
        sdf = pd.DataFrame()

    with conn.cursor() as cur:
        for i, (_, r) in enumerate(sdf.iterrows()):
            link = r.get("link")
            title = r.get("title")
            fp = story_fingerprint(link, title)
            seen_fp = dedupe_fingerprint(fp, "stock_news")
            cur.execute(
                """
                INSERT INTO news_briefing_seen_stories (
                    briefing_date_ist, story_fingerprint, first_seen_run_id, first_seen_cycle
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (briefing_date_ist, story_fingerprint) DO NOTHING
                RETURNING story_fingerprint
                """,
                (briefing_date_ist, seen_fp, run_id, cycle),
            )
            if cur.fetchone() is None:
                skipped += 1
                continue
            sn_rows.append(
                (
                    run_id,
                    i,
                    str(r.get("affected_stocks") or "")[:2000],
                    str(r.get("bse_scrip_code") or "")[:40],
                    str(r.get("nse_symbol") or "")[:40],
                    str(r.get("isin") or "")[:20],
                    str(r.get("market_cap_cr") or "")[:80],
                    float(r["score"]) if pd.notna(r.get("score")) else None,
                    str(r.get("direction") or "")[:40],
                    str(r.get("industry_primary") or "")[:500],
                    str(r.get("affected_industries") or "")[:2000],
                    str(r.get("ai_summary") or ""),
                    str(r.get("implication") or ""),
                    str(r.get("key_numbers") or "")[:2000],
                    str(r.get("category") or "")[:120],
                    str(r.get("source") or "")[:300],
                    _cell(r.get("published_at_ist")),
                    str(r.get("title") or "")[:2000],
                    str(r.get("link") or "")[:3000],
                    fp,
                )
            )

    with conn.cursor() as cur:
        if sn_rows:
            execute_batch(
                cur,
                """
                INSERT INTO news_briefing_stock_news (
                    run_id, sort_order, affected_stocks, bse_scrip_code, nse_symbol, isin,
                    market_cap_cr, score, direction, industry_primary, affected_industries,
                    ai_summary, implication, key_numbers, category, source,
                    published_at_ist, title, link, story_fingerprint
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                sn_rows,
                page_size=100,
            )

    # --- all_items_by_industry ---
    ai_rows: List[Tuple[Any, ...]] = []
    if "all_items_by_industry" in xl.sheet_names:
        adf = pd.read_excel(xlsx_path, sheet_name="all_items_by_industry").fillna("")
    else:
        adf = pd.DataFrame()

    with conn.cursor() as cur:
        for j, (_, r) in enumerate(adf.iterrows()):
            link = r.get("link")
            title = r.get("title")
            fp = story_fingerprint(link, title)
            seen_fp = dedupe_fingerprint(fp, "all_items_by_industry")
            cur.execute(
                """
                INSERT INTO news_briefing_seen_stories (
                    briefing_date_ist, story_fingerprint, first_seen_run_id, first_seen_cycle
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (briefing_date_ist, story_fingerprint) DO NOTHING
                RETURNING story_fingerprint
                """,
                (briefing_date_ist, seen_fp, run_id, cycle),
            )
            if cur.fetchone() is None:
                skipped += 1
                continue
            ai_rows.append(
                (
                    run_id,
                    j,
                    str(r.get("industry_primary") or "")[:500],
                    str(r.get("affected_stocks") or "")[:2000],
                    str(r.get("bse_scrip_code") or "")[:40],
                    str(r.get("nse_symbol") or "")[:40],
                    str(r.get("isin") or "")[:20],
                    str(r.get("market_cap_cr") or "")[:80],
                    float(r["score"]) if pd.notna(r.get("score")) else None,
                    str(r.get("direction") or "")[:40],
                    str(r.get("category") or "")[:120],
                    str(r.get("ai_summary") or ""),
                    str(r.get("implication") or ""),
                    str(r.get("key_numbers") or "")[:2000],
                    str(r.get("source") or "")[:300],
                    _cell(r.get("published_at_ist")),
                    str(r.get("title") or "")[:2000],
                    str(r.get("link") or "")[:3000],
                    fp,
                )
            )

    with conn.cursor() as cur:
        if ai_rows:
            execute_batch(
                cur,
                """
                INSERT INTO news_briefing_all_items (
                    run_id, sort_order, industry_primary, affected_stocks,
                    bse_scrip_code, nse_symbol, isin, market_cap_cr, score, direction,
                    category, ai_summary, implication, key_numbers, source,
                    published_at_ist, title, link, story_fingerprint
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                ai_rows,
                page_size=150,
            )

    update_run_counts(
        conn,
        run_id,
        stock_news_rows=len(sn_rows),
        industry_insight_rows=len(insight_rows),
        all_items_rows=len(ai_rows),
        cross_cycle_skipped=skipped,
    )
    return len(insight_rows), len(sn_rows), len(ai_rows), skipped


def ingest_briefing_file(
    *,
    xlsx_path: Path,
    cycle: str,
    briefing_date_ist: date,
    run_at_ist: datetime,
    window_start_ist: datetime,
    window_end_ist: datetime,
) -> Dict[str, Any]:
    """
    Transaction: delete old runs (same cycle, today+yesterday dates), insert run,
    ingest sheets, prune old seen rows.
    """
    xlsx_path = Path(xlsx_path)
    prune_days = int(os.getenv("NEWS_BRIEFING_SEEN_PRUNE_DAYS", "14"))
    with get_conn() as conn:
        prune_seen_stories_older_than(conn, days=prune_days)
        delete_runs_for_cycle_dates(conn, cycle, briefing_date_ist)
        rid = insert_run_row(
            conn,
            cycle=cycle,
            briefing_date_ist=briefing_date_ist,
            run_at_ist=run_at_ist,
            window_start_ist=window_start_ist,
            window_end_ist=window_end_ist,
            status="completed",
            error_message=None,
        )
        n_i, n_s, n_a, skip = ingest_excel(
            conn,
            xlsx_path,
            run_id=rid,
            cycle=cycle,
            briefing_date_ist=briefing_date_ist,
        )
    return {
        "run_id": rid,
        "industry_insight_rows": n_i,
        "stock_news_rows": n_s,
        "all_items_rows": n_a,
        "cross_cycle_skipped": skip,
        "path": str(xlsx_path),
    }
