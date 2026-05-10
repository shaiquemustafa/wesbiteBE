"""
IST UI digest summaries over ui_data windows (midday / evening slots).
Uses naive IST datetimes for windows, consistent with the rest of the backend.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional

from database import get_conn

logger = logging.getLogger("uvicorn.error")

SLOT_MIDDAY = "12_30"
SLOT_EVENING = "19_30"

UI_SUMMARY_MIN_OBSERVATIONS = max(0, int(os.getenv("UI_SUMMARY_MIN_OBSERVATIONS", "4")))
UI_SUMMARY_MODEL = os.getenv("UI_SUMMARY_MODEL", "gpt-4.1-mini").strip() or "gpt-4.1-mini"

_DIGEST_SYSTEM = """You write concise WhatsApp-friendly digests of stock market news observations.
Rules:
- Plain text only (no markdown headings unless simple bullets).
- Group items by theme or category where it helps readability.
- Keep names readable; if many distinct companies appear, summarize clusters and say \"and N other names omitted\" rather than listing everything.
- Lead with what matters most for investors.
- Stay within reasonable length for a single WhatsApp message."""


def _naive_ist(d: date, hh: int, mm: int) -> datetime:
    return datetime.combine(d, time(hh, mm, 0))


def midday_window_ist(briefing_day: date) -> tuple[datetime, datetime]:
    """D-1 07:30 through D 12:30 IST inclusive."""
    start = _naive_ist(briefing_day - timedelta(days=1), 7, 30)
    end = _naive_ist(briefing_day, 12, 30)
    return start, end


def evening_window_normal_ist(briefing_day: date) -> tuple[datetime, datetime]:
    """news_time strictly after D 12:30 through D 19:30 IST inclusive."""
    start = _naive_ist(briefing_day, 12, 30)
    end = _naive_ist(briefing_day, 19, 30)
    return start, end


def evening_window_extended_ist(briefing_day: date) -> tuple[datetime, datetime]:
    """D-1 07:30 through D 19:30 IST inclusive."""
    start = _naive_ist(briefing_day - timedelta(days=1), 7, 30)
    end = _naive_ist(briefing_day, 19, 30)
    return start, end


def fetch_ui_rows_in_window(
    start_ist: datetime,
    end_ist: datetime,
    *,
    start_exclusive: bool = False,
    end_inclusive: bool = True,
) -> List[Dict[str, Any]]:
    """
    Load ui_data rows whose news_time falls in the IST wall-clock window.
    start_ist/end_ist are naive datetimes in IST.
    """
    if start_exclusive:
        start_op = ">"
    else:
        start_op = ">="
    if end_inclusive:
        end_op = "<="
    else:
        end_op = "<"

    sql = f"""
        SELECT id, news_time, data
        FROM ui_data
        WHERE (news_time AT TIME ZONE 'Asia/Kolkata') {start_op} %s::timestamp
          AND (news_time AT TIME ZONE 'Asia/Kolkata') {end_op} %s::timestamp
        ORDER BY news_time ASC
    """

    out: List[Dict[str, Any]] = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (start_ist, end_ist))
            for row in cur.fetchall():
                rid, news_time, data = row
                blob = dict(data) if isinstance(data, dict) else {}
                blob["_ui_row_id"] = rid
                blob["_news_time"] = news_time
                out.append(blob)
    return out


def _midday_row_status(briefing_day: date) -> Optional[str]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status FROM summary_ui_data
                WHERE briefing_date_ist = %s AND slot = %s
                LIMIT 1
                """,
                (briefing_day, SLOT_MIDDAY),
            )
            r = cur.fetchone()
            return r[0] if r else None


def summary_already_done(slot: str, briefing_day: date) -> bool:
    """True if a completed summary already exists for this IST calendar day and slot."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM summary_ui_data
                WHERE briefing_date_ist = %s AND slot = %s AND status = 'completed'
                LIMIT 1
                """,
                (briefing_day, slot),
            )
            return cur.fetchone() is not None


def _delete_failed_slot_row(slot: str, briefing_day: date) -> None:
    """Allow a retry after a failed run (INSERT uses ON CONFLICT DO NOTHING)."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM summary_ui_data
                WHERE briefing_date_ist = %s AND slot = %s AND status = 'failed'
                """,
                (briefing_day, slot),
            )


def _should_skip_existing_slot(slot: str, briefing_day: date) -> bool:
    """Skip if we already finished successfully or intentionally skipped volume."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status FROM summary_ui_data
                WHERE briefing_date_ist = %s AND slot = %s
                LIMIT 1
                """,
                (briefing_day, slot),
            )
            r = cur.fetchone()
            if not r:
                return False
            return r[0] in ("completed", "skipped_low_volume")


def _insert_summary_row(
    slot: str,
    briefing_day: date,
    window_start_ist: datetime,
    window_end_ist: datetime,
    observation_count: int,
    status: str,
    summary_text: Optional[str] = None,
    model: Optional[str] = None,
    error_message: Optional[str] = None,
) -> bool:
    """
    INSERT INTO summary_ui_data ON CONFLICT (briefing_date_ist, slot) DO NOTHING.
    Returns True if a new row was inserted.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO summary_ui_data (
                    slot, briefing_date_ist, window_start_ist, window_end_ist,
                    observation_count, status, summary_text, model, error_message
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (briefing_date_ist, slot) DO NOTHING
                RETURNING id
                """,
                (
                    slot,
                    briefing_day,
                    window_start_ist,
                    window_end_ist,
                    observation_count,
                    status,
                    summary_text,
                    model,
                    error_message,
                ),
            )
            return cur.fetchone() is not None


def _build_observation_digest_lines(rows: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    names: List[str] = []
    for r in rows:
        name = (r.get("company_name") or r.get("Company") or "?").strip()
        if name:
            names.append(name)
        cat = (r.get("category") or r.get("Category") or "").strip()
        impact = (r.get("impact") or r.get("Impact") or "").strip()
        summary = (r.get("summary") or "")[:400]
        lines.append(f"- {name} | {cat} | {impact}\n  {summary}")
    # Name cardinality hint for the model
    uniq = sorted(set(names))
    if len(uniq) > 20:
        header = f"[{len(rows)} items; {len(uniq)} distinct companies — truncate long name lists in output]\n"
    else:
        header = f"[{len(rows)} items]\n"
    return header + "\n".join(lines)


def _openai_digest(observation_text: str) -> str:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set")

    from openai import OpenAI

    client = OpenAI(api_key=api_key)
    user_msg = (
        "Summarize the following market observations into one cohesive digest.\n\n"
        f"{observation_text}"
    )
    resp = client.chat.completions.create(
        model=UI_SUMMARY_MODEL,
        temperature=0.3,
        messages=[
            {"role": "system", "content": _DIGEST_SYSTEM},
            {"role": "user", "content": user_msg},
        ],
        max_tokens=1200,
    )
    text = (resp.choices[0].message.content or "").strip()
    if not text:
        raise RuntimeError("Empty completion from model")
    return text


def run_midday_summary(briefing_day: date) -> None:
    slot = SLOT_MIDDAY
    if summary_already_done(slot, briefing_day):
        logger.info("ui_summary midday: already completed for %s", briefing_day)
        return
    if _should_skip_existing_slot(slot, briefing_day):
        logger.info("ui_summary midday: existing terminal row for %s — skip", briefing_day)
        return
    _delete_failed_slot_row(slot, briefing_day)

    w_start, w_end = midday_window_ist(briefing_day)
    rows = fetch_ui_rows_in_window(
        w_start, w_end, start_exclusive=False, end_inclusive=True
    )
    n = len(rows)
    if n < UI_SUMMARY_MIN_OBSERVATIONS:
        if _insert_summary_row(
            slot,
            briefing_day,
            w_start,
            w_end,
            n,
            "skipped_low_volume",
            summary_text=None,
            model=None,
            error_message=None,
        ):
            logger.info(
                "ui_summary midday: skipped_low_volume (%s obs < %s) for %s",
                n,
                UI_SUMMARY_MIN_OBSERVATIONS,
                briefing_day,
            )
        return

    try:
        digest = _openai_digest(_build_observation_digest_lines(rows))
        inserted = _insert_summary_row(
            slot,
            briefing_day,
            w_start,
            w_end,
            n,
            "completed",
            summary_text=digest,
            model=UI_SUMMARY_MODEL,
            error_message=None,
        )
        if inserted:
            logger.info("ui_summary midday: completed for %s (%s obs)", briefing_day, n)
        else:
            logger.info("ui_summary midday: insert skipped (conflict) for %s", briefing_day)
    except Exception as e:
        logger.exception("ui_summary midday: failed for %s: %s", briefing_day, e)
        msg = str(e)[:2000]
        if _insert_summary_row(
            slot,
            briefing_day,
            w_start,
            w_end,
            n,
            "failed",
            summary_text=None,
            model=UI_SUMMARY_MODEL,
            error_message=msg,
        ):
            logger.info("ui_summary midday: recorded failed status for %s", briefing_day)


def run_evening_summary(briefing_day: date) -> None:
    slot = SLOT_EVENING
    if summary_already_done(slot, briefing_day):
        logger.info("ui_summary evening: already completed for %s", briefing_day)
        return
    if _should_skip_existing_slot(slot, briefing_day):
        logger.info("ui_summary evening: existing terminal row for %s — skip", briefing_day)
        return
    _delete_failed_slot_row(slot, briefing_day)

    midday_ok = _midday_row_status(briefing_day) == "completed"
    if midday_ok:
        w_start, w_end = evening_window_normal_ist(briefing_day)
        rows = fetch_ui_rows_in_window(
            w_start, w_end, start_exclusive=True, end_inclusive=True
        )
    else:
        w_start, w_end = evening_window_extended_ist(briefing_day)
        rows = fetch_ui_rows_in_window(
            w_start, w_end, start_exclusive=False, end_inclusive=True
        )

    n = len(rows)
    if n < UI_SUMMARY_MIN_OBSERVATIONS:
        if _insert_summary_row(
            slot,
            briefing_day,
            w_start,
            w_end,
            n,
            "skipped_low_volume",
            summary_text=None,
            model=None,
            error_message=None,
        ):
            logger.info(
                "ui_summary evening: skipped_low_volume (%s obs < %s) for %s",
                n,
                UI_SUMMARY_MIN_OBSERVATIONS,
                briefing_day,
            )
        return

    try:
        digest = _openai_digest(_build_observation_digest_lines(rows))
        inserted = _insert_summary_row(
            slot,
            briefing_day,
            w_start,
            w_end,
            n,
            "completed",
            summary_text=digest,
            model=UI_SUMMARY_MODEL,
            error_message=None,
        )
        if inserted:
            logger.info("ui_summary evening: completed for %s (%s obs)", briefing_day, n)
        else:
            logger.info("ui_summary evening: insert skipped (conflict) for %s", briefing_day)
    except Exception as e:
        logger.exception("ui_summary evening: failed for %s: %s", briefing_day, e)
        msg = str(e)[:2000]
        if _insert_summary_row(
            slot,
            briefing_day,
            w_start,
            w_end,
            n,
            "failed",
            summary_text=None,
            model=UI_SUMMARY_MODEL,
            error_message=msg,
        ):
            logger.info("ui_summary evening: recorded failed status for %s", briefing_day)


def run_slot(slot: str, briefing_day: date) -> None:
    if slot == SLOT_MIDDAY:
        run_midday_summary(briefing_day)
    elif slot == SLOT_EVENING:
        run_evening_summary(briefing_day)
    else:
        logger.warning("ui_summary: unknown slot %s", slot)


def delete_summary_slot_row(briefing_day: date, slot: str) -> int:
    """Remove any existing row for this day+slot so a manual trigger can re-run."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM summary_ui_data
                WHERE briefing_date_ist = %s AND slot = %s
                """,
                (briefing_day, slot),
            )
            return cur.rowcount


def fetch_summary_row(briefing_day: date, slot: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, slot, briefing_date_ist, window_start_ist, window_end_ist,
                       observation_count, status, summary_text, model, error_message, created_at
                FROM summary_ui_data
                WHERE briefing_date_ist = %s AND slot = %s
                LIMIT 1
                """,
                (briefing_day, slot),
            )
            r = cur.fetchone()
            if not r:
                return None
            cols = [d[0] for d in cur.description]
            out: Dict[str, Any] = {}
            for k, v in zip(cols, r):
                if isinstance(v, datetime):
                    out[k] = v.isoformat()
                elif isinstance(v, date):
                    out[k] = v.isoformat()
                else:
                    out[k] = v
            return out


def manual_trigger(slot: str, briefing_day: date, *, force: bool) -> Dict[str, Any]:
    """
    Run one summary slot immediately (for QA). If force=True, deletes existing DB row first.
    Returns the resulting row (if any) plus observation meta.
    """
    if slot not in (SLOT_MIDDAY, SLOT_EVENING):
        raise ValueError(f"slot must be {SLOT_MIDDAY} or {SLOT_EVENING}, got {slot!r}")
    deleted = 0
    if force:
        deleted = delete_summary_slot_row(briefing_day, slot)
    run_slot(slot, briefing_day)
    row = fetch_summary_row(briefing_day, slot)
    return {
        "slot": slot,
        "briefing_date_ist": briefing_day.isoformat(),
        "force_deleted_prior_row": deleted,
        "row": row,
    }
