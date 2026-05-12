"""
IST UI digest summaries over ui_data windows (midday / evening slots).
Uses naive IST datetimes for windows, consistent with the rest of the backend.
Slot keys stored in DB are ``12.30`` and ``19.30`` (the scheduled run times).

Windowing is cyclical: if the *previous* slot in the chain completed successfully,
only the incremental slice is considered; if it skipped/failed or is missing,
a 24-hour catch-up window between the same slot times is used. Each run still
requires at least UI_SUMMARY_MIN_OBSERVATIONS (default 4) ui_data rows or it
records skipped_low_volume.

``summary_ui_data.created_at`` is written explicitly in IST (timezone-aware)
so TIMESTAMPTZ reflects the correct instant for India.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional

from database import get_conn

logger = logging.getLogger("uvicorn.error")

# IST wall-clock labels for the two digest runs (stored in DB + whatsapp_broadcast.slot).
IST = timezone(timedelta(hours=5, minutes=30))
SLOT_MIDDAY = "12.30"
SLOT_EVENING = "19.30"

# Accept legacy admin/API query strings and normalize.
_LEGACY_SLOT_ALIASES = {"12_30": SLOT_MIDDAY, "19_30": SLOT_EVENING}


def normalize_ui_summary_slot_label(slot: str) -> str:
    s = (slot or "").strip()
    return _LEGACY_SLOT_ALIASES.get(s, s)

UI_SUMMARY_MIN_OBSERVATIONS = max(0, int(os.getenv("UI_SUMMARY_MIN_OBSERVATIONS", "4")))
UI_SUMMARY_MODEL = os.getenv("UI_SUMMARY_MODEL", "gpt-4.1-mini").strip() or "gpt-4.1-mini"
# Target length for one WhatsApp bubble (~900 chars comfortable; hard stay under ~1100).
UI_SUMMARY_TARGET_CHARS = max(400, min(int(os.getenv("UI_SUMMARY_TARGET_CHARS", "950")), 2500))

_DIGEST_SYSTEM = f"""You write a short \"Quick pulse\" digest for Indian equity investors. It will be sent on WhatsApp as a gentle nudge — not a full article. Users get details on the website later.

OUTPUT SHAPE (follow closely):
1) First line exactly: Quick pulse:
2) Then one bullet per company or distinct story, using the • character and this pattern:
   • *Company Name* — WhatsApp bold: wrap ONLY the company name in single asterisks (*Like This*) so it renders bold when pasted into a WhatsApp template variable. Then one flowing sentence after the em dash that tells the story AND weaves in exactly ONE salient number from the input (order value, % growth, EPS, revenue, PAT, capacity, run-rate, etc.). Pick the single most investor-meaningful number for that line; do not add a second figure on the same line unless unavoidable (prefer one).
3) After the bullets, one short closing line. Use approachable wording like: Check the website for full details and filings if these names matter to your portfolio. (Do not use the word \"skim\". Keep it plain and friendly.)
4) Plain text only aside from *bold company names*. No markdown headings beyond what is specified. No numbered lists except the • bullets.

STYLE RULES:
- Warm, readable, conversational — mini-story per line, not keyword dumps.
- Do NOT add a separate \"Overall\", \"In summary\", or wrap-up paragraph at the end (only the bullets + one closing nudge line).
- Do NOT strip numbers entirely: every bullet must include one concrete number when the source material supports it; if a row truly has no numeric fact, one qualitative anchor is OK but prefer a number from the text.
- If there are many companies (>8–10), prioritize the most material names and briefly cluster the rest (\"Several smaller names also filed updates — see site\") without inventing numbers.
- Stay roughly under {UI_SUMMARY_TARGET_CHARS} characters total (including header and closing line) so it fits one WhatsApp-style message comfortably.
- No investment advice; factual tone."""


def _naive_ist(d: date, hh: int, mm: int) -> datetime:
    return datetime.combine(d, time(hh, mm, 0))


def _summary_slot_status(slot: str, briefing_day: date) -> Optional[str]:
    """Terminal status for this IST calendar day + slot, or None if no row."""
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
            return r[0] if r else None


def _previous_evening_completed_for_midday(briefing_day: date) -> bool:
    """True iff yesterday's 19:30 slot finished with status completed."""
    prev_day = briefing_day - timedelta(days=1)
    return _summary_slot_status(SLOT_EVENING, prev_day) == "completed"


def _resolve_midday_window(briefing_day: date) -> tuple[datetime, datetime, bool, str]:
    """
    Cyclical window for D 12:30:
    - If previous calendar day's evening slot **completed** → narrow slice only:
      (D-1 19:30, D 12:30] IST (news after last evening digest through midday).
    - Otherwise (skipped/failed/missing) → 24h catch-up between midday anchors:
      (D-1 12:30, D 12:30] IST.

    Returns (window_start, window_end, start_exclusive, label_for_logs).
    """
    if _previous_evening_completed_for_midday(briefing_day):
        w_start = _naive_ist(briefing_day - timedelta(days=1), 19, 30)
        w_end = _naive_ist(briefing_day, 12, 30)
        return w_start, w_end, True, "narrow_after_evening_completed"
    w_start = _naive_ist(briefing_day - timedelta(days=1), 12, 30)
    w_end = _naive_ist(briefing_day, 12, 30)
    return w_start, w_end, True, "wide_catchup_24h_midday"


def _resolve_evening_window(briefing_day: date) -> tuple[datetime, datetime, bool, str]:
    """
    Cyclical window for D 19:30:
    - If today's midday **completed** → narrow (12:30, 19:30] same day.
    - Otherwise → 24h catch-up between evening anchors: (D-1 19:30, D 19:30] IST.

    Returns (window_start, window_end, start_exclusive, label_for_logs).
    """
    if _summary_slot_status(SLOT_MIDDAY, briefing_day) == "completed":
        w_start = _naive_ist(briefing_day, 12, 30)
        w_end = _naive_ist(briefing_day, 19, 30)
        return w_start, w_end, True, "narrow_after_midday_completed"
    w_start = _naive_ist(briefing_day - timedelta(days=1), 19, 30)
    w_end = _naive_ist(briefing_day, 19, 30)
    return w_start, w_end, True, "wide_catchup_24h_evening"


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
) -> Optional[int]:
    """
    INSERT INTO summary_ui_data ON CONFLICT (briefing_date_ist, slot) DO NOTHING.
    Returns the new row id, or None if the insert was skipped (conflict).
    ``created_at`` is written explicitly in IST so TIMESTAMPTZ reflects the correct instant.
    """
    created_at_ist = datetime.now(IST)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO summary_ui_data (
                    slot, briefing_date_ist, window_start_ist, window_end_ist,
                    observation_count, status, summary_text, model, error_message, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    created_at_ist,
                ),
            )
            row = cur.fetchone()
            return int(row[0]) if row else None


def _maybe_publish_quick_pulse_whatsapp(
    summary_ui_data_id: int,
    slot: str,
    briefing_day: date,
    digest: str,
) -> None:
    """completed summaries → whatsapp_broadcast + Quick pulse template (best-effort)."""
    try:
        from service.notification_service import NotificationService

        out = NotificationService().publish_quick_pulse_digest_after_summary(
            summary_ui_data_id, slot, briefing_day, digest
        )
        logger.info(
            "ui_summary WhatsApp digest publish summary_ui_data_id=%s out=%s",
            summary_ui_data_id,
            out,
        )
    except Exception:
        logger.exception(
            "ui_summary WhatsApp digest publish failed summary_ui_data_id=%s",
            summary_ui_data_id,
        )


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
        "Using ONLY the facts below (do not invent companies or numbers), produce the Quick pulse digest.\n"
        "Remember: first line 'Quick pulse:', then • bullets with *Company Name* in WhatsApp bold asterisks, "
        "each line one story + one key number where possible, "
        "then one closing line (Check the website for full details and filings… — not 'skim'). "
        f"Aim ~{UI_SUMMARY_TARGET_CHARS} characters total.\n\n"
        f"{observation_text}"
    )
    resp = client.chat.completions.create(
        model=UI_SUMMARY_MODEL,
        temperature=0.35,
        messages=[
            {"role": "system", "content": _DIGEST_SYSTEM},
            {"role": "user", "content": user_msg},
        ],
        max_tokens=900,
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

    w_start, w_end, start_exc, win_mode = _resolve_midday_window(briefing_day)
    rows = fetch_ui_rows_in_window(
        w_start, w_end, start_exclusive=start_exc, end_inclusive=True
    )
    n = len(rows)
    logger.info(
        "ui_summary midday: window_mode=%s obs=%s window=[%s .. %s]",
        win_mode,
        n,
        w_start.isoformat(),
        w_end.isoformat(),
    )
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
        ) is not None:
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
            _maybe_publish_quick_pulse_whatsapp(inserted, slot, briefing_day, digest)
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
        ) is not None:
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

    w_start, w_end, start_exc, win_mode = _resolve_evening_window(briefing_day)
    rows = fetch_ui_rows_in_window(
        w_start, w_end, start_exclusive=start_exc, end_inclusive=True
    )

    n = len(rows)
    logger.info(
        "ui_summary evening: window_mode=%s obs=%s window=[%s .. %s]",
        win_mode,
        n,
        w_start.isoformat(),
        w_end.isoformat(),
    )
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
        ) is not None:
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
            _maybe_publish_quick_pulse_whatsapp(inserted, slot, briefing_day, digest)
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
        ) is not None:
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
    slot = normalize_ui_summary_slot_label(slot)
    if slot not in (SLOT_MIDDAY, SLOT_EVENING):
        raise ValueError(
            f"slot must be {SLOT_MIDDAY} or {SLOT_EVENING} (legacy 12_30 / 19_30 accepted), got {slot!r}"
        )
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
