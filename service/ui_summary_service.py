"""
IST UI digest summaries — single daily run at 14:30 IST.
Slot key stored in DB: ``14.30``.

Window: previous day 14:30 → today 14:30 IST (exclusive start, inclusive end),
covering a clean 24-hour trading day.

Uses all non-neutral ui_data rows in the window (ui_data already excludes
NEUTRAL / N/A / MATCHED at ingest). Optional market cap floor: UI_SUMMARY_MIN_MARKET_CAP_CR.

Minimum observations: 1 (runs whenever at least one qualifying stock exists).
Hard cap on digest text: 800 characters to stay safe under WhatsApp template limits.

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

IST = timezone(timedelta(hours=5, minutes=30))

# Single daily digest slot at 14:30 IST.
SLOT_DAILY = "14.30"
# Keep legacy names so existing imports / admin API still work without a migration.
SLOT_MIDDAY = SLOT_DAILY
SLOT_EVENING = SLOT_DAILY

_LEGACY_SLOT_ALIASES: Dict[str, str] = {
    "12_30": SLOT_DAILY,
    "19_30": SLOT_DAILY,
    "12.30": SLOT_DAILY,
    "19.30": SLOT_DAILY,
    "14_30": SLOT_DAILY,
}


def normalize_ui_summary_slot_label(slot: str) -> str:
    s = (slot or "").strip()
    return _LEGACY_SLOT_ALIASES.get(s, s)

UI_SUMMARY_MIN_OBSERVATIONS = 1   # run whenever ≥1 qualifying stock exists
UI_SUMMARY_MODEL = os.getenv("UI_SUMMARY_MODEL", "gpt-4.1-mini").strip() or "gpt-4.1-mini"
# LLM target length: aim well under the 800-char hard send cap.
UI_SUMMARY_TARGET_CHARS = max(400, min(int(os.getenv("UI_SUMMARY_TARGET_CHARS", "750")), 2500))
# Market cap threshold for digest inclusion (Crores).
UI_SUMMARY_MIN_MARKET_CAP_CR: float = float(os.getenv("UI_SUMMARY_MIN_MARKET_CAP_CR", "2500"))

_DIGEST_SYSTEM = """You write a WhatsApp market digest for Indian equity investors.

OUTPUT FORMAT (follow exactly):
- Output ONE continuous line of text with NO line breaks anywhere.
- For each company use this pattern:
  📊 *COMPANY NAME IN CAPS* - [one sharp sentence with one key number]
- Separate each company entry with: ⚡
- Do NOT add any header, footer, closing line, or "Quick pulse:" label.
- Do NOT use bullet points (•), hyphens as bullets, or any line breaks.
- End with exactly: Check website for full details.

COMPANY NAME RULES:
- Always UPPERCASE the company name.
- Always wrap it in single asterisks: *COMPANY NAME* (WhatsApp bold).
- Shorten long names where obvious (e.g. "Shaily Engineering Plastics Limited" → "SHAILY ENGINEERING").

DESCRIPTION RULES:
- Each entry including 📊, company name, spaces and description must be ~91 characters total.
- Use Rs instead of ₹ symbol.
- Include exactly ONE key number per entry (revenue %, PAT %, EPS, GRM, margins etc.).
- Plain factual tone. No investment advice.

EXAMPLE OUTPUT:
📊 *ZYDUS WELLNESS* - Q4 income up 63%, swung to Rs 162 Cr net profit from a prior loss. ⚡ 📊 *BHARAT PETROLEUM* - FY26 PAT jumped 75% to Rs 23,303 Cr. GRM nearly doubled. Check website for full details."""


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


def _resolve_daily_window(briefing_day: date) -> tuple[datetime, datetime]:
    """24-hour window ending at today 14:30 IST (previous day 14:30 exclusive → today 14:30 inclusive)."""
    w_start = _naive_ist(briefing_day - timedelta(days=1), 14, 30)
    w_end = _naive_ist(briefing_day, 14, 30)
    return w_start, w_end


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


def _impact_upper_from_ui_row(row: Dict[str, Any]) -> str:
    v = row.get("impact") or row.get("Impact") or ""
    return str(v).strip().upper()


def row_is_high_signal_for_ui_digest(row: Dict[str, Any]) -> bool:
    """
    Quick pulse digest: include any row already stored in ui_data (non-low-impact
    at ingest). Exclude only immaterial / neutral-style impacts.

    Includes: STRONGLY POSITIVE, BEAT, STRONGLY NEGATIVE, POSITIVE, NEGATIVE,
    MISSED, and similar directional tags.
    Excludes: NEUTRAL, N/A, MATCHED, IMMATERIAL (and variants like LIKELY NEUTRAL).
    """
    imp = _impact_upper_from_ui_row(row)
    if not imp:
        return False
    if imp in ("N/A", "MATCHED", "NEUTRAL", "IMMATERIAL"):
        return False
    if "IMMATERIAL" in imp or "N/A" in imp or "MATCHED" in imp:
        return False
    # e.g. LIKELY NEUTRAL, MOSTLY NEUTRAL — but not STRONGLY NEGATIVE
    if "NEUTRAL" in imp and "STRONGLY NEGATIVE" not in imp:
        return False
    return True


def _market_cap_for_row(row: Dict[str, Any]) -> Optional[float]:
    for key in ("marketCap", "market_cap", "mkt_cap_cr", "market_cap_cr"):
        v = row.get(key)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    return None


def filter_ui_rows_for_digest(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """High-signal rows (impact filter) AND market cap ≥ UI_SUMMARY_MIN_MARKET_CAP_CR.
    Rows with no market cap data are included (unknown cap = not excluded)."""
    out = []
    for r in rows:
        if not row_is_high_signal_for_ui_digest(r):
            continue
        cap = _market_cap_for_row(r)
        if cap is not None and cap < UI_SUMMARY_MIN_MARKET_CAP_CR:
            continue
        out.append(r)
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
        "Using ONLY the facts below (do not invent companies or numbers), produce the digest.\n"
        "Output ONE continuous line. Each entry: 📊 *COMPANY NAME IN CAPS* - one sentence with one key number. "
        "Separate entries with ⚡. End with: Check website for full details. No header, no line breaks, no bullets, use Rs not ₹. "
        "Each entry ~91 characters including emoji and company name.\n\n"
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


def run_daily_summary(briefing_day: date) -> None:
    """Single daily digest at 14:30 IST. Window: previous day 14:30 → today 14:30."""
    slot = SLOT_DAILY
    if summary_already_done(slot, briefing_day):
        logger.info("ui_summary daily: already completed for %s", briefing_day)
        return
    if _should_skip_existing_slot(slot, briefing_day):
        logger.info("ui_summary daily: existing terminal row for %s — skip", briefing_day)
        return
    _delete_failed_slot_row(slot, briefing_day)

    w_start, w_end = _resolve_daily_window(briefing_day)
    rows_all = fetch_ui_rows_in_window(
        w_start, w_end, start_exclusive=True, end_inclusive=True
    )
    rows = filter_ui_rows_for_digest(rows_all)[-7:]  # cap at 7 most recent companies
    n = len(rows)
    logger.info(
        "ui_summary daily: raw_obs=%s digest_obs=%s window=[%s .. %s] cap_filter>=%.0fCr",
        len(rows_all),
        n,
        w_start.isoformat(),
        w_end.isoformat(),
        UI_SUMMARY_MIN_MARKET_CAP_CR,
    )
    if n == 0 and rows_all:
        logger.info(
            "ui_summary daily: digest filter excluded all %s row(s); impacts=%s",
            len(rows_all),
            [_impact_upper_from_ui_row(r) for r in rows_all],
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
                "ui_summary daily: skipped_low_volume (%s obs < %s) for %s",
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
            logger.info("ui_summary daily: completed for %s (%s obs)", briefing_day, n)
            _maybe_publish_quick_pulse_whatsapp(inserted, slot, briefing_day, digest)
        else:
            logger.info("ui_summary daily: insert skipped (conflict) for %s", briefing_day)
    except Exception as e:
        logger.exception("ui_summary daily: failed for %s: %s", briefing_day, e)
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
            logger.info("ui_summary daily: recorded failed status for %s", briefing_day)


# Aliases kept for backward compatibility (API + scheduler use run_slot / run_midday / run_evening).
def run_midday_summary(briefing_day: date) -> None:
    run_daily_summary(briefing_day)


def run_evening_summary(briefing_day: date) -> None:
    run_daily_summary(briefing_day)


def run_slot(slot: str, briefing_day: date) -> None:
    run_daily_summary(briefing_day)


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
