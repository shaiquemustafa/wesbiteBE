"""
WhatsApp watchlist notifications for `news_briefing_stock_news` rows.

After a briefing run is ingested, each stock-news row is matched to users who
have that BSE scrip on `user_watchlist`. Template uses WATCHLIST_TEMPLATE_ID
(five body variables):

  1) literal "user"         2) company line   3) literal "general"
  4) AI summary             5) time (IST) only — no link (keeps message shorter)

Env:
  NEWS_BRIEFING_WHATSAPP_ENABLED — set to 1/true/yes to send after each ingest.
"""
from __future__ import annotations

import os
import logging
import re
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

from database import get_conn
from service.whatsapp_service import WhatsAppService, WATCHLIST_TEMPLATE_ID

logger = logging.getLogger("uvicorn.error")

IST = timezone(timedelta(hours=5, minutes=30))

# Meta/Gupshup variable practical limits
_MAX_P1, _MAX_P2, _MAX_P3, _MAX_P4, _MAX_P5 = 80, 220, 120, 1000, 900


def briefing_whatsapp_enabled() -> bool:
    return os.getenv("NEWS_BRIEFING_WHATSAPP_ENABLED", "").lower() in ("1", "true", "yes")


def parse_bse_scrip(raw: Any) -> Optional[int]:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    try:
        if "." in s or "e" in s.lower():
            return int(float(s))
        return int(s)
    except (TypeError, ValueError):
        return None


def _clamp(s: str, n: int) -> str:
    s = s.strip() if s else ""
    if len(s) <= n:
        return s
    return s[: n - 1] + "…"


def _format_time_only(published_at_ist: Any) -> str:
    """published_at_ist is naive IST in DB. Variable 5 is time only (no URL)."""
    if not published_at_ist:
        return "N/A"
    try:
        if isinstance(published_at_ist, datetime):
            dt = published_at_ist
        else:
            dt = datetime.fromisoformat(str(published_at_ist)[:19])
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=IST)
        else:
            dt = dt.astimezone(IST)
        try:
            date_str = dt.strftime("%-d %b")
            time_str = dt.strftime("%-I:%M %p")
        except ValueError:
            date_str = f"{dt.day} {dt.strftime('%b')}"
            time_str = dt.strftime("%I:%M %p").lstrip("0")
        time_part = f"{date_str}, {time_str}"
        return _clamp(time_part, _MAX_P5)
    except Exception:
        return _clamp(str(published_at_ist), _MAX_P5)


def _company_label(scrip: int, row: Dict[str, Any]) -> str:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT company_name FROM company_master WHERE bse_scrip_code = %s",
                    (scrip,),
                )
                r = cur.fetchone()
                if r and r[0]:
                    return str(r[0]).strip()
    except Exception as e:
        logger.warning("company_master lookup failed scrip=%s: %s", scrip, e)
    # fallbacks from briefing row
    t = (row.get("title") or "").strip()
    aff = (row.get("affected_stocks") or "").strip()
    if aff:
        return aff.split(",")[0].strip()[:200] or "Stock update"
    if t:
        return t[:200]
    return "Stock update"


def build_briefing_watchlist_params(
    company_display: str,
    ai_summary: Optional[str],
    published_at_ist: Any,
) -> List[str]:
    """Five Gupshup body variables for the watchlist briefing template."""
    p1 = _clamp("user", _MAX_P1)
    raw_company = company_display.strip() if company_display else "Stock"
    p2 = _clamp(f"📊 *{raw_company}*", _MAX_P2)
    p3 = _clamp("general", _MAX_P3)
    p4 = _clamp((ai_summary or "").strip() or "No summary available.", _MAX_P4)
    p5 = _format_time_only(published_at_ist)
    return [p1, p2, p3, p4, p5]


def fetch_watchers_for_scrip(bse_scrip: int) -> List[Tuple[str, str]]:
    """Return [(phone, name), ...] phones normalized without '+'."""
    out: List[Tuple[str, str]] = []
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT u.phone, u.name
                    FROM user_watchlist w
                    JOIN users u ON u.id = w.user_id
                    WHERE w.bse_scrip_code = %s
                      AND (u.is_active IS NULL OR u.is_active = TRUE)
                    """,
                    (bse_scrip,),
                )
                for phone, name in cur.fetchall():
                    if not phone:
                        continue
                    p = re.sub(r"[^\d]", "", str(phone))
                    if p:
                        out.append((p, name or ""))
    except Exception as e:
        logger.warning("watchlist lookup failed scrip=%s: %s", bse_scrip, e)
    return out


def _row_dict_from_sn(
    row: Tuple[Any, ...], cols: List[str],
) -> Dict[str, Any]:
    return dict(zip(cols, row))


def notify_watchlist_for_run(run_id: int) -> Dict[str, Any]:
    """
    For each news_briefing_stock_news row in this run, notify users watching that scrip.
    """
    if not briefing_whatsapp_enabled():
        return {"skipped": True, "reason": "NEWS_BRIEFING_WHATSAPP_ENABLED not set"}

    jobs: List[Tuple[str, dict, str]] = []
    rows_seen = 0
    rows_no_scrip = 0
    rows_no_watchers = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, bse_scrip_code, nse_symbol, ai_summary, category,
                       published_at_ist, title, link, affected_stocks
                FROM news_briefing_stock_news
                WHERE run_id = %s
                ORDER BY sort_order, id
                """,
                (run_id,),
            )
            cols = [d[0] for d in cur.description]
            stock_rows = cur.fetchall()

    for tup in stock_rows:
        row = _row_dict_from_sn(tup, cols)
        rows_seen += 1
        scrip = parse_bse_scrip(row.get("bse_scrip_code"))
        if not scrip:
            rows_no_scrip += 1
            continue

        watchers = fetch_watchers_for_scrip(scrip)
        if not watchers:
            rows_no_watchers += 1
            continue

        company = _company_label(scrip, row)
        title_short = (row.get("title") or company)[:80]
        for phone, _name in watchers:
            params = build_briefing_watchlist_params(
                company,
                row.get("ai_summary"),
                row.get("published_at_ist"),
            )
            tpl = {"id": WATCHLIST_TEMPLATE_ID, "params": params}
            jobs.append((phone, tpl, title_short))

    if not jobs:
        logger.info(
            "news_briefing WhatsApp: run_id=%s rows=%s no sends (no_scrip=%s no_watchers=%s)",
            run_id, rows_seen, rows_no_scrip, rows_no_watchers,
        )
        return {
            "skipped": False,
            "run_id": run_id,
            "stock_news_rows": rows_seen,
            "jobs": 0,
            "sent": 0,
            "failed": 0,
            "rows_no_scrip": rows_no_scrip,
            "rows_no_watchers": rows_no_watchers,
        }

    ws = WhatsAppService()
    result = ws.send_template_batch(jobs)
    logger.info(
        "news_briefing WhatsApp: run_id=%s jobs=%s sent=%s failed=%s",
        run_id, result["total"], result["sent"], result["failed"],
    )
    return {
        "skipped": False,
        "run_id": run_id,
        "stock_news_rows": rows_seen,
        "jobs": result["total"],
        "sent": result["sent"],
        "failed": result["failed"],
        "rows_no_scrip": rows_no_scrip,
        "rows_no_watchers": rows_no_watchers,
    }


def load_stock_news_row_by_id(stock_news_id: int) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, run_id, bse_scrip_code, nse_symbol, ai_summary, category,
                       published_at_ist, title, link, affected_stocks
                FROM news_briefing_stock_news
                WHERE id = %s
                """,
                (stock_news_id,),
            )
            cols = [d[0] for d in cur.description]
            r = cur.fetchone()
            if not r:
                return None
            return _row_dict_from_sn(r, cols)


def find_latest_stock_news_for_scrip(bse_scrip: int) -> Optional[Dict[str, Any]]:
    """Most recent row matching the scrip (scan recent ids)."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, run_id, bse_scrip_code, nse_symbol, ai_summary, category,
                       published_at_ist, title, link, affected_stocks
                FROM news_briefing_stock_news
                ORDER BY id DESC
                LIMIT 1200
                """,
            )
            cols = [d[0] for d in cur.description]
            for tup in cur.fetchall():
                row = _row_dict_from_sn(tup, cols)
                if parse_bse_scrip(row.get("bse_scrip_code")) == bse_scrip:
                    return row
    return None


def send_test_briefing_watchlist(
    phone: str,
    *,
    stock_news_id: Optional[int] = None,
    bse_scrip: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Send one watchlist briefing template to a single phone for QA.
    Provide either stock_news_id OR bse_scrip (latest row for that scrip).
    """
    phone = re.sub(r"[^\d]", "", str(phone))
    if len(phone) < 10:
        return {"ok": False, "error": "invalid phone"}

    row: Optional[Dict[str, Any]] = None
    if stock_news_id is not None:
        row = load_stock_news_row_by_id(int(stock_news_id))
        if not row:
            return {"ok": False, "error": f"news_briefing_stock_news id={stock_news_id} not found"}
    elif bse_scrip is not None:
        row = find_latest_stock_news_for_scrip(int(bse_scrip))
        if not row:
            return {"ok": False, "error": f"No stock_news row found for bse_scrip_code={bse_scrip}"}
    else:
        return {"ok": False, "error": "provide stock_news_id or bse_scrip"}

    scrip = parse_bse_scrip(row.get("bse_scrip_code"))
    if not scrip:
        return {"ok": False, "error": "row has no parseable bse_scrip_code"}

    company = _company_label(scrip, row)

    params = build_briefing_watchlist_params(
        company,
        row.get("ai_summary"),
        row.get("published_at_ist"),
    )
    tpl = {"id": WATCHLIST_TEMPLATE_ID, "params": params}
    ws = WhatsAppService()
    title = (row.get("title") or company)[:120]
    batch = ws.send_template_batch([(phone, tpl, title)])
    ok = batch.get("sent", 0) >= 1 and batch.get("failed", 1) == 0
    out: Dict[str, Any] = {
        "ok": ok,
        "phone": phone,
        "stock_news_id": row.get("id"),
        "run_id": row.get("run_id"),
        "bse_scrip_code": scrip,
        "template_id": WATCHLIST_TEMPLATE_ID,
        "params_preview": params,
    }
    if not ok:
        out["error"] = "Gupshup send failed"
        out["batch"] = batch
    return out
