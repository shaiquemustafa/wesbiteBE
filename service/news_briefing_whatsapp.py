"""
WhatsApp watchlist notifications for `news_briefing_stock_news` rows.

After a briefing run is ingested, each stock-news row is matched to users who
have that BSE scrip on `user_watchlist`. Uses ``NEWS_BRIEFING_WATCHLIST_TEMPLATE_ID``
(Gupshup body variables):

  1) literal "User"     2) 📊 *company* (bold)   3) literal "General news"
  4) AI summary         5) published time (IST)   6) article / PDF link

Env:
  NEWS_BRIEFING_WHATSAPP_ENABLED — optional. Sends run by default after each ingest
  for users who watch the stock. Set to 0/false/no/off to disable (e.g. staging).
"""
from __future__ import annotations

import os
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from database import get_conn
from service.whatsapp_service import (
    NEWS_BRIEFING_WATCHLIST_TEMPLATE_ID,
    RITO_WEBSITE_URL,
    WhatsAppService,
)

logger = logging.getLogger("uvicorn.error")

# Meta/Gupshup variable practical limits (approved template body vars)
_MAX_P1, _MAX_P2, _MAX_P3, _MAX_P4, _MAX_P5, _MAX_P6 = 80, 220, 120, 1000, 80, 1024


def briefing_whatsapp_enabled() -> bool:
    """
    Briefing → watchlist WhatsApp is on by default (matches product: watchlist = notify).

    Opt out only: set NEWS_BRIEFING_WHATSAPP_ENABLED to 0, false, no, or off.
    """
    raw = os.getenv("NEWS_BRIEFING_WHATSAPP_ENABLED")
    if raw is None or not str(raw).strip():
        return True
    s = str(raw).strip().lower()
    if s in ("0", "false", "no", "off"):
        return False
    return True


def _parse_one_bse_scrip_token(token: str) -> Optional[int]:
    token = (token or "").strip()
    if not token:
        return None
    try:
        if "." in token or "e" in token.lower():
            v = int(float(token))
        else:
            v = int(token)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def parse_all_bse_scrips(raw: Any) -> List[int]:
    """
    All positive BSE scrip codes from a cell: comma, semicolon, pipe, or ' and '.
    Handles Excel floats like 532540.0 per token.
    """
    if raw is None:
        return []
    s = str(raw).strip()
    if not s:
        return []
    for sep in ";", "|":
        s = s.replace(sep, ",")
    parts = re.split(r"\s*,\s*|\s+and\s+", s, flags=re.I)
    seen: set[int] = set()
    out: List[int] = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        n = _parse_one_bse_scrip_token(p)
        if n is not None and n not in seen:
            seen.add(n)
            out.append(n)
    return out


def parse_bse_scrip(raw: Any) -> Optional[int]:
    """First parseable BSE scrip from a cell (backward compatible)."""
    allv = parse_all_bse_scrips(raw)
    return allv[0] if allv else None


def _clamp(s: str, n: int) -> str:
    s = s.strip() if s else ""
    if len(s) <= n:
        return s
    return s[: n - 1] + "…"


def _default_article_link() -> str:
    base = (RITO_WEBSITE_URL or "rito.co.in").strip().rstrip("/")
    if base.startswith("http://") or base.startswith("https://"):
        return base
    return f"https://{base}"


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
    link: Optional[str],
    news_time: Optional[Any] = None,
) -> List[str]:
    """Six Gupshup body variables for NEWS_BRIEFING_WATCHLIST_TEMPLATE_ID."""
    p1 = _clamp("User", _MAX_P1)
    raw_company = (company_display or "").strip() or "Stock"
    # WhatsApp bold: *text*; chart emoji for visibility (variable 2 sits in template body).
    safe_name = raw_company.replace("*", "")
    p2 = _clamp(f"📊 *{safe_name}*", _MAX_P2)
    p3 = _clamp("General news", _MAX_P3)
    p4 = _clamp((ai_summary or "").strip() or "No summary available.", _MAX_P4)
    # Variable 5: formatted publish time in IST (e.g. "May 24, 2:30 PM")
    if news_time:
        try:
            if hasattr(news_time, "strftime"):
                p5 = news_time.strftime("%-d %b, %-I:%M %p IST")
            else:
                from datetime import datetime as _dt
                p5 = _dt.strptime(str(news_time), "%Y-%m-%d %H:%M:%S").strftime("%-d %b, %-I:%M %p IST")
        except Exception:
            p5 = str(news_time)[:_MAX_P5]
    else:
        p5 = ""
    p5 = _clamp(p5, _MAX_P5)
    # Variable 6: article / BSE PDF link
    url = (link or "").strip()
    if not url:
        url = _default_article_link()
    p6 = _clamp(url, _MAX_P6)
    return [p1, p2, p3, p4, p5, p6]


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
    For each news_briefing_stock_news row in this run, notify users watching any
    BSE scrip listed in that row (comma / semicolon / ' and '). One WhatsApp per
    phone per row: company line lists each watched scrip's label for that user.
    """
    if not briefing_whatsapp_enabled():
        return {"skipped": True, "reason": "NEWS_BRIEFING_WHATSAPP_ENABLED=off"}

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
        scrips = parse_all_bse_scrips(row.get("bse_scrip_code"))
        if not scrips:
            rows_no_scrip += 1
            continue

        phone_to_scrips: Dict[str, List[int]] = {}
        for scrip in scrips:
            for phone, _name in fetch_watchers_for_scrip(scrip):
                phone_to_scrips.setdefault(phone, []).append(scrip)

        if not phone_to_scrips:
            rows_no_watchers += 1
            continue

        base_title = ((row.get("title") or "").strip() or "")[:80]
        for phone, scrip_list in phone_to_scrips.items():
            uniq = list(dict.fromkeys(scrip_list))
            if len(uniq) == 1:
                company = _company_label(uniq[0], row)
            else:
                labels = [_company_label(s, row) for s in uniq]
                company = " / ".join(dict.fromkeys(labels))[:220] or "Stock update"
            title_short = base_title if base_title else (company or "Stock update")[:80]
            params = build_briefing_watchlist_params(
                company,
                row.get("ai_summary"),
                row.get("link"),
                row.get("published_at_ist"),
            )
            tpl = {"id": NEWS_BRIEFING_WATCHLIST_TEMPLATE_ID, "params": params}
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
                if bse_scrip in parse_all_bse_scrips(row.get("bse_scrip_code")):
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

    scrips = parse_all_bse_scrips(row.get("bse_scrip_code"))
    if not scrips:
        return {"ok": False, "error": "row has no parseable bse_scrip_code"}

    if bse_scrip is not None and int(bse_scrip) not in scrips:
        return {"ok": False, "error": f"bse_scrip {bse_scrip} not in row codes {scrips}"}

    primary = int(bse_scrip) if bse_scrip is not None else scrips[0]
    company = _company_label(primary, row)

    params = build_briefing_watchlist_params(
        company,
        row.get("ai_summary"),
        row.get("link"),
        row.get("published_at_ist"),
    )
    tpl = {"id": NEWS_BRIEFING_WATCHLIST_TEMPLATE_ID, "params": params}
    ws = WhatsAppService()
    title = (row.get("title") or company)[:120]
    batch = ws.send_template_batch([(phone, tpl, title)])
    ok = batch.get("sent", 0) >= 1 and batch.get("failed", 1) == 0
    out: Dict[str, Any] = {
        "ok": ok,
        "phone": phone,
        "stock_news_id": row.get("id"),
        "run_id": row.get("run_id"),
        "bse_scrip_code": primary,
        "bse_scrip_codes": scrips,
        "template_id": NEWS_BRIEFING_WATCHLIST_TEMPLATE_ID,
        "params_preview": params,
    }
    if not ok:
        out["error"] = "Gupshup send failed"
        out["batch"] = batch
    return out
