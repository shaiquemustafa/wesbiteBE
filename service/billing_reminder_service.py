# service/billing_reminder_service.py – unpaid-user billing reminder cadence + sends
import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from psycopg2.extras import Json

from database import get_conn

logger = logging.getLogger("uvicorn.error")

IST = timezone(timedelta(hours=5, minutes=30))

BILLING_REMINDER_JOB_NAME = "billing_reminder_daily"

BILLING_REMINDER_PARAM1 = os.getenv("BILLING_REMINDER_PARAM1", "User")
BILLING_REMINDER_PARAM2_AMOUNT = os.getenv("BILLING_REMINDER_PARAM2_AMOUNT", "199")
BILLING_REMINDER_PARAM3_LINK = os.getenv("BILLING_REMINDER_PARAM3_LINK", "rito-billing")


def _today_ist() -> date:
    return datetime.now(IST).date()


def _to_ist_date(dt: Optional[datetime]) -> Optional[date]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(IST).date()


def cadence_interval_days(days_since_expiry: int) -> Optional[int]:
    """
    Target spacing between reminders based on days since subscription expired.
    None = stop sending.
    """
    if days_since_expiry < 0:
        return None
    if days_since_expiry <= 14:
        return 1
    if days_since_expiry <= 30:
        return 2
    if days_since_expiry <= 90:
        return 7
    if days_since_expiry <= 120:
        return 15
    return None


def cadence_phase_label(days_since_expiry: int) -> str:
    if days_since_expiry < 0:
        return "active"
    if days_since_expiry <= 14:
        return "daily"
    if days_since_expiry <= 30:
        return "every_2_days"
    if days_since_expiry <= 90:
        return "weekly"
    if days_since_expiry <= 120:
        return "twice_monthly"
    return "stopped"


def should_send_billing_reminder_today(
    days_since_expiry: int,
    last_sent_date: Optional[date],
    today: Optional[date] = None,
) -> Tuple[bool, Optional[int], str]:
    """Returns (should_send, interval_days, phase_label)."""
    today = today or _today_ist()
    interval = cadence_interval_days(days_since_expiry)
    phase = cadence_phase_label(days_since_expiry)
    if interval is None:
        return False, None, phase
    if last_sent_date == today:
        return False, interval, phase
    if last_sent_date is None:
        return True, interval, phase
    days_since_last = (today - last_sent_date).days
    return days_since_last >= interval, interval, phase


def _expiry_anchor_date(
    current_period_end: Optional[datetime],
    user_created_at: Optional[datetime],
) -> Optional[date]:
    if current_period_end is not None:
        return _to_ist_date(current_period_end)
    if user_created_at is not None:
        return _to_ist_date(user_created_at)
    return None


def is_user_paid(current_period_end: Optional[datetime]) -> bool:
    if current_period_end is None:
        return False
    end = current_period_end
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    return end > datetime.now(timezone.utc)


def fetch_unpaid_users(user_ids: Optional[List[int]] = None) -> List[Dict[str, Any]]:
    """Unpaid active users with phone numbers."""
    rows: List[Dict[str, Any]] = []
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                sql = """
                    SELECT u.id,
                           u.phone,
                           u.name,
                           u.created_at,
                           s.current_period_end,
                           s.last_billing_reminder_at
                    FROM users u
                    JOIN user_subscriptions s ON s.user_id = u.id
                    WHERE u.is_active = TRUE
                      AND u.phone IS NOT NULL
                      AND TRIM(u.phone) <> ''
                      AND NOT (
                        s.current_period_end IS NOT NULL
                        AND s.current_period_end > NOW()
                      )
                """
                params: tuple = ()
                if user_ids:
                    sql += " AND u.id = ANY(%s)"
                    params = (user_ids,)
                cur.execute(sql, params)
                for row in cur.fetchall():
                    rows.append(
                        {
                            "user_id": row[0],
                            "phone": row[1],
                            "name": row[2],
                            "created_at": row[3],
                            "current_period_end": row[4],
                            "last_billing_reminder_at": row[5],
                        }
                    )
    except Exception as e:
        logger.error("Failed to fetch unpaid users for billing reminder: %s", e)
    return rows


def evaluate_user_for_send(
    user_row: Dict[str, Any],
    today: Optional[date] = None,
    force: bool = False,
) -> Dict[str, Any]:
    today = today or _today_ist()
    anchor = _expiry_anchor_date(
        user_row.get("current_period_end"),
        user_row.get("created_at"),
    )
    if anchor is None:
        return {
            "user_id": user_row["user_id"],
            "should_send": False,
            "reason": "no_expiry_anchor",
        }

    days_since_expiry = (today - anchor).days
    last_sent = _to_ist_date(user_row.get("last_billing_reminder_at"))
    should_send, interval, phase = should_send_billing_reminder_today(
        days_since_expiry, last_sent, today
    )
    if force:
        should_send = True

    return {
        "user_id": user_row["user_id"],
        "phone": user_row["phone"],
        "should_send": should_send,
        "days_since_expiry": days_since_expiry,
        "cadence_phase": phase,
        "cadence_interval_days": interval,
        "last_sent_date": last_sent.isoformat() if last_sent else None,
        "expiry_anchor": anchor.isoformat(),
        "reason": "due" if should_send else "not_due",
    }


def record_billing_reminder_send(
    user_id: int,
    phone: str,
    *,
    template_id: str,
    days_since_expiry: int,
    cadence_phase: str,
    status: str,
    raw_payload: Optional[dict] = None,
) -> None:
    now = datetime.now(timezone.utc)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO billing_reminder_log (
                    user_id, phone, template_id, days_since_expiry,
                    cadence_phase, status, raw_payload
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    user_id,
                    phone,
                    template_id,
                    days_since_expiry,
                    cadence_phase,
                    status,
                    Json(raw_payload) if raw_payload else None,
                ),
            )
            if status == "sent":
                cur.execute(
                    """
                    UPDATE user_subscriptions
                    SET last_billing_reminder_at = %s, updated_at = NOW()
                    WHERE user_id = %s
                    """,
                    (now, user_id),
                )


def already_ran_daily_job_today_ist() -> bool:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT last_run_at FROM scheduled_jobs WHERE job_name = %s
                    """,
                    (BILLING_REMINDER_JOB_NAME,),
                )
                row = cur.fetchone()
                if not row or not row[0]:
                    return False
                last = row[0]
                if last.tzinfo is None:
                    last = last.replace(tzinfo=timezone.utc)
                return last.astimezone(IST).date() == _today_ist()
    except Exception as e:
        logger.warning("billing_reminder already_ran check failed: %s", e)
    return False


def stamp_daily_job_run(status: str, meta: Optional[dict] = None) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO scheduled_jobs (job_name, last_run_at, last_status, last_meta, updated_at)
                VALUES (%s, NOW(), %s, %s, NOW())
                ON CONFLICT (job_name) DO UPDATE SET
                    last_run_at = NOW(),
                    last_status = EXCLUDED.last_status,
                    last_meta = EXCLUDED.last_meta,
                    updated_at = NOW()
                """,
                (BILLING_REMINDER_JOB_NAME, status, Json(meta) if meta else None),
            )


def run_billing_reminder_broadcast(
    *,
    user_ids: Optional[List[int]] = None,
    force: bool = False,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Evaluate unpaid users and send billing reminder template where due.
    """
    from service.whatsapp_service import WhatsAppService, BILLING_REMINDER_TEMPLATE_ID

    users = fetch_unpaid_users(user_ids=user_ids)
    evaluations = [evaluate_user_for_send(u, force=force) for u in users]
    due = [e for e in evaluations if e.get("should_send")]

    if dry_run:
        return {
            "dry_run": True,
            "total_unpaid": len(users),
            "due_count": len(due),
            "evaluations": evaluations,
        }

    if not due:
        return {
            "sent": 0,
            "failed": 0,
            "total_unpaid": len(users),
            "due_count": 0,
            "evaluations": evaluations,
        }

    svc = WhatsAppService()
    sent = 0
    failed = 0
    results: List[Dict[str, Any]] = []

    for ev in due:
        phone = str(ev["phone"]).strip()
        user_id = ev["user_id"]
        template_obj = {
            "id": BILLING_REMINDER_TEMPLATE_ID,
            "params": [
                BILLING_REMINDER_PARAM1,
                BILLING_REMINDER_PARAM2_AMOUNT,
                BILLING_REMINDER_PARAM3_LINK,
            ],
        }
        ok = svc.send_billing_reminder_message(phone, template_obj)
        status = "sent" if ok else "failed"
        if ok:
            sent += 1
        else:
            failed += 1
        record_billing_reminder_send(
            user_id,
            phone,
            template_id=BILLING_REMINDER_TEMPLATE_ID,
            days_since_expiry=ev.get("days_since_expiry") or 0,
            cadence_phase=ev.get("cadence_phase") or "unknown",
            status=status,
            raw_payload={"evaluation": ev},
        )
        results.append({"user_id": user_id, "phone": phone, "status": status})

    summary = {
        "sent": sent,
        "failed": failed,
        "total_unpaid": len(users),
        "due_count": len(due),
        "results": results,
        "evaluations": evaluations,
    }
    logger.info(
        "Billing reminder broadcast: %d sent, %d failed (unpaid=%d due=%d)",
        sent,
        failed,
        len(users),
        len(due),
    )
    return summary


def send_billing_reminder_to_user(user_id: int, *, force: bool = True) -> Dict[str, Any]:
    """Send billing reminder to a single user (admin / test)."""
    users = fetch_unpaid_users(user_ids=[user_id])
    if not users:
        paid_check = None
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT current_period_end FROM user_subscriptions WHERE user_id = %s",
                    (user_id,),
                )
                row = cur.fetchone()
                if row:
                    paid_check = is_user_paid(row[0])
        if paid_check:
            return {
                "success": False,
                "message": "User has an active subscription — reminder not sent.",
            }
        return {"success": False, "message": "User not found or has no phone."}

    return run_billing_reminder_broadcast(user_ids=[user_id], force=force, dry_run=False)
