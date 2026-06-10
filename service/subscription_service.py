# service/subscription_service.py – subscription state & paid-user checks
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from psycopg2.extras import Json

from database import get_conn

logger = logging.getLogger("uvicorn.error")

PLAN_ID = os.getenv("SUBSCRIPTION_PLAN_ID", "monthly_199")
AMOUNT_PAISE = int(os.getenv("SUBSCRIPTION_AMOUNT_PAISE", "19900"))
PERIOD_DAYS = int(os.getenv("SUBSCRIPTION_PERIOD_DAYS", "30"))

# SQL fragment: user u must be joined; checks paid access at query time.
PAID_USER_EXISTS_SQL = """
    EXISTS (
        SELECT 1 FROM user_subscriptions s
        WHERE s.user_id = u.id
          AND s.current_period_end IS NOT NULL
          AND s.current_period_end > NOW()
    )
"""


class SubscriptionService:
    """Manages subscription rows and paid-user filtering."""

    @staticmethod
    def ensure_row(user_id: int) -> None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO user_subscriptions (user_id, status)
                    VALUES (%s, 'inactive')
                    ON CONFLICT (user_id) DO NOTHING
                    """,
                    (user_id,),
                )

    @staticmethod
    def get_subscription(user_id: int) -> Dict[str, Any]:
        SubscriptionService.ensure_row(user_id)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT user_id, status, plan_id, current_period_start, current_period_end
                    FROM user_subscriptions
                    WHERE user_id = %s
                    """,
                    (user_id,),
                )
                row = cur.fetchone()
        if not row:
            return {
                "status": "inactive",
                "plan_id": PLAN_ID,
                "current_period_start": None,
                "current_period_end": None,
                "is_paid": False,
                "amount_inr": AMOUNT_PAISE // 100,
                "amount_paise": AMOUNT_PAISE,
                "period_days": PERIOD_DAYS,
            }
        period_end = row[4]
        is_paid = period_end is not None and period_end > datetime.now(timezone.utc)
        status = row[1]
        if status == "active" and not is_paid:
            status = "expired"
        return {
            "status": status,
            "plan_id": row[2],
            "current_period_start": row[3].isoformat() if row[3] else None,
            "current_period_end": period_end.isoformat() if period_end else None,
            "is_paid": is_paid,
            "amount_inr": AMOUNT_PAISE // 100,
            "amount_paise": AMOUNT_PAISE,
            "period_days": PERIOD_DAYS,
        }

    @staticmethod
    def is_user_paid(user_id: int) -> bool:
        return SubscriptionService.get_subscription(user_id)["is_paid"]

    @staticmethod
    def filter_paid_phones(phones: List[str]) -> List[str]:
        if not phones:
            return []
        normalized = [str(p).strip() for p in phones if p and str(p).strip()]
        if not normalized:
            return []
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT u.phone
                    FROM users u
                    JOIN user_subscriptions s ON s.user_id = u.id
                    WHERE u.phone = ANY(%s)
                      AND s.current_period_end IS NOT NULL
                      AND s.current_period_end > NOW()
                    """,
                    (normalized,),
                )
                paid = {row[0] for row in cur.fetchall()}
        return [p for p in normalized if p in paid]

    @staticmethod
    def activate_from_payment(
        user_id: int,
        *,
        razorpay_order_id: str,
        razorpay_payment_id: str,
        paid_at: datetime,
        raw_payload: Optional[dict] = None,
    ) -> Dict[str, Any]:
        """
        Extend subscription by PERIOD_DAYS from payment time.
        If already active, stack from max(now, current_period_end).
        """
        SubscriptionService.ensure_row(user_id)
        if paid_at.tzinfo is None:
            paid_at = paid_at.replace(tzinfo=timezone.utc)

        with get_conn() as conn:
            with conn.cursor() as cur:
                # Idempotent: already processed this payment
                cur.execute(
                    """
                    SELECT id, status FROM payment_transactions
                    WHERE razorpay_payment_id = %s AND status = 'captured'
                    """,
                    (razorpay_payment_id,),
                )
                existing = cur.fetchone()
                if existing:
                    logger.info(
                        "Payment %s already captured — skipping duplicate activation",
                        razorpay_payment_id,
                    )
                    return SubscriptionService.get_subscription(user_id)

                cur.execute(
                    """
                    SELECT current_period_end FROM user_subscriptions WHERE user_id = %s
                    """,
                    (user_id,),
                )
                sub_row = cur.fetchone()
                current_end = sub_row[0] if sub_row else None
                if current_end and current_end.tzinfo is None:
                    current_end = current_end.replace(tzinfo=timezone.utc)

                now = datetime.now(timezone.utc)
                stacking = current_end is not None and current_end > now
                if stacking:
                    period_end = current_end + timedelta(days=PERIOD_DAYS)
                else:
                    period_end = paid_at + timedelta(days=PERIOD_DAYS)

                if stacking:
                    cur.execute(
                        """
                        UPDATE user_subscriptions
                        SET status = 'active',
                            plan_id = %s,
                            current_period_end = %s,
                            updated_at = NOW()
                        WHERE user_id = %s
                        """,
                        (PLAN_ID, period_end, user_id),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE user_subscriptions
                        SET status = 'active',
                            plan_id = %s,
                            current_period_start = %s,
                            current_period_end = %s,
                            updated_at = NOW()
                        WHERE user_id = %s
                        """,
                        (PLAN_ID, paid_at, period_end, user_id),
                    )

                cur.execute(
                    """
                    UPDATE payment_transactions
                    SET razorpay_payment_id = %s,
                        status = 'captured',
                        paid_at = %s,
                        period_start = %s,
                        period_end = %s,
                        raw_payload = %s,
                        updated_at = NOW()
                    WHERE razorpay_order_id = %s AND user_id = %s
                    """,
                    (
                        razorpay_payment_id,
                        paid_at,
                        paid_at,
                        period_end,
                        Json(raw_payload) if raw_payload else None,
                        razorpay_order_id,
                        user_id,
                    ),
                )
                if cur.rowcount == 0:
                    cur.execute(
                        """
                        INSERT INTO payment_transactions (
                            user_id, razorpay_order_id, razorpay_payment_id,
                            amount_paise, currency, status, paid_at,
                            period_start, period_end, raw_payload
                        )
                        VALUES (%s, %s, %s, %s, 'INR', 'captured', %s, %s, %s, %s)
                        """,
                        (
                            user_id,
                            razorpay_order_id,
                            razorpay_payment_id,
                            AMOUNT_PAISE,
                            paid_at,
                            paid_at,
                            period_end,
                            Json(raw_payload) if raw_payload else None,
                        ),
                    )

        logger.info(
            "Subscription activated for user %s until %s (payment %s)",
            user_id,
            period_end.isoformat(),
            razorpay_payment_id,
        )
        return SubscriptionService.get_subscription(user_id)

    @staticmethod
    def record_order_created(user_id: int, order_id: str, amount_paise: int) -> None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO payment_transactions (
                        user_id, razorpay_order_id, amount_paise, currency, status
                    )
                    VALUES (%s, %s, %s, 'INR', 'created')
                    ON CONFLICT (razorpay_order_id) DO NOTHING
                    """,
                    (user_id, order_id, amount_paise),
                )

    @staticmethod
    def record_payment_failed(order_id: str, raw_payload: Optional[dict] = None) -> None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE payment_transactions
                    SET status = 'failed', raw_payload = %s, updated_at = NOW()
                    WHERE razorpay_order_id = %s
                    """,
                    (Json(raw_payload) if raw_payload else None, order_id),
                )
