# service/payment_service.py – Razorpay Orders API integration
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict

import razorpay
from dotenv import load_dotenv

from database import get_conn
from service.subscription_service import AMOUNT_PAISE, SubscriptionService

load_dotenv()

logger = logging.getLogger("uvicorn.error")

RAZORPAY_KEY_ID = os.getenv("RAZORPAY_KEY_ID", "")
RAZORPAY_KEY_SECRET = os.getenv("RAZORPAY_KEY_SECRET", "")
RAZORPAY_WEBHOOK_SECRET = os.getenv("RAZORPAY_WEBHOOK_SECRET", "")


class PaymentService:
    """Creates Razorpay orders and verifies payments."""

    def __init__(self):
        if not RAZORPAY_KEY_ID or not RAZORPAY_KEY_SECRET:
            logger.warning("Razorpay API keys not configured.")
        self.client = razorpay.Client(auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET))

    @staticmethod
    def is_configured() -> bool:
        return bool(RAZORPAY_KEY_ID and RAZORPAY_KEY_SECRET)

    @staticmethod
    def public_config() -> Dict[str, Any]:
        return {
            "key_id": RAZORPAY_KEY_ID,
            "amount_inr": AMOUNT_PAISE // 100,
            "amount_paise": AMOUNT_PAISE,
            "currency": "INR",
            "plan_id": os.getenv("SUBSCRIPTION_PLAN_ID", "monthly_199"),
        }

    def create_order(self, user_id: int) -> Dict[str, Any]:
        if not self.is_configured():
            raise RuntimeError("Payment gateway is not configured.")

        receipt = f"rito_{user_id}_{int(datetime.now(timezone.utc).timestamp())}"
        order = self.client.order.create(
            {
                "amount": AMOUNT_PAISE,
                "currency": "INR",
                "receipt": receipt,
                "notes": {"user_id": str(user_id), "plan": "monthly_199"},
            }
        )
        order_id = order["id"]
        SubscriptionService.record_order_created(user_id, order_id, AMOUNT_PAISE)
        return {
            "order_id": order_id,
            "amount": order["amount"],
            "currency": order["currency"],
            "key_id": RAZORPAY_KEY_ID,
        }

    def verify_checkout_payment(
        self,
        user_id: int,
        order_id: str,
        payment_id: str,
        signature: str,
    ) -> Dict[str, Any]:
        if not self.is_configured():
            raise RuntimeError("Payment gateway is not configured.")

        self.client.utility.verify_payment_signature(
            {
                "razorpay_order_id": order_id,
                "razorpay_payment_id": payment_id,
                "razorpay_signature": signature,
            }
        )
        paid_at = datetime.now(timezone.utc)
        return SubscriptionService.activate_from_payment(
            user_id,
            razorpay_order_id=order_id,
            razorpay_payment_id=payment_id,
            paid_at=paid_at,
            raw_payload={
                "source": "checkout_verify",
                "order_id": order_id,
                "payment_id": payment_id,
            },
        )

    def verify_webhook_signature(self, body: bytes, signature: str) -> bool:
        if not RAZORPAY_WEBHOOK_SECRET:
            logger.error("RAZORPAY_WEBHOOK_SECRET not set — rejecting webhook.")
            return False
        body_str = body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else str(body)
        try:
            # Razorpay SDK expects str; bytes(body, 'utf-8') raises TypeError.
            self.client.utility.verify_webhook_signature(
                body_str, signature, RAZORPAY_WEBHOOK_SECRET
            )
            return True
        except razorpay.errors.SignatureVerificationError:
            return False

    def handle_webhook_event(self, payload: dict) -> Dict[str, Any]:
        event = payload.get("event", "")

        if event == "payment.captured":
            payment = payload.get("payload", {}).get("payment", {}).get("entity", {})
            return self._handle_payment_captured(payment, payload)

        if event == "payment.failed":
            payment = payload.get("payload", {}).get("payment", {}).get("entity", {})
            order_id = payment.get("order_id")
            if order_id:
                SubscriptionService.record_payment_failed(order_id, payload)
            return {"handled": True, "event": event}

        if event == "order.paid":
            payment = payload.get("payload", {}).get("payment", {}).get("entity", {})
            if payment:
                return self._handle_payment_captured(payment, payload)
            return {"handled": True, "event": event, "note": "no payment entity"}

        return {"handled": False, "event": event}

    def _handle_payment_captured(self, payment: dict, raw_payload: dict) -> Dict[str, Any]:
        order_id = payment.get("order_id")
        payment_id = payment.get("id")
        if not order_id or not payment_id:
            return {"handled": False, "error": "missing order_id or payment_id"}

        notes = payment.get("notes") or {}
        user_id = notes.get("user_id")
        if not user_id:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT user_id FROM payment_transactions WHERE razorpay_order_id = %s",
                        (order_id,),
                    )
                    row = cur.fetchone()
                    if row:
                        user_id = row[0]

        if not user_id:
            logger.warning(
                "Webhook payment.captured — could not resolve user_id for order %s",
                order_id,
            )
            return {"handled": False, "error": "user_id not found"}

        created_at = payment.get("created_at")
        if created_at:
            paid_at = datetime.fromtimestamp(int(created_at), tz=timezone.utc)
        else:
            paid_at = datetime.now(timezone.utc)

        subscription = SubscriptionService.activate_from_payment(
            int(user_id),
            razorpay_order_id=order_id,
            razorpay_payment_id=payment_id,
            paid_at=paid_at,
            raw_payload=raw_payload,
        )
        return {"handled": True, "event": "payment.captured", "subscription": subscription}
