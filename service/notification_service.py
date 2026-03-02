# service/notification_service.py – Send market updates to all registered users
import logging
from typing import List

from database import get_conn
from service.whatsapp_service import WhatsAppService

logger = logging.getLogger("uvicorn.error")


class NotificationService:
    """Sends WhatsApp notifications to all active users."""

    def __init__(self):
        self.whatsapp = WhatsAppService()

    def _get_all_active_users(self) -> List[str]:
        """Returns a list of phone numbers for all active users."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT phone FROM users WHERE is_active = TRUE")
                rows = cur.fetchall()
        return [row[0] for row in rows]

    def notify_all_users(self, enriched_item: dict) -> dict:
        """
        Sends a market update notification to ALL active users
        for a single enriched UI data item.

        Args:
            enriched_item: The enriched prediction dict with company_name,
                           impact, summary, category, news_time, etc.

        Returns:
            {"total_users": int, "sent": int, "failed": int}
        """
        phones = self._get_all_active_users()

        if not phones:
            logger.info("  📭 No active users to notify.")
            return {"total_users": 0, "sent": 0, "failed": 0}

        company = enriched_item.get("company_name", "Unknown")
        logger.info(
            "  📢 Sending market update for '%s' to %d user(s)...",
            company, len(phones),
        )

        sent = 0
        failed = 0

        for phone in phones:
            try:
                success = self.whatsapp.send_market_update(phone, enriched_item)
                if success:
                    sent += 1
                else:
                    failed += 1
            except Exception as e:
                logger.error("  ❌ Failed to notify %s: %s", phone, e)
                failed += 1

        logger.info(
            "  📢 Notification complete for '%s': %d sent, %d failed out of %d users.",
            company, sent, failed, len(phones),
        )

        return {"total_users": len(phones), "sent": sent, "failed": failed}

    def notify_all_users_bulk(self, enriched_items: List[dict]) -> dict:
        """
        Sends market update notifications for multiple items.
        Each item is sent to all active users.

        Args:
            enriched_items: List of enriched prediction dicts.

        Returns:
            {"items_processed": int, "total_sent": int, "total_failed": int}
        """
        total_sent = 0
        total_failed = 0

        for item in enriched_items:
            result = self.notify_all_users(item)
            total_sent += result["sent"]
            total_failed += result["failed"]

        return {
            "items_processed": len(enriched_items),
            "total_sent": total_sent,
            "total_failed": total_failed,
        }
