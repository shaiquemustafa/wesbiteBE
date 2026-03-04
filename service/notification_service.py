# service/notification_service.py – Send market updates to relevant users
import logging
from typing import List

from database import get_conn
from service.whatsapp_service import WhatsAppService
from service.watchlist_service import WatchlistService

logger = logging.getLogger("uvicorn.error")


class NotificationService:
    """Sends WhatsApp notifications to users based on their watchlists."""

    def __init__(self):
        self.whatsapp = WhatsAppService()
        self.watchlist_service = WatchlistService()

    def _get_all_active_phones(self) -> List[str]:
        """Fallback: returns all active user phone numbers."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT phone FROM users WHERE is_active = TRUE")
                return [row[0] for row in cur.fetchall()]

    def notify_all_users(self, enriched_item: dict) -> dict:
        """
        Sends a market update notification using WATI's broadcast API (v2).

        Targets:
        - Users who have this stock in their watchlist
        - Users who have receive_all_updates = TRUE
        - Users who haven't completed onboarding yet (they get everything)

        Args:
            enriched_item: The enriched prediction dict.

        Returns:
            {"total_users": int, "sent": int, "failed": int}
        """
        company = enriched_item.get("company_name", "Unknown")
        scrip_cd = enriched_item.get("scrip_cd") or enriched_item.get("SCRIP_CD")

        if scrip_cd:
            try:
                scrip_cd = int(scrip_cd)
                phones = self.watchlist_service.get_users_to_notify(scrip_cd)
            except (ValueError, TypeError):
                phones = self._get_all_active_phones()
        else:
            phones = self._get_all_active_phones()

        if not phones:
            logger.info("  📭 No users to notify for '%s'.", company)
            return {"total_users": 0, "sent": 0, "failed": 0}

        logger.info(
            "  📢 Sending market update for '%s' (scrip=%s) to %d user(s) via broadcast API...",
            company, scrip_cd, len(phones),
        )

        # Use the broadcast (v2) API — single HTTP request for all users
        result = self.whatsapp.send_market_update_broadcast(phones, enriched_item)

        sent = result.get("sent", 0)
        failed = result.get("failed", 0)

        logger.info(
            "  📢 Notification complete for '%s': %d sent, %d failed out of %d users.",
            company, sent, failed, len(phones),
        )

        return {"total_users": len(phones), "sent": sent, "failed": failed}

    def notify_watchlist_only(self, enriched_item: dict) -> dict:
        """
        Sends a market update notification ONLY to users who have the stock
        in their watchlist. Used for low-impact (N/A, NEUTRAL, MATCHED) items.
        """
        company = enriched_item.get("company_name", "Unknown")
        scrip_cd = enriched_item.get("scrip_cd") or enriched_item.get("SCRIP_CD")

        if not scrip_cd:
            logger.info("  📭 No scrip_cd for '%s', skipping watchlist-only notification.", company)
            return {"total_users": 0, "sent": 0, "failed": 0}

        try:
            scrip_cd = int(scrip_cd)
            phones = self.watchlist_service.get_watchlist_only_users(scrip_cd)
        except (ValueError, TypeError):
            logger.warning("  ⚠️ Invalid scrip_cd '%s' for watchlist-only notification.", scrip_cd)
            return {"total_users": 0, "sent": 0, "failed": 0}

        if not phones:
            logger.info("  📭 No watchlist users for '%s' (scrip=%s).", company, scrip_cd)
            return {"total_users": 0, "sent": 0, "failed": 0}

        logger.info(
            "  📢 Sending watchlist-only update for '%s' (scrip=%s, impact=%s) to %d user(s)...",
            company, scrip_cd, enriched_item.get("impact", "?"), len(phones),
        )

        result = self.whatsapp.send_market_update_broadcast(phones, enriched_item)
        sent = result.get("sent", 0)
        failed = result.get("failed", 0)

        logger.info(
            "  📢 Watchlist-only notification for '%s': %d sent, %d failed.",
            company, sent, failed,
        )
        return {"total_users": len(phones), "sent": sent, "failed": failed}

    def notify_watchlist_only_bulk(self, enriched_items: List[dict]) -> dict:
        """
        Sends watchlist-only notifications for multiple low-impact items.
        """
        total_sent = 0
        total_failed = 0

        for item in enriched_items:
            result = self.notify_watchlist_only(item)
            total_sent += result["sent"]
            total_failed += result["failed"]

        return {
            "items_processed": len(enriched_items),
            "total_sent": total_sent,
            "total_failed": total_failed,
        }

    def notify_all_users_bulk(self, enriched_items: List[dict]) -> dict:
        """
        Sends market update notifications for multiple items.

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
