# service/notification_service.py – Send market updates to relevant users
import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import List

from psycopg2.extras import Json

from database import get_conn
from service.whatsapp_service import WhatsAppService
from service.watchlist_service import WatchlistService

logger = logging.getLogger("uvicorn.error")

IST = timezone(timedelta(hours=5, minutes=30))


def _digest_broadcast_enabled() -> bool:
    return os.getenv("WHATSAPP_DIGEST_BROADCAST_ENABLED", "true").strip().lower() in (
        "1",
        "true",
        "yes",
    )


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

    def _get_receive_all_active_phones(self) -> List[str]:
        """Active users who opted into receive_all_updates (high-impact / all-company path)."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT phone FROM users
                    WHERE is_active = TRUE AND receive_all_updates = TRUE
                    """
                )
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

        # Use the broadcast API — sends to all users (regular broadcast, not watchlist-only)
        result = self.whatsapp.send_market_update_broadcast(phones, enriched_item, is_watchlist=False)

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

        result = self.whatsapp.send_market_update_broadcast(phones, enriched_item, is_watchlist=True)
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

    def notify_quick_pulse_digest(self, summary_text: str) -> dict:
        """
        Sends the Quick pulse digest to active users with receive_all_updates = TRUE
        (same high-impact audience as the per-news “all companies” path). Param 3 is
        current IST send time.
        """
        text = (summary_text or "").strip()
        if not text:
            logger.info("  📭 Quick pulse digest skipped — empty summary.")
            return {"total_users": 0, "sent": 0, "failed": 0}

        phones = [
            str(p).strip()
            for p in self._get_receive_all_active_phones()
            if p and str(p).strip()
        ]
        if not phones:
            logger.info("  📭 Quick pulse digest — no active users with receive_all_updates.")
            return {"total_users": 0, "sent": 0, "failed": 0}

        logger.info(
            "  📢 Quick pulse digest (receive_all_updates) → %d recipient(s)",
            len(phones),
        )
        result = self.whatsapp.send_quick_pulse_digest_broadcast(phones, text)
        sent = result.get("sent", 0)
        failed = result.get("failed", 0)
        logger.info(
            "  📢 Quick pulse digest done: %d sent, %d failed.",
            sent,
            failed,
        )
        return {"total_users": len(phones), "sent": sent, "failed": failed}

    def send_quick_pulse_digest_test(self, phone: str, summary_text: str) -> dict:
        """Single-phone QA send using the Quick pulse digest template."""
        p = (phone or "").strip()
        text = (summary_text or "").strip()
        if not p:
            return {"ok": False, "error": "phone required"}
        if not text:
            return {"ok": False, "error": "summary_text empty"}
        result = self.whatsapp.send_quick_pulse_digest_broadcast([p], text)
        return {
            "ok": result.get("sent", 0) > 0,
            "sent": result.get("sent", 0),
            "failed": result.get("failed", 0),
            "phone_tail": p[-4:] if len(p) >= 4 else p,
        }

    def publish_quick_pulse_digest_after_summary(
        self,
        summary_ui_data_id: int,
        slot: str,
        briefing_day: date,
        summary_text: str,
    ) -> dict:
        """
        Inserts a whatsapp_broadcast row (kind ui_summary_digest) and sends immediately.
        Idempotent per summary_ui_data_id.
        """
        if not _digest_broadcast_enabled():
            logger.info(
                "Quick pulse digest skipped (WHATSAPP_DIGEST_BROADCAST_ENABLED=false) summary_ui_data_id=%s",
                summary_ui_data_id,
            )
            return {"ok": False, "skipped": "digest_broadcast_disabled"}

        text = (summary_text or "").strip()
        if not text:
            return {"ok": False, "skipped": "empty_summary"}

        data_obj = {
            "kind": "ui_summary_digest",
            "summary_ui_data_id": summary_ui_data_id,
            "slot": slot,
            "briefing_date_ist": briefing_day.isoformat(),
        }
        created_at_ist = datetime.now(IST)

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id FROM whatsapp_broadcast
                        WHERE data->>'kind' = 'ui_summary_digest'
                          AND (data->>'summary_ui_data_id')::bigint = %s
                        LIMIT 1
                        """,
                        (summary_ui_data_id,),
                    )
                    existing = cur.fetchone()
                    if existing:
                        logger.info(
                            "Quick pulse digest already queued for summary_ui_data_id=%s (broadcast id=%s)",
                            summary_ui_data_id,
                            existing[0],
                        )
                        return {
                            "ok": False,
                            "skipped": "already_enqueued",
                            "broadcast_id": existing[0],
                        }

                    cur.execute(
                        """
                        INSERT INTO whatsapp_broadcast (
                            scrip_cd, company_name, impact, category, slot,
                            summary, pdf_link, news_time, mkt_cap_cr, data, created_at
                        )
                        VALUES (
                            NULL, %s, %s, NULL, %s, %s, NULL, NULL, NULL, %s, %s
                        )
                        RETURNING id
                        """,
                        (
                            "Quick pulse",
                            "DIGEST",
                            slot,
                            text,
                            Json(data_obj),
                            created_at_ist,
                        ),
                    )
                    wb_id = cur.fetchone()[0]
        except Exception as e:
            logger.exception("Quick pulse whatsapp_broadcast insert failed: %s", e)
            return {"ok": False, "error": str(e)}

        notify = self.notify_quick_pulse_digest(text)
        sent = notify.get("sent", 0)

        if sent > 0:
            try:
                sent_at_ist = datetime.now(IST)
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE whatsapp_broadcast
                            SET sent_at = %s
                            WHERE id = %s
                            """,
                            (sent_at_ist, wb_id),
                        )
            except Exception as e:
                logger.warning("Failed to set sent_at for digest broadcast %s: %s", wb_id, e)

        return {
            "ok": True,
            "broadcast_id": wb_id,
            "notify": notify,
        }
