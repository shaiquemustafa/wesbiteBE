# service/watchlist_service.py – User stock watchlist & preference management
import logging
from typing import List, Optional

from database import get_conn

logger = logging.getLogger("uvicorn.error")

MIN_WATCHLIST = 3
MAX_WATCHLIST = 15


class WatchlistService:
    """CRUD operations for user watchlists and notification preferences."""

    # ──────────────────────────────────────────────────────────────────
    # Search companies (for the stock picker)
    # ──────────────────────────────────────────────────────────────────
    @staticmethod
    def search_companies(query: str, limit: int = 20) -> List[dict]:
        """
        Searches company_master by company_name or nse_symbol.
        Returns up to `limit` results sorted by market cap (largest first).
        """
        if not query or len(query.strip()) < 1:
            return []

        q = f"%{query.strip().upper()}%"

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT bse_scrip_code, company_name, nse_symbol, mkt_cap_full
                    FROM company_master
                    WHERE UPPER(company_name) LIKE %s
                       OR UPPER(nse_symbol) LIKE %s
                    ORDER BY mkt_cap_full DESC NULLS LAST
                    LIMIT %s
                    """,
                    (q, q, limit),
                )
                rows = cur.fetchall()

        return [
            {
                "bse_scrip_code": r[0],
                "company_name": r[1],
                "nse_symbol": r[2],
                "mkt_cap_cr": r[3],
            }
            for r in rows
        ]

    # ──────────────────────────────────────────────────────────────────
    # Get watchlist
    # ──────────────────────────────────────────────────────────────────
    @staticmethod
    def get_watchlist(user_id: int) -> dict:
        """
        Returns the user's watchlist and preferences.
        {
            "stocks": [{"bse_scrip_code": ..., "company_name": ..., "nse_symbol": ...}, ...],
            "receive_all_updates": bool,
            "onboarding_complete": bool,
        }
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Get user preferences
                cur.execute(
                    "SELECT receive_all_updates, onboarding_complete FROM users WHERE id = %s",
                    (user_id,),
                )
                user_row = cur.fetchone()

                if not user_row:
                    return {"stocks": [], "receive_all_updates": False, "onboarding_complete": False}

                receive_all = user_row[0] or False
                onboarding = user_row[1] or False

                # Get watchlist stocks with company info
                cur.execute(
                    """
                    SELECT w.bse_scrip_code, c.company_name, c.nse_symbol, c.mkt_cap_full
                    FROM user_watchlist w
                    LEFT JOIN company_master c ON w.bse_scrip_code = c.bse_scrip_code
                    WHERE w.user_id = %s
                    ORDER BY c.company_name
                    """,
                    (user_id,),
                )
                stocks = [
                    {
                        "bse_scrip_code": r[0],
                        "company_name": r[1] or "Unknown",
                        "nse_symbol": r[2],
                        "mkt_cap_cr": r[3],
                    }
                    for r in cur.fetchall()
                ]

        return {
            "stocks": stocks,
            "receive_all_updates": receive_all,
            "onboarding_complete": onboarding,
        }

    # ──────────────────────────────────────────────────────────────────
    # Save watchlist (replace all)
    # ──────────────────────────────────────────────────────────────────
    @staticmethod
    def save_watchlist(
        user_id: int,
        scrip_codes: List[int],
        receive_all_updates: bool,
    ) -> dict:
        """
        Replaces the user's entire watchlist with the given scrip codes
        and updates the receive_all_updates preference.

        Validates min/max constraints.

        Returns:
            {"success": True, "count": int} or {"success": False, "message": str}
        """
        if len(scrip_codes) < MIN_WATCHLIST:
            return {
                "success": False,
                "message": f"Please select at least {MIN_WATCHLIST} stocks.",
            }
        if len(scrip_codes) > MAX_WATCHLIST:
            return {
                "success": False,
                "message": f"You can select at most {MAX_WATCHLIST} stocks.",
            }

        # Deduplicate
        scrip_codes = list(set(scrip_codes))

        with get_conn() as conn:
            with conn.cursor() as cur:
                # Update user preferences
                cur.execute(
                    """
                    UPDATE users
                    SET receive_all_updates = %s,
                        onboarding_complete = TRUE,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (receive_all_updates, user_id),
                )

                # Delete old watchlist
                cur.execute(
                    "DELETE FROM user_watchlist WHERE user_id = %s",
                    (user_id,),
                )

                # Insert new watchlist
                for code in scrip_codes:
                    cur.execute(
                        """
                        INSERT INTO user_watchlist (user_id, bse_scrip_code)
                        VALUES (%s, %s)
                        ON CONFLICT (user_id, bse_scrip_code) DO NOTHING
                        """,
                        (user_id, code),
                    )

        logger.info(
            "Watchlist saved for user %s: %d stocks, receive_all=%s",
            user_id, len(scrip_codes), receive_all_updates,
        )
        return {"success": True, "count": len(scrip_codes)}

    # ──────────────────────────────────────────────────────────────────
    # Get users to notify for a specific scrip code
    # ──────────────────────────────────────────────────────────────────
    @staticmethod
    def get_users_to_notify(bse_scrip_code: int) -> List[str]:
        """
        Returns phone numbers of users who should receive a notification
        for the given scrip code. This includes:
        1. Users who have this scrip in their watchlist
        2. Users who have receive_all_updates = TRUE

        Only active users are included.
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT u.phone
                    FROM users u
                    WHERE u.is_active = TRUE
                      AND (
                          u.receive_all_updates = TRUE
                          OR u.id IN (
                              SELECT w.user_id
                              FROM user_watchlist w
                              WHERE w.bse_scrip_code = %s
                          )
                      )
                    """,
                    (bse_scrip_code,),
                )
                return [row[0] for row in cur.fetchall()]

    @staticmethod
    def get_watchlist_only_users(bse_scrip_code: int) -> List[str]:
        """
        Returns phone numbers of users who have this specific scrip in
        their watchlist. Used for low-impact (N/A, NEUTRAL, MATCHED)
        announcements — these are only sent to users who explicitly
        follow the stock, NOT to receive_all_updates users.

        Only active users are included.
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT u.phone
                    FROM users u
                    INNER JOIN user_watchlist w ON w.user_id = u.id
                    WHERE u.is_active = TRUE
                      AND w.bse_scrip_code = %s
                    """,
                    (bse_scrip_code,),
                )
                return [row[0] for row in cur.fetchall()]
