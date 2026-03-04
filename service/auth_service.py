# service/auth_service.py – OTP generation, verification & JWT token management
import os
import re
import random
import logging
from datetime import datetime, timedelta, timezone

import jwt
from dotenv import load_dotenv

from database import get_conn
from service.whatsapp_service import WhatsAppService

load_dotenv()

logger = logging.getLogger("uvicorn.error")

# ── Configuration ────────────────────────────────────────────────────
JWT_SECRET = os.getenv("JWT_SECRET", "change-me-to-a-random-secret-key")
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_DAYS = int(os.getenv("JWT_EXPIRY_DAYS", "30"))

OTP_LENGTH = 4
OTP_EXPIRY_MINUTES = 5
OTP_MAX_ATTEMPTS = 3
OTP_COOLDOWN_SECONDS = 60   # min gap between OTP requests for the same number


class AuthService:
    """Handles OTP login, JWT issuance, and user management."""

    def __init__(self):
        self.whatsapp = WhatsAppService()

    # ──────────────────────────────────────────────────────────────────
    # Phone number validation / normalization
    # ──────────────────────────────────────────────────────────────────
    @staticmethod
    def normalize_phone(phone: str) -> str:
        """
        Strips spaces, dashes, leading '+', and ensures the number
        starts with a country code.  Returns digits only.
        E.g. "+91 94748-41416" → "919474841416"
        """
        digits = re.sub(r"[^\d]", "", phone)
        # If someone passes a 10-digit Indian number, prepend 91
        if len(digits) == 10:
            digits = "91" + digits
        return digits

    # ──────────────────────────────────────────────────────────────────
    # Send OTP
    # ──────────────────────────────────────────────────────────────────
    def send_otp(self, phone: str) -> dict:
        """
        Generates a 6-digit OTP, saves it in the DB, and sends it
        via WhatsApp.

        Returns:
            {"success": True/False, "message": "..."}
        """
        phone = self.normalize_phone(phone)

        if len(phone) < 10 or len(phone) > 15:
            return {"success": False, "message": "Invalid phone number."}

        # ── Cooldown check: don't spam OTPs ──
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT created_at FROM otp_requests
                    WHERE phone = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (phone,),
                )
                last = cur.fetchone()

        if last:
            last_created = last[0]
            # Make both timezone-aware for comparison
            if last_created.tzinfo is None:
                last_created = last_created.replace(tzinfo=timezone.utc)
            now_utc = datetime.now(timezone.utc)
            diff = (now_utc - last_created).total_seconds()
            if diff < OTP_COOLDOWN_SECONDS:
                wait = int(OTP_COOLDOWN_SECONDS - diff)
                return {
                    "success": False,
                    "message": f"Please wait {wait} seconds before requesting a new OTP.",
                }

        # ── Generate OTP ──
        otp_code = str(random.randint(10 ** (OTP_LENGTH - 1), (10 ** OTP_LENGTH) - 1))
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=OTP_EXPIRY_MINUTES)

        # ── Save to DB ──
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO otp_requests (phone, otp_code, expires_at)
                    VALUES (%s, %s, %s)
                    """,
                    (phone, otp_code, expires_at),
                )

        # ── Send via WhatsApp ──
        sent = self.whatsapp.send_otp(phone, otp_code)

        if not sent:
            return {"success": False, "message": "Failed to send OTP. Please try again."}

        logger.info("OTP sent to %s (expires at %s UTC)", phone, expires_at.strftime("%H:%M:%S"))
        return {"success": True, "message": "OTP sent to your WhatsApp."}

    # ──────────────────────────────────────────────────────────────────
    # Verify OTP  →  issue JWT
    # ──────────────────────────────────────────────────────────────────
    def verify_otp(self, phone: str, otp_code: str, name: str | None = None) -> dict:
        """
        Verifies the OTP.  On success, creates/updates the user row
        and returns a JWT token.

        Returns:
            {"success": True, "token": "...", "is_new_user": bool, "user": {...}}
            or
            {"success": False, "message": "..."}
        """
        phone = self.normalize_phone(phone)
        # Sanitize name
        clean_name = name.strip() if name else None

        # ── Fetch the latest unverified OTP for this phone ──
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, otp_code, expires_at, attempts
                    FROM otp_requests
                    WHERE phone = %s
                      AND is_verified = FALSE
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (phone,),
                )
                row = cur.fetchone()

        if not row:
            return {"success": False, "message": "No OTP found. Please request a new one."}

        otp_id, stored_otp, expires_at, attempts = row

        # Make expires_at timezone-aware if needed
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)

        # ── Check expiry ──
        if datetime.now(timezone.utc) > expires_at:
            return {"success": False, "message": "OTP has expired. Please request a new one."}

        # ── Check max attempts ──
        if attempts >= OTP_MAX_ATTEMPTS:
            return {"success": False, "message": "Too many attempts. Please request a new OTP."}

        # ── Increment attempts ──
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE otp_requests SET attempts = attempts + 1 WHERE id = %s",
                    (otp_id,),
                )

        # ── Compare OTP ──
        if otp_code.strip() != stored_otp:
            remaining = OTP_MAX_ATTEMPTS - (attempts + 1)
            return {
                "success": False,
                "message": f"Invalid OTP. {remaining} attempt(s) remaining.",
            }

        # ── Mark as verified ──
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE otp_requests SET is_verified = TRUE WHERE id = %s",
                    (otp_id,),
                )

        # ── Create or update user ──
        is_new_user = False
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, phone, name, created_at FROM users WHERE phone = %s", (phone,))
                user_row = cur.fetchone()

                if user_row:
                    # Existing user — update last_login (and name if provided)
                    if clean_name:
                        cur.execute(
                            "UPDATE users SET name = %s, last_login_at = NOW(), updated_at = NOW() WHERE phone = %s",
                            (clean_name, phone),
                        )
                    else:
                        cur.execute(
                            "UPDATE users SET last_login_at = NOW(), updated_at = NOW() WHERE phone = %s",
                            (phone,),
                        )
                    # Fetch onboarding flag
                    cur.execute(
                        "SELECT COALESCE(onboarding_complete, FALSE) FROM users WHERE phone = %s",
                        (phone,),
                    )
                    ob_row = cur.fetchone()
                    user = {
                        "id": user_row[0],
                        "phone": user_row[1],
                        "name": clean_name or user_row[2],
                        "created_at": user_row[3].isoformat() if user_row[3] else None,
                        "onboarding_complete": ob_row[0] if ob_row else False,
                    }
                else:
                    # New user — insert with name
                    is_new_user = True
                    cur.execute(
                        """
                        INSERT INTO users (phone, name, created_at, last_login_at)
                        VALUES (%s, %s, NOW(), NOW())
                        RETURNING id, phone, name, created_at
                        """,
                        (phone, clean_name),
                    )
                    new_row = cur.fetchone()
                    user = {
                        "id": new_row[0],
                        "phone": new_row[1],
                        "name": new_row[2],
                        "created_at": new_row[3].isoformat() if new_row[3] else None,
                        "onboarding_complete": False,
                    }

        # ── Issue JWT ──
        token = self._create_token(phone, user["id"])

        logger.info("User %s logged in (new=%s)", phone, is_new_user)
        return {
            "success": True,
            "token": token,
            "is_new_user": is_new_user,
            "user": user,
        }

    # ──────────────────────────────────────────────────────────────────
    # JWT helpers
    # ──────────────────────────────────────────────────────────────────
    @staticmethod
    def _create_token(phone: str, user_id: int) -> str:
        """Creates a JWT token with phone and user_id."""
        payload = {
            "phone": phone,
            "user_id": user_id,
            "exp": datetime.now(timezone.utc) + timedelta(days=JWT_EXPIRY_DAYS),
            "iat": datetime.now(timezone.utc),
        }
        return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

    @staticmethod
    def decode_token(token: str) -> dict:
        """
        Decodes and validates a JWT token.

        Returns:
            {"valid": True, "phone": "...", "user_id": ...}
            or
            {"valid": False, "message": "..."}
        """
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
            return {
                "valid": True,
                "phone": payload["phone"],
                "user_id": payload["user_id"],
            }
        except jwt.ExpiredSignatureError:
            return {"valid": False, "message": "Token expired. Please login again."}
        except jwt.InvalidTokenError:
            return {"valid": False, "message": "Invalid token."}

    # ──────────────────────────────────────────────────────────────────
    # Get user by phone
    # ──────────────────────────────────────────────────────────────────
    @staticmethod
    def get_user(phone: str) -> dict | None:
        """Fetches full user record by phone number."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, phone, name, is_active, created_at, last_login_at,
                           COALESCE(onboarding_complete, FALSE) AS onboarding_complete
                    FROM users WHERE phone = %s
                    """,
                    (phone,),
                )
                row = cur.fetchone()

        if not row:
            return None
        return {
            "id": row[0],
            "phone": row[1],
            "name": row[2],
            "is_active": row[3],
            "created_at": row[4].isoformat() if row[4] else None,
            "last_login_at": row[5].isoformat() if row[5] else None,
            "onboarding_complete": row[6],
        }

    # ──────────────────────────────────────────────────────────────────
    # Update user name
    # ──────────────────────────────────────────────────────────────────
    @staticmethod
    def update_user_name(phone: str, name: str) -> bool:
        """Updates the user's display name."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET name = %s, updated_at = NOW() WHERE phone = %s",
                    (name, phone),
                )
                return cur.rowcount > 0
