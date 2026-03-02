# service/whatsapp_service.py – WATI WhatsApp API integration
import os
import logging
import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("uvicorn.error")

WATI_BASE_URL = os.getenv("WATI_BASE_URL", "https://live-mt-server.wati.io/10103450")
WATI_API_TOKEN = os.getenv("WATI_API_TOKEN", "")

# Template used for OTP messages (approved authentication template)
OTP_TEMPLATE_NAME = os.getenv("WATI_OTP_TEMPLATE", "otp_login")
# The parameter name in your template that holds the OTP value
OTP_PARAM_NAME = os.getenv("WATI_OTP_PARAM", "1")

# Template for market update notifications
MARKET_UPDATE_TEMPLATE = os.getenv("WATI_MARKET_TEMPLATE", "market_update_1")

# RITO website URL (used in notification messages)
RITO_WEBSITE_URL = os.getenv("RITO_WEBSITE_URL", "https://rito.co.in")
RITO_MANAGE_URL = os.getenv("RITO_MANAGE_URL", "https://rito.co.in/preferences")


class WhatsAppService:
    """Sends WhatsApp messages via WATI API."""

    def __init__(self):
        self.base_url = WATI_BASE_URL.rstrip("/") if WATI_BASE_URL else ""
        # Strip "Bearer " prefix if someone pasted the full header value
        raw_token = (WATI_API_TOKEN or "").strip()
        if raw_token.lower().startswith("bearer "):
            raw_token = raw_token[7:].strip()
        self.token = raw_token
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json-patch+json",
        }

    def send_otp(self, phone: str, otp_code: str) -> bool:
        """
        Sends an OTP to the given phone number via WhatsApp.

        Args:
            phone: Phone number with country code, no '+' (e.g., "919474841416")
            otp_code: The 6-digit OTP code to send

        Returns:
            True if the message was sent successfully, False otherwise.
        """
        url = f"{self.base_url}/api/v1/sendTemplateMessage?whatsappNumber={phone}"
        payload = {
            "template_name": OTP_TEMPLATE_NAME,
            "broadcast_name": "otp_login",
            "parameters": [
                {"name": OTP_PARAM_NAME, "value": otp_code}
            ],
        }

        try:
            response = requests.post(url, json=payload, headers=self.headers, timeout=15)

            if not response.text or not response.text.strip():
                logger.error("  ❌ WATI returned empty response (status %s) for %s", response.status_code, phone)
                return False

            data = response.json()
            success = data.get("result", False)

            if success:
                logger.info("  ✅ OTP sent to %s via WhatsApp", phone)
            else:
                logger.warning("  ❌ Failed to send OTP to %s: %s", phone, data.get("info", "Unknown error"))

            return success
        except Exception as e:
            logger.error("  ❌ WhatsApp API error sending OTP to %s: %s", phone, e)
            return False

    def send_market_update(self, phone: str, item: dict) -> bool:
        """
        Sends a market update notification using the market_update_1 template.

        Template parameter mapping:
            shop_name                          → Company Name
            first_name                         → Category
            last_name                          → Impact tag (🟢 POSITIVE / 🔴 NEGATIVE etc.)
            product_details                    → Summary
            tracking_number                    → News Time
            catalog_checkout_url_partial_variable → RITO website link
            tracking_url_partial_variable      → Manage alerts link
        """
        impact_raw = (item.get("impact") or "UNKNOWN").upper()
        impact_emoji_map = {
            "POSITIVE": "🟢 POSITIVE",
            "STRONGLY POSITIVE": "🟢 STRONGLY POSITIVE",
            "BEAT": "🟢 BEAT",
            "NEGATIVE": "🔴 NEGATIVE",
            "STRONGLY NEGATIVE": "🔴 STRONGLY NEGATIVE",
            "MISSED": "🔴 MISSED",
            "NEUTRAL": "⚪ NEUTRAL",
        }
        impact_display = impact_emoji_map.get(impact_raw, f"⚪ {impact_raw}")

        # Format news_time nicely
        news_time = item.get("news_time", "")
        if news_time:
            try:
                from datetime import datetime
                if isinstance(news_time, str):
                    dt = datetime.fromisoformat(news_time)
                elif isinstance(news_time, datetime):
                    dt = news_time
                else:
                    dt = None
                if dt:
                    news_time = dt.strftime("%-d %b, %I:%M %p")
            except Exception:
                news_time = str(news_time)

        parameters = [
            {"name": "shop_name", "value": item.get("company_name", "Unknown")},
            {"name": "first_name", "value": item.get("category", "General")},
            {"name": "last_name", "value": impact_display},
            {"name": "product_details", "value": item.get("summary", "No details available.")},
            {"name": "tracking_number", "value": str(news_time)},
            {"name": "catalog_checkout_url_partial_variable", "value": RITO_WEBSITE_URL},
            {"name": "tracking_url_partial_variable", "value": RITO_MANAGE_URL},
        ]

        return self.send_template_message(phone, MARKET_UPDATE_TEMPLATE, parameters)

    def send_template_message(self, phone: str, template_name: str, parameters: list = None) -> bool:
        """
        Sends a generic template message via WhatsApp.

        Args:
            phone: Phone number with country code, no '+'
            template_name: Name of the approved template
            parameters: List of dicts [{"name": "...", "value": "..."}]

        Returns:
            True if sent successfully.
        """
        url = f"{self.base_url}/api/v1/sendTemplateMessage?whatsappNumber={phone}"
        payload = {
            "template_name": template_name,
            "broadcast_name": "api_message",
            "parameters": parameters or [],
        }

        try:
            logger.info("  📤 WATI template request: POST %s", url)
            logger.info("  📤 WATI template: %s | params: %s", template_name, parameters)

            response = requests.post(url, json=payload, headers=self.headers, timeout=15)

            logger.info("  📥 WATI response status: %s", response.status_code)
            logger.info("  📥 WATI response body: %s", response.text[:500] if response.text else "(empty)")

            if not response.text or not response.text.strip():
                logger.error("  ❌ WATI returned empty response (status %s) for %s", response.status_code, phone)
                return False

            data = response.json()
            success = data.get("result", False)

            if success:
                logger.info("  ✅ Template '%s' sent to %s", template_name, phone)
            else:
                logger.warning("  ❌ Template '%s' failed for %s: %s", template_name, phone, data.get("info", "Unknown"))

            return success
        except Exception as e:
            logger.error("  ❌ WhatsApp API error for %s: %s", phone, e)
            return False
