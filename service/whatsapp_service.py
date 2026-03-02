# service/whatsapp_service.py – WATI WhatsApp API integration
import os
import logging
import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("uvicorn.error")

WATI_BASE_URL = os.getenv("WATI_BASE_URL", "https://live-mt-server.wati.io/10103450")
WATI_API_TOKEN = os.getenv("WATI_API_TOKEN", "")

# Template used for OTP messages (change this when you get a proper OTP template approved)
OTP_TEMPLATE_NAME = os.getenv("WATI_OTP_TEMPLATE", "test22")
# The parameter name in your template that holds the OTP value
OTP_PARAM_NAME = os.getenv("WATI_OTP_PARAM", "total_price")


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
            logger.info("  📤 WATI request: POST %s", url)
            logger.info("  📤 WATI payload: %s", payload)
            logger.info("  📤 WATI base_url env: %s", self.base_url)
            logger.info("  📤 WATI token present: %s (len=%s)", bool(self.token), len(self.token) if self.token else 0)

            response = requests.post(url, json=payload, headers=self.headers, timeout=15)

            logger.info("  📥 WATI response status: %s", response.status_code)
            logger.info("  📥 WATI response body: %s", response.text[:500] if response.text else "(empty)")

            if not response.text or not response.text.strip():
                logger.error("  ❌ WATI returned empty response body (status %s)", response.status_code)
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
            response = requests.post(url, json=payload, headers=self.headers, timeout=15)
            data = response.json()
            return data.get("result", False)
        except Exception as e:
            logger.error("  ❌ WhatsApp API error for %s: %s", phone, e)
            return False
