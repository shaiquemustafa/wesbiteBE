# service/whatsapp_service.py – WATI WhatsApp API integration
import os
import logging
import requests
from datetime import datetime
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
RITO_WEBSITE_URL = os.getenv("RITO_WEBSITE_URL", "rito.co.in")
RITO_MANAGE_URL = os.getenv("RITO_MANAGE_URL", "rito.co.in/#watchlist")


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
            otp_code: The OTP code to send

        Returns:
            True if the message was sent successfully, False otherwise.
        """
        url = f"{self.base_url}/api/v1/sendTemplateMessage?whatsappNumber={phone}"
        payload = {
            "template_name": OTP_TEMPLATE_NAME,
            "broadcast_name": f"otp_{phone}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
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

    def _update_contact_attributes(self, phone: str, attributes: list) -> bool:
        """
        Updates contact attributes in WATI before sending a template
        that uses Shopify/WooCommerce variable names.
        """
        url = f"{self.base_url}/api/v1/updateContactAttributes/{phone}"
        payload = {"customParams": attributes}

        try:
            response = requests.post(url, json=payload, headers=self.headers, timeout=15)
            logger.info("  📋 Contact attributes update for %s: status=%s body=%s",
                        phone, response.status_code,
                        response.text[:300] if response.text else "(empty)")
            return response.status_code == 200
        except Exception as e:
            logger.error("  ❌ Failed to update contact attributes for %s: %s", phone, e)
            return False

    def _build_market_update_params(self, item: dict) -> list:
        """
        Builds the template parameters list for a market update item.
        Returns list of {"name": ..., "value": ...} dicts.
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

        return [
            {"name": "shop_name", "value": item.get("company_name", "Unknown")},
            {"name": "first_name", "value": item.get("category", "General")},
            {"name": "last_name", "value": impact_display},
            {"name": "product_details", "value": item.get("summary", "No details available.")},
            {"name": "tracking_number", "value": str(news_time)},
            {"name": "catalog_checkout_url_partial_variable", "value": RITO_WEBSITE_URL},
            {"name": "tracking_url_partial_variable", "value": RITO_MANAGE_URL},
        ]

    def send_market_update(self, phone: str, item: dict) -> bool:
        """
        Sends a market update notification to a SINGLE user using the v1 API.
        Used as fallback if broadcast fails.
        """
        params = self._build_market_update_params(item)

        # Step 1: Set contact attributes
        self._update_contact_attributes(phone, params)

        # Step 2: Send template with UNIQUE broadcast name
        broadcast_name = f"market_{item.get('scrip_cd', 'x')}_{phone}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        url = f"{self.base_url}/api/v1/sendTemplateMessage?whatsappNumber={phone}"
        payload = {
            "template_name": MARKET_UPDATE_TEMPLATE,
            "broadcast_name": broadcast_name,
            "parameters": params,
        }

        try:
            logger.info("  📤 WATI v1 template: %s to %s | broadcast: %s", MARKET_UPDATE_TEMPLATE, phone, broadcast_name)
            response = requests.post(url, json=payload, headers=self.headers, timeout=15)

            logger.info("  📥 WATI response status: %s", response.status_code)
            logger.info("  📥 WATI response body: %s", response.text[:500] if response.text else "(empty)")

            if not response.text or not response.text.strip():
                return False

            data = response.json()
            success = data.get("result", False)

            if success:
                logger.info("  ✅ Template '%s' sent to %s", MARKET_UPDATE_TEMPLATE, phone)
            else:
                logger.warning("  ❌ Template failed for %s: %s", phone, data.get("info", "Unknown"))

            return success
        except Exception as e:
            logger.error("  ❌ WhatsApp API error for %s: %s", phone, e)
            return False

    def send_market_update_broadcast(self, phones: list, item: dict) -> dict:
        """
        Sends a market update to MULTIPLE users in a single API call
        using WATI's broadcast API (/api/v2/sendTemplateMessages).

        This is the scalable approach — 1 HTTP request for 1000+ users.

        Args:
            phones: List of phone numbers (with country code, no '+')
            item:   Enriched prediction dict

        Returns:
            {"sent": int, "failed": int}
        """
        if not phones:
            return {"sent": 0, "failed": 0}

        params = self._build_market_update_params(item)
        scrip = item.get("scrip_cd", "x")
        broadcast_name = f"market_{scrip}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Build receivers list — each gets the same params for this stock
        receivers = []
        for phone in phones:
            receivers.append({
                "whatsappNumber": phone,
                "customParams": params,
            })

        url = f"{self.base_url}/api/v2/sendTemplateMessages"
        payload = {
            "template_name": MARKET_UPDATE_TEMPLATE,
            "broadcast_name": broadcast_name,
            "receivers": receivers,
        }

        try:
            logger.info(
                "  📤 WATI v2 broadcast: template=%s | broadcast=%s | receivers=%d",
                MARKET_UPDATE_TEMPLATE, broadcast_name, len(receivers),
            )

            response = requests.post(url, json=payload, headers=self.headers, timeout=30)

            logger.info("  📥 WATI broadcast response status: %s", response.status_code)
            logger.info("  📥 WATI broadcast response body: %s", response.text[:500] if response.text else "(empty)")

            if not response.text or not response.text.strip():
                logger.error("  ❌ WATI broadcast returned empty response (status %s)", response.status_code)
                return {"sent": 0, "failed": len(phones)}

            data = response.json()
            success = data.get("result", False)

            if success:
                logger.info(
                    "  ✅ Broadcast '%s' accepted by WATI for %d contacts",
                    broadcast_name, len(phones),
                )
                return {"sent": len(phones), "failed": 0}
            else:
                error_info = data.get("info", "Unknown error")
                logger.warning("  ❌ Broadcast failed: %s", error_info)

                # Fallback: try sending one by one via v1 API
                logger.info("  🔄 Falling back to v1 single-send for %d contacts...", len(phones))
                sent = 0
                failed = 0
                for phone in phones:
                    try:
                        ok = self.send_market_update(phone, item)
                        if ok:
                            sent += 1
                        else:
                            failed += 1
                    except Exception as e:
                        logger.error("  ❌ v1 fallback failed for %s: %s", phone, e)
                        failed += 1
                return {"sent": sent, "failed": failed}

        except Exception as e:
            logger.error("  ❌ WATI broadcast API error: %s", e)
            # Fallback to v1
            logger.info("  🔄 Falling back to v1 single-send for %d contacts...", len(phones))
            sent = 0
            failed = 0
            for phone in phones:
                try:
                    ok = self.send_market_update(phone, item)
                    if ok:
                        sent += 1
                    else:
                        failed += 1
                except Exception as ex:
                    logger.error("  ❌ v1 fallback failed for %s: %s", phone, ex)
                    failed += 1
            return {"sent": sent, "failed": failed}

    def send_template_message(self, phone: str, template_name: str, parameters: list = None) -> bool:
        """
        Sends a generic template message via WhatsApp (v1, single contact).
        """
        broadcast_name = f"{template_name}_{phone}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        url = f"{self.base_url}/api/v1/sendTemplateMessage?whatsappNumber={phone}"
        payload = {
            "template_name": template_name,
            "broadcast_name": broadcast_name,
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
