# service/whatsapp_service.py – Gupshup WhatsApp API integration
import os
import json
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv
from database import get_conn

load_dotenv()

logger = logging.getLogger("uvicorn.error")

# Gupshup API Configuration - Hardcoded values
GUPSHUP_BASE_URL = "https://api.gupshup.io/wa/api/v1"
GUPSHUP_API_KEY = "yaxv5f904hj8saxxdu9enwlhkwvwyyns"
GUPSHUP_SOURCE_NUMBER = "919187624274"  # Your WhatsApp Business number
GUPSHUP_APP_NAME = "RITI01"  # Your app name

# Template used for OTP messages (approved authentication template)
OTP_TEMPLATE_ID = "0c9997c6-a0da-4294-b215-62b75e7fab61"
OTP_TEMPLATE_NAME = "auth_user_1"

# Template for market update notifications (WhatsApp broadcast)
# Watchlist template: for users who have specific stocks in their watchlist
WATCHLIST_TEMPLATE_ID = "1f019a3b-1bd3-4af9-91ff-118955cea4a0"
# High-impact template: for users who opted for high-impact news from all companies
HIGH_IMPACT_TEMPLATE_ID = "ade884d4-7716-46a0-a371-a234158dd9c1"
# Legacy template (kept for backward compatibility, but not used)
MARKET_UPDATE_TEMPLATE_ID = "109c6887-f03c-4617-a4d3-efd0c8c59ddf"

# RITO website URL (used in notification messages)
RITO_WEBSITE_URL = os.getenv("RITO_WEBSITE_URL", "rito.co.in")
RITO_MANAGE_URL = os.getenv("RITO_MANAGE_URL", "rito.co.in/#watchlist")


class WhatsAppService:
    """Sends WhatsApp messages via Gupshup API."""

    def __init__(self):
        self.base_url = GUPSHUP_BASE_URL.rstrip("/")
        self.api_key = GUPSHUP_API_KEY
        self.headers = {
            "apikey": self.api_key,
            "Content-Type": "application/x-www-form-urlencoded",
        }
    
    def _store_message_delivery_record(self, message_id: str, phone: str, message_title: str):
        """
        Stores message delivery record directly in message_delivery_status table.
        Creates initial record when message is sent with user_name and message_title.
        """
        try:
            # Look up user name
            user_name = None
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT name FROM users WHERE phone = %s", (phone,))
                    user_row = cur.fetchone()
                    if user_row:
                        user_name = user_row[0]
                    
                    # Create initial record in message_delivery_status
                    cur.execute(
                        """
                        INSERT INTO message_delivery_status 
                            (message_id, phone, user_name, message_title, status, timestamp)
                        VALUES (%s, %s, %s, %s, 'sent', NOW())
                        ON CONFLICT (message_id) DO UPDATE SET
                            user_name = COALESCE(message_delivery_status.user_name, EXCLUDED.user_name),
                            message_title = COALESCE(message_delivery_status.message_title, EXCLUDED.message_title)
                        """,
                        (message_id, phone, user_name, message_title),
                    )
        except Exception as e:
            logger.warning("  ⚠️ Failed to store message delivery record for %s: %s", message_id, e)

    def send_otp(self, phone: str, otp_code: str) -> bool:
        """
        Sends an OTP to the given phone number via WhatsApp using Gupshup API.

        Args:
            phone: Phone number with country code, no '+' (e.g., "919474841416")
            otp_code: The OTP code to send

        Returns:
            True if the message was sent successfully, False otherwise.
        """
        # Gupshup API endpoint for template messages
        url = f"{self.base_url}/template/msg"
        
        # Build template object as per Gupshup API format
        # Based on your curl, the template expects 2 parameters
        # Both are set to the OTP code - adjust if your template needs different values
        template_obj = {
            "id": OTP_TEMPLATE_ID,
            "params": [otp_code, otp_code]  # Template has 2 params - both set to OTP code
        }
        
        # Gupshup expects form-urlencoded data (matching your curl format)
        payload = {
            "channel": "whatsapp",
            "source": GUPSHUP_SOURCE_NUMBER,  # 919187624274
            "destination": phone,  # Recipient phone number
            "src.name": GUPSHUP_APP_NAME,  # RITI01
            "template": json.dumps(template_obj),  # Template as JSON string
        }
        
        try:
            logger.info("  📤 Sending OTP via Gupshup to %s using template ID %s", phone, OTP_TEMPLATE_ID)
            logger.info("  📤 Template params: %s", template_obj["params"])
            response = requests.post(url, data=payload, headers=self.headers, timeout=15)

            logger.info("  📥 Gupshup response status: %s", response.status_code)
            logger.info("  📥 Gupshup response body: %s", response.text[:500] if response.text else "(empty)")

            if not response.text or not response.text.strip():
                logger.error("  ❌ Gupshup returned empty response (status %s) for %s", response.status_code, phone)
                return False

            # Gupshup typically returns JSON
            try:
                data = response.json()
            except ValueError:
                # If response is not JSON, check status code
                if response.status_code == 200:
                    logger.info("  ✅ OTP sent to %s via WhatsApp (non-JSON response)", phone)
                    return True
                else:
                    logger.error("  ❌ Gupshup returned non-JSON response (status %s): %s", response.status_code, response.text[:200])
                    return False

            # Check for success indicators in Gupshup response
            status = data.get("status", "").lower()
            if isinstance(data.get("response"), dict):
                response_status = data.get("response", {}).get("status", "")
                status = status or response_status.lower()
            
            # Also check for "accepted" status
            success = (
                status in ["success", "submitted", "accepted"] or 
                response.status_code == 200 or
                data.get("accepted", False)
            )

            if success:
                # Log message ID if available (for tracking via webhooks)
                message_id = data.get("messageId") or data.get("id") or data.get("response", {}).get("messageId")
                if message_id:
                    # Store directly in message_delivery_status
                    self._store_message_delivery_record(message_id, phone, "OTP")
                    logger.info("  ✅ OTP sent to %s via WhatsApp (Gupshup) - messageId: %s", phone, message_id)
                else:
                    logger.info("  ✅ OTP sent to %s via WhatsApp (Gupshup)", phone)
            else:
                error_msg = (
                    data.get("message") or 
                    data.get("error") or 
                    data.get("response", {}).get("message", "") if isinstance(data.get("response"), dict) else "" or
                    "Unknown error"
                )
                logger.warning("  ❌ Failed to send OTP to %s: %s", phone, error_msg)

            return success
        except Exception as e:
            logger.error("  ❌ Gupshup API error sending OTP to %s: %s", phone, e)
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

    def _build_market_update_params(self, item: dict, is_watchlist: bool = False) -> list:
        """
        Builds the template parameters list for a market update item.
        Returns list of 5 parameters for Gupshup template.
        
        Args:
            item: Enriched prediction dict
            is_watchlist: True if this is a watchlist-only notification, False for regular broadcast
        
        Returns:
            List of 5 parameter values: [user, company_context, category_impact, summary, time]
        """
        company_name = item.get("company_name", "Unknown")
        
        # Parameter 1: Just "user" (not personalized)
        param1 = "user"
        
        # Parameter 2: Company name (templates are clear, no need for prefixes)
        # Just the company name with formatting for visual appeal
        param2 = f"📊 *{company_name}*"
        
        # Parameter 3: Category and impact with color-coded emojis
        category = item.get("category", "General")
        impact_raw = (item.get("impact") or "UNKNOWN").upper()
        
        # Format impact with color-coded emojis (like WATI had)
        impact_emoji_map = {
            "POSITIVE": "🟢 Positive",
            "STRONGLY POSITIVE": "🟢 Strongly Positive",
            "BEAT": "🟢 Beat",
            "NEGATIVE": "🔴 Negative",
            "STRONGLY NEGATIVE": "🔴 Strongly Negative",
            "MISSED": "🔴 Missed",
            "NEUTRAL": "⚪ Neutral",
        }
        impact_display = impact_emoji_map.get(impact_raw, f"⚪ {impact_raw}")
        
        # Combine category and impact with emoji for better visual impact
        param3 = f"{category} {impact_display}"
        
        # Parameter 4: Summary
        param4 = item.get("summary", "No details available.")
        
        # Parameter 5: Time with full date (e.g., "9 Mar, 10:52 PM")
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
                    # Format as "9 Mar, 10:52 PM" style (date + time with uppercase PM)
                    date_str = dt.strftime("%-d %b")  # e.g., "9 Mar"
                    time_str = dt.strftime("%-I:%M %p")  # e.g., "10:52 PM"
                    param5 = f"{date_str}, {time_str}"  # e.g., "9 Mar, 10:52 PM"
                else:
                    param5 = str(news_time)
            except Exception:
                param5 = str(news_time)
        else:
            param5 = "N/A"
        
        return [param1, param2, param3, param4, param5]

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

    def send_market_update_broadcast(self, phones: list, item: dict, is_watchlist: bool = False) -> dict:
        """
        Sends a market update to MULTIPLE users using Gupshup API.

        Args:
            phones: List of phone numbers (with country code, no '+')
            item: Enriched prediction dict
            is_watchlist: True if this is a watchlist-only notification, False for regular broadcast

        Returns:
            {"sent": int, "failed": int}
        """
        if not phones:
            return {"sent": 0, "failed": 0}

        # Build parameters for Gupshup template
        params = self._build_market_update_params(item, is_watchlist)
        
        # Gupshup API endpoint for template messages
        url = f"{self.base_url}/template/msg"
        
        # Build template object
        template_obj = {
            "id": MARKET_UPDATE_TEMPLATE_ID,
            "params": params  # 5 parameters as array
        }
        
        sent_count = 0
        failed_count = 0
    
        # Gupshup doesn't have a bulk API like WATI, so we send individually
        template_id = WATCHLIST_TEMPLATE_ID if is_watchlist else HIGH_IMPACT_TEMPLATE_ID
        logger.info(
            "  📤 Gupshup broadcast: template=%s | receivers=%d | watchlist=%s",
            template_id, len(phones), is_watchlist,
        )
        
        for phone in phones:
            try:
                payload = {
                    "channel": "whatsapp",
                    "source": GUPSHUP_SOURCE_NUMBER,  # 919187624274
                    "destination": phone,  # Recipient phone number
                    "src.name": GUPSHUP_APP_NAME,  # RITI01
                    "template": json.dumps(template_obj),  # Template as JSON string
                }
                
                response = requests.post(url, data=payload, headers=self.headers, timeout=15)
                
                if not response.text or not response.text.strip():
                    logger.warning("  ⚠️ Gupshup returned empty response (status %s) for %s", response.status_code, phone)
                    failed_count += 1
                    continue
                
                # Check response
                try:
                    data = response.json()
                except ValueError:
                    if response.status_code == 200:
                        sent_count += 1
                        continue
                    else:
                        logger.warning("  ⚠️ Gupshup non-JSON response (status %s) for %s: %s", response.status_code, phone, response.text[:200])
                        failed_count += 1
                        continue
                
                # Check for success
                status = data.get("status", "").lower()
                if isinstance(data.get("response"), dict):
                    response_status = data.get("response", {}).get("status", "")
                    status = status or response_status.lower()
                
                success = (
                    status in ["success", "submitted", "accepted"] or 
                    response.status_code == 200 or
                    data.get("accepted", False)
                )
                
                if success:
                    # Log message ID if available (for tracking via webhooks)
                    message_id = data.get("messageId") or data.get("id") or data.get("response", {}).get("messageId")
                    if message_id:
                        # Store directly in message_delivery_status with company name
                        company_name = item.get("company_name", "Unknown Company")
                        self._store_message_delivery_record(message_id, phone, company_name)
                        logger.info("  ✅ Message sent to %s - messageId: %s", phone, message_id)
                    sent_count += 1
                else:
                    failed_count += 1
                    error_msg = (
                        data.get("message") or 
                        data.get("error") or 
                        data.get("response", {}).get("message", "") if isinstance(data.get("response"), dict) else "" or
                        "Unknown error"
                    )
                    logger.warning("  ⚠️ Failed to send to %s: %s", phone, error_msg)
                    
            except Exception as e:
                logger.error("  ❌ Gupshup API error for %s: %s", phone, e)
                failed_count += 1
        
        logger.info(
            "  ✅ Gupshup broadcast complete: %d sent, %d failed out of %d contacts",
            sent_count, failed_count, len(phones),
        )
        return {"sent": sent_count, "failed": failed_count}

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
