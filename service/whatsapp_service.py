# service/whatsapp_service.py – Gupshup WhatsApp API integration
import os
import json
import time
import logging
import threading
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional, Sequence
from requests.adapters import HTTPAdapter
from dotenv import load_dotenv
from database import get_conn

# Fixed IST offset
IST = timezone(timedelta(hours=5, minutes=30))

load_dotenv()

logger = logging.getLogger("uvicorn.error")

# ─────────────────────────────────────────────────────────────────────
# Concurrency / HTTP tuning
# ─────────────────────────────────────────────────────────────────────
# Number of WhatsApp messages we send to Gupshup in parallel during a
# broadcast. Gupshup's WhatsApp Cloud API tolerates ~80 msg/sec on a
# verified business number; 20 keeps us comfortably under that while
# also keeping CPU/memory usage tiny on a Render Starter dyno.
WHATSAPP_PARALLELISM = int(os.getenv("WHATSAPP_PARALLELISM", "20"))
WHATSAPP_REQUEST_TIMEOUT = int(os.getenv("WHATSAPP_REQUEST_TIMEOUT", "20"))
WHATSAPP_MAX_RETRIES = int(os.getenv("WHATSAPP_MAX_RETRIES", "1"))
WHATSAPP_RETRY_BACKOFF_SEC = float(os.getenv("WHATSAPP_RETRY_BACKOFF_SEC", "1.0"))

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
# Periodic user education template (utility, approved as 'user_training')
# Params: [name_label, watchlist_companies_csv, high_impact_status_label]
USER_TRAINING_TEMPLATE_ID = "b6082566-52b3-43e4-b13a-3053db1f456b"

# RITO website URL (used in notification messages)
RITO_WEBSITE_URL = os.getenv("RITO_WEBSITE_URL", "rito.co.in")
RITO_MANAGE_URL = os.getenv("RITO_MANAGE_URL", "rito.co.in/#watchlist")


# ─────────────────────────────────────────────────────────────────────
# Module-level singletons: HTTP session (connection pooling) and a
# dedicated thread pool for parallel WhatsApp sends. Keeping these
# outside the class means they survive across all WhatsAppService()
# instances and never compete with the asyncio default thread pool
# (which serves heavier work like the BSE pipeline via asyncio.to_thread).
# ─────────────────────────────────────────────────────────────────────
def _build_gupshup_session() -> requests.Session:
    sess = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=WHATSAPP_PARALLELISM,
        pool_maxsize=WHATSAPP_PARALLELISM * 2,
        max_retries=0,  # we handle retries manually for richer logging
    )
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess


_GUPSHUP_SESSION: requests.Session = _build_gupshup_session()
_WHATSAPP_EXECUTOR: ThreadPoolExecutor = ThreadPoolExecutor(
    max_workers=WHATSAPP_PARALLELISM,
    thread_name_prefix="whatsapp-send",
)
_EXECUTOR_LOCK = threading.Lock()


def shutdown_whatsapp_executor(wait: bool = False) -> None:
    """Gracefully shut down the WhatsApp worker pool (called on app stop)."""
    with _EXECUTOR_LOCK:
        try:
            _WHATSAPP_EXECUTOR.shutdown(wait=wait, cancel_futures=not wait)
        except Exception:
            pass


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
                    
                    # Create initial record in message_delivery_status (timestamp in IST)
                    now_ist = datetime.now(IST)
                    cur.execute(
                        """
                        INSERT INTO message_delivery_status 
                            (message_id, phone, user_name, message_title, status, timestamp)
                        VALUES (%s, %s, %s, %s, 'sent', %s)
                        ON CONFLICT (message_id) DO UPDATE SET
                            user_name = COALESCE(message_delivery_status.user_name, EXCLUDED.user_name),
                            message_title = COALESCE(message_delivery_status.message_title, EXCLUDED.message_title)
                        """,
                        (message_id, phone, user_name, message_title, now_ist),
                    )
        except Exception as e:
            logger.warning("  ⚠️ Failed to store message delivery record for %s: %s", message_id, e)

    # ─────────────────────────────────────────────────────────────────
    # Internal: single-template send with retry on 429/5xx/network err
    # ─────────────────────────────────────────────────────────────────
    def _post_template_request(self, phone: str, template_obj: dict) -> tuple[bool, dict | None, int]:
        """
        Posts ONE template message to Gupshup with up to WHATSAPP_MAX_RETRIES
        extra attempts on 429 / 5xx / network errors.

        Returns (success, response_data_or_None, last_status_code).
        """
        url = f"{self.base_url}/template/msg"
        payload = {
            "channel": "whatsapp",
            "source": GUPSHUP_SOURCE_NUMBER,
            "destination": phone,
            "src.name": GUPSHUP_APP_NAME,
            "template": json.dumps(template_obj),
        }

        last_status = 0
        for attempt in range(1 + WHATSAPP_MAX_RETRIES):
            try:
                response = _GUPSHUP_SESSION.post(
                    url,
                    data=payload,
                    headers=self.headers,
                    timeout=WHATSAPP_REQUEST_TIMEOUT,
                )
            except requests.RequestException as e:
                last_status = -1
                if attempt < WHATSAPP_MAX_RETRIES:
                    time.sleep(WHATSAPP_RETRY_BACKOFF_SEC)
                    continue
                logger.warning("  ⚠️ Gupshup network error to %s: %s", phone, e)
                return False, None, last_status

            last_status = response.status_code

            # Retry on rate-limit / transient server errors
            if (response.status_code == 429 or response.status_code >= 500) and attempt < WHATSAPP_MAX_RETRIES:
                logger.info(
                    "  ↻ Gupshup %s for %s — retrying after %.1fs",
                    response.status_code, phone, WHATSAPP_RETRY_BACKOFF_SEC,
                )
                time.sleep(WHATSAPP_RETRY_BACKOFF_SEC)
                continue

            if not response.text or not response.text.strip():
                return False, None, last_status

            try:
                data = response.json()
            except ValueError:
                return response.status_code == 200, None, last_status

            status = (data.get("status") or "").lower()
            if isinstance(data.get("response"), dict):
                status = status or (data["response"].get("status") or "").lower()
            success = (
                status in ("success", "submitted", "accepted")
                or response.status_code == 200
                or data.get("accepted", False)
            )
            return success, data, last_status

        return False, None, last_status

    def _send_one_template(self, phone: str, template_obj: dict, message_title: str) -> bool:
        """
        Sends one template, records delivery status on success. This is the
        single entry point used by both single-shot calls (OTP) and the
        parallel broadcast fan-out paths.
        """
        try:
            success, data, status_code = self._post_template_request(phone, template_obj)
        except Exception as e:
            logger.error("  ❌ _send_one_template crashed for %s: %s", phone, e)
            return False

        if success and data:
            message_id = (
                data.get("messageId")
                or data.get("id")
                or (data.get("response", {}) or {}).get("messageId")
            )
            if message_id:
                try:
                    self._store_message_delivery_record(message_id, phone, message_title)
                except Exception as e:
                    logger.warning("  ⚠️ Failed to store delivery record for %s: %s", phone, e)
        elif not success:
            err = None
            if isinstance(data, dict):
                err = data.get("message") or data.get("error")
                if not err and isinstance(data.get("response"), dict):
                    err = data["response"].get("message")
            logger.warning(
                "  ⚠️ Gupshup template send failed phone=%s status=%s err=%s",
                phone, status_code, err or "unknown",
            )
        return success

    def _fan_out_template_sends(
        self,
        jobs: Sequence[tuple[str, dict, str]],
    ) -> dict:
        """
        Submits a list of (phone, template_obj, message_title) jobs to the
        dedicated WhatsApp thread pool and waits for all of them to finish.

        Returns {"sent": int, "failed": int, "total": int}.
        """
        if not jobs:
            return {"sent": 0, "failed": 0, "total": 0}

        sent = 0
        failed = 0
        futures = [
            _WHATSAPP_EXECUTOR.submit(self._send_one_template, phone, tpl, title)
            for phone, tpl, title in jobs
        ]
        for fut in as_completed(futures):
            try:
                if fut.result():
                    sent += 1
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                logger.error("  ❌ WhatsApp send future raised: %s", e)
        return {"sent": sent, "failed": failed, "total": len(jobs)}

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

    def _lookup_nse_symbol(self, item: dict) -> Optional[str]:
        """
        Best-effort NSE symbol resolution for a broadcast item.

        Order of preference:
          1. item['nse_symbol']        (set by stock_enrichment for new items)
          2. item['NSE_Symbol']        (legacy key from results.py)
          3. company_master.nse_symbol via scrip_cd  (fallback for older
             whatsapp_broadcast rows that pre-date enrichment)

        Runs at most one tiny indexed SELECT per news item (NOT per
        recipient), so it does not add any per-user overhead during fan-out.
        """
        for key in ("nse_symbol", "NSE_Symbol"):
            val = item.get(key)
            if val is None:
                continue
            val = str(val).strip()
            if val and val.lower() != "none":
                return val

        scrip = item.get("scrip_cd") or item.get("SCRIP_CD")
        if not scrip:
            return None
        try:
            scrip_int = int(str(scrip).strip())
        except (TypeError, ValueError):
            return None

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT nse_symbol FROM company_master WHERE bse_scrip_code = %s",
                        (scrip_int,),
                    )
                    row = cur.fetchone()
                    if row and row[0]:
                        sym = str(row[0]).strip()
                        return sym or None
        except Exception as e:
            logger.warning("  ⚠️ NSE symbol lookup failed for scrip=%s: %s", scrip_int, e)
        return None

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
        
        # Parameter 2: Company name. For the HIGH-IMPACT broadcast we also
        # append the NSE symbol in brackets when available (e.g.
        # "📊 *Wipro Limited (WIPRO)*") so recipients can instantly pull
        # the stock up in their trading app. Watchlist users already know
        # the symbol, so we keep that template unchanged.
        if not is_watchlist:
            nse_symbol = self._lookup_nse_symbol(item)
            if nse_symbol:
                param2 = f"📊 *{company_name} ({nse_symbol})*"
            else:
                param2 = f"📊 *{company_name}*"
        else:
            param2 = f"📊 *{company_name}*"
        
        # Parameter 3: Category and impact with color-coded emojis
        category = item.get("category", "General")
        impact_raw = (item.get("impact") or "UNKNOWN").strip().upper()
        
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
        Sends a market update to MULTIPLE users via Gupshup, fanning out
        across the dedicated WhatsApp thread pool (WHATSAPP_PARALLELISM
        concurrent requests). The asyncio event loop is NOT touched here —
        callers should invoke this from a worker thread (asyncio.to_thread)
        so the API stays responsive.

        Args:
            phones: List of phone numbers (with country code, no '+')
            item: Enriched prediction dict
            is_watchlist: True if this is a watchlist-only notification

        Returns:
            {"sent": int, "failed": int}
        """
        if not phones:
            return {"sent": 0, "failed": 0}

        params = self._build_market_update_params(item, is_watchlist)
        template_id = WATCHLIST_TEMPLATE_ID if is_watchlist else HIGH_IMPACT_TEMPLATE_ID
        template_obj = {"id": template_id, "params": params}

        # Dedupe & basic clean-up so we don't send the same person twice
        unique_phones = []
        seen = set()
        for p in phones:
            if not p:
                continue
            p = str(p).strip()
            if p and p not in seen:
                seen.add(p)
                unique_phones.append(p)

        company_name = item.get("company_name", "Unknown Company")
        logger.info(
            "  📤 Gupshup broadcast: template=%s | receivers=%d | watchlist=%s | parallelism=%d",
            template_id, len(unique_phones), is_watchlist, WHATSAPP_PARALLELISM,
        )

        t0 = time.monotonic()
        jobs = [(phone, template_obj, company_name) for phone in unique_phones]
        result = self._fan_out_template_sends(jobs)
        elapsed = time.monotonic() - t0

        logger.info(
            "  ✅ Gupshup broadcast complete: %d sent, %d failed out of %d contacts in %.2fs",
            result["sent"], result["failed"], result["total"], elapsed,
        )
        return {"sent": result["sent"], "failed": result["failed"]}

    def send_template_batch(
        self,
        jobs: Sequence[tuple[str, dict, str]],
    ) -> dict:
        """
        Parallel Gupshup template sends; each job may use different template id/params.
        Jobs are (phone, {"id": uuid, "params": [...]}, message_title).

        Returns {"sent": int, "failed": int, "total": int}.
        """
        return self._fan_out_template_sends(jobs)

    def send_user_training_message(
        self, phone: str, name_label: str, watchlist_csv: str, high_impact_label: str
    ) -> bool:
        """
        Sends ONE 'user_training' utility template message. For mass sends
        prefer `send_user_training_messages` which fans out in parallel.
        """
        template_obj = {
            "id": USER_TRAINING_TEMPLATE_ID,
            "params": [name_label, watchlist_csv, high_impact_label],
        }
        return self._send_one_template(phone, template_obj, "User Training")

    def send_user_training_messages(self, recipients: Iterable[dict]) -> dict:
        """
        Sends the 'user_training' template to many users in parallel.

        Each `recipient` dict must contain:
          - phone (str)
          - name_label (str)
          - watchlist_csv (str)
          - high_impact_label (str)

        Returns {"sent": int, "failed": int, "total": int}.
        """
        jobs: list[tuple[str, dict, str]] = []
        seen_phones: set[str] = set()
        for r in recipients:
            phone = (r.get("phone") or "").strip()
            if not phone or phone in seen_phones:
                continue
            seen_phones.add(phone)
            template_obj = {
                "id": USER_TRAINING_TEMPLATE_ID,
                "params": [
                    r.get("name_label", "user"),
                    r.get("watchlist_csv", "no stocks added yet"),
                    r.get("high_impact_label", "turned off"),
                ],
            }
            jobs.append((phone, template_obj, "User Training"))

        if not jobs:
            return {"sent": 0, "failed": 0, "total": 0}

        logger.info(
            "  📤 Gupshup user_training broadcast: receivers=%d | parallelism=%d",
            len(jobs), WHATSAPP_PARALLELISM,
        )
        t0 = time.monotonic()
        result = self._fan_out_template_sends(jobs)
        elapsed = time.monotonic() - t0
        logger.info(
            "  ✅ user_training broadcast: %d sent, %d failed out of %d in %.2fs",
            result["sent"], result["failed"], result["total"], elapsed,
        )
        return result

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
