import os
from typing import List, Dict, Any, Optional, Tuple
from pydantic import ValidationError, TypeAdapter
from datetime import datetime, timedelta
from psycopg2.extras import execute_values, Json

from database import get_conn
from entity.ui_data import UIDataItem

# Keep in sync with api.py cleanup / feed behaviour.
UI_DATA_NEW_CYCLE_RELEASE_THRESHOLD = max(
    1, min(int(os.getenv("UI_DATA_NEW_CYCLE_RELEASE_THRESHOLD", "3")), 500)
)
UI_DATA_MIN_OLD_CYCLE_ROWS = max(
    0, min(int(os.getenv("UI_DATA_MIN_OLD_CYCLE_ROWS", "2")), 500)
)


def ui_data_cycle_start_ist(now_ist_naive: datetime) -> datetime:
    """
    Same lower bound as GET /ui-data/today: previous calendar day at 15:30 IST (naive).
    Used for querying the feed and for cleanup (pre-cycle vs new-cycle rows).
    """
    previous_day = now_ist_naive - timedelta(days=1)
    return previous_day.replace(hour=15, minute=30, second=0, microsecond=0)


def _normalize_for_json(value):
    if isinstance(value, dict):
        return {k: _normalize_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize_for_json(v) for v in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return value

class UIDataService:
    """
    Service class to handle database operations for UI-ready data.
    """

    def create_ui_data_document(self, data_items: List[dict], collection_name: str = "ui_data") -> List[dict]:    
        """
        Validates a list of UI data items, wraps them in a UIDataDocument,
        and inserts the single document into the specified MongoDB collection.

        Args:
            data_items: A list of dictionaries, where each dictionary represents a company's UI data.
            collection_name: The name of the MongoDB collection to insert the document into.

        Returns:
            A list of dictionaries, each containing the inserted ID and any validation errors for each item.
        """
        try:
            # Use TypeAdapter for robust list validation
            validated_items = TypeAdapter(List[UIDataItem]).validate_python(data_items)
            
            # Prepare each item for insertion
            documents_to_insert = [item.model_dump() for item in validated_items]

            if not documents_to_insert:
                return []

            rows = []
            for item in documents_to_insert:
                category = item.get("category") or item.get("Category")
                rows.append(
                    (
                        item.get("news_time"),
                        category,
                        Json(_normalize_for_json(item)),
                    )
                )

            with get_conn() as conn:
                with conn.cursor() as cur:
                    execute_values(
                        cur,
                        """
                        INSERT INTO ui_data (news_time, category, data)
                        VALUES %s
                        RETURNING id
                        """,
                        rows,
                    )
                    inserted_ids = [row[0] for row in cur.fetchall()]

            return [{"inserted_id": inserted_id, "errors": []} for inserted_id in inserted_ids]
        except ValidationError as e:
            # Handle validation errors
            return [{"inserted_id": None, "errors": e.json()}]
        except Exception as e:
            # Handle other exceptions
            return [{"inserted_id": None, "errors": str(e)}]
    def bulk_store_enriched(self, enriched_items: List[dict]) -> int:
        """
        Stores a list of enriched prediction dicts into ui_data as JSONB.
        No strict Pydantic validation — accepts any dict.
        Returns the number of rows inserted.
        """
        if not enriched_items:
            return 0

        rows = []
        for item in enriched_items:
            news_time = item.get("news_time")
            if isinstance(news_time, str):
                try:
                    news_time = datetime.fromisoformat(news_time)
                except (ValueError, TypeError):
                    news_time = None

            category = item.get("category")
            rows.append((news_time, category, Json(_normalize_for_json(item))))

        with get_conn() as conn:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO ui_data (news_time, category, data)
                    VALUES %s
                    RETURNING id
                    """,
                    rows,
                )
                inserted = cur.fetchall()

        return len(inserted)

    def get_latest_ui_data(self, target_date: datetime = None, collection_name: str = "ui_data") -> List[Dict[str, Any]]:
        """
        Fetches UI data items. If a date is provided, it fetches
        all items with 'news_time' from the previous day's 15:30:00 up to the target_date.
        If no target_date is provided, it fetches all items.

        Args:
            target_date: The specific date (and time) to define the end of the query window.
                         If None, no date filtering is applied.
            collection_name: The name of the MongoDB collection to fetch from.

        Returns:
            A list of dictionaries, each representing a UI data item, sorted by news_time descending.
            Returns an empty list if no items are found.
        """
        start_of_query_window = (
            ui_data_cycle_start_ist(target_date) if target_date else None
        )
        rows: List[Tuple[int, Any, Optional[datetime]]] = []
        with get_conn() as conn:
            with conn.cursor() as cur:
                if target_date:
                    cur.execute(
                        """
                        SELECT id, data, news_time FROM ui_data
                        WHERE news_time >= %s
                        ORDER BY news_time DESC NULLS LAST, id DESC
                        """,
                        (start_of_query_window,),
                    )
                    rows = cur.fetchall()

                    # Mirror cleanup fallback: when the current IST cycle is empty
                    # (e.g. after 15:30 boundary, before new BSE items arrive), show
                    # pre-cycle rows still in the table so the UI is not blank.
                    if not rows and UI_DATA_MIN_OLD_CYCLE_ROWS > 0:
                        cur.execute(
                            """
                            SELECT id, data, news_time FROM ui_data
                            WHERE news_time < %s OR news_time IS NULL
                            ORDER BY news_time DESC NULLS LAST, id DESC
                            """,
                            (start_of_query_window,),
                        )
                        rows = cur.fetchall()
                    elif (
                        len(rows) < UI_DATA_NEW_CYCLE_RELEASE_THRESHOLD
                        and UI_DATA_MIN_OLD_CYCLE_ROWS > 0
                    ):
                        cur.execute(
                            """
                            SELECT id, data, news_time FROM ui_data
                            WHERE news_time < %s OR news_time IS NULL
                            ORDER BY news_time DESC NULLS LAST, id DESC
                            LIMIT %s
                            """,
                            (start_of_query_window, UI_DATA_MIN_OLD_CYCLE_ROWS),
                        )
                        old_rows = cur.fetchall()
                        seen_ids = {r[0] for r in rows}
                        for old_row in old_rows:
                            if old_row[0] not in seen_ids:
                                rows.append(old_row)
                        rows.sort(
                            key=lambda r: (r[2] is not None, r[2] or datetime.min, r[0]),
                            reverse=True,
                        )
                else:
                    cur.execute(
                        """
                        SELECT id, data, news_time FROM ui_data
                        ORDER BY news_time DESC NULLS LAST, id DESC
                        """
                    )
                    rows = cur.fetchall()

        items = [row[1] for row in rows]
        item_ids = {i: row[0] for i, row in enumerate(rows)}

        # Normalize and backfill fields for UI consumers
        items_to_update = []  # Track items that need DB update
        for idx, item in enumerate(items):
            # Backfill 'category' from 'Category' or default if missing
            if 'category' not in item or item.get('category') in (None, ""):
                if 'Category' in item and item.get('Category') not in (None, ""):
                    item['category'] = item['Category']
                else:
                    item['category'] = "General Investor Info & Clarifications"
            
            # Backfill market cap if missing (on-the-fly fetch)
            if not item.get('mkt_cap_cr') and item.get('scrip_cd'):
                try:
                    from service.company_service import CompanyService
                    company_service = CompanyService()
                    scrip_int = int(item['scrip_cd'])
                    
                    # Try company_master first
                    caps = company_service.get_market_caps([scrip_int])
                    mkt_cap = caps.get(scrip_int)
                    
                    # If not found, try BSE API
                    if not mkt_cap:
                        mkt_cap = company_service.fetch_mcap_from_bse_api(scrip_int)
                        if mkt_cap:
                            # Store in company_master for future use
                            company_info = company_service.get_company(scrip_int)
                            comp_name = company_info.get("company_name", "") if company_info else item.get('company_name', '')
                            company_service.upsert_company(
                                bse_scrip_code=scrip_int,
                                company_name=comp_name,
                                mkt_cap_full=mkt_cap,
                            )
                    
                    if mkt_cap:
                        item['mkt_cap_cr'] = mkt_cap
                        # Track for batch update
                        record_id = item_ids.get(idx)
                        if record_id:
                            items_to_update.append((record_id, item))
                except (ValueError, TypeError, Exception) as e:
                    # Silently fail - don't break the response if market cap fetch fails
                    pass
        
        # Batch update database records with market cap
        if items_to_update:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    for record_id, item in items_to_update:
                        cur.execute(
                            "UPDATE ui_data SET data = %s WHERE id = %s",
                            (Json(item), record_id)
                        )
                    conn.commit()
        
        return items