from typing import List, Dict, Any, Optional
from pydantic import ValidationError, TypeAdapter
from datetime import datetime, timedelta
from psycopg2.extras import execute_values, Json

from database import get_conn
from entity.ui_data import UIDataItem


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
        # The query to find matching documents.
        find_query = {}
        params = []
        if target_date:
            # Calculate start_of_query_window: previous day 15:30:00 of target_date
            previous_day = target_date - timedelta(days=1)
            start_of_query_window = previous_day.replace(hour=15, minute=30, second=0, microsecond=0)
            #print(start_of_query_window)
            # Calculate end_of_query_window: target_date itself
            # end_of_query_window is the target_date itself (which includes time)
            end_of_query_window = target_date
            #print(end_of_query_window)

            # write a query to get news for "news_time" > start_of_query_window
            find_query = {
                # "impact" : {"$in": ["POSITIVE", "STRONGLY POSITIVE"]}
            }
            #print(find_query)
        # Find all matching documents, sort them by news_time in descending order.
        # We don't limit to 1 anymore, as "latest" now implies a time window.
        base_query = "SELECT data FROM ui_data"
        if target_date:
            base_query += " WHERE news_time >= %s"
            params.append(start_of_query_window)
        base_query += " ORDER BY news_time DESC"

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(base_query, params)
                items = [row[0] for row in cur.fetchall()]

        # Normalize and backfill fields for UI consumers
        for item in items:
            # Backfill 'category' from 'Category' or default if missing
            if 'category' not in item or item.get('category') in (None, ""):
                if 'Category' in item and item.get('Category') not in (None, ""):
                    item['category'] = item['Category']
                else:
                    item['category'] = "General Investor Info & Clarifications"
        return items