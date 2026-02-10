from typing import List, Dict, Any
from pydantic import ValidationError, TypeAdapter

from datetime import datetime, timedelta
from database import db_mongo
from entity.ui_data import UIDataItem
from typing import Dict, Any, Optional

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
        if db_mongo.db is None:
            raise ConnectionError("Database connection is not established.")

        collection = db_mongo.db[collection_name]

        try:
            # Use TypeAdapter for robust list validation
            validated_items = TypeAdapter(List[UIDataItem]).validate_python(data_items)
            
            # Prepare each item for insertion
            documents_to_insert = [item.model_dump() for item in validated_items]
            
            # Insert many documents in one go
            if documents_to_insert:
                result = collection.insert_many(documents_to_insert)
                inserted_ids = [str(id) for id in result.inserted_ids]
                return [{"inserted_id": id, "errors": []} for id in inserted_ids]
            else:
                return [] # Return empty list if no documents to insert
        except ValidationError as e:
            # Handle validation errors
            return [{"inserted_id": None, "errors": e.json()}]
        except Exception as e:
            # Handle other exceptions
            return [{"inserted_id": None, "errors": str(e)}]
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
        if db_mongo.db is None:
            raise ConnectionError("Database connection is not established.")

        collection = db_mongo.db[collection_name]
        
        # The query to find matching documents.
        find_query = {}

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
                "news_time": {"$gte": start_of_query_window},
                # "impact" : {"$in": ["POSITIVE", "STRONGLY POSITIVE"]}
            }
            #print(find_query)

        # Find all matching documents, sort them by news_time in descending order.
        # We don't limit to 1 anymore, as "latest" now implies a time window.
        cursor = collection.find(find_query, projection={'_id': 0}).sort("news_time", -1)

        # Normalize and backfill fields for UI consumers
        items = list(cursor)
        for item in items:
            # Backfill 'category' from 'Category' or default if missing
            if 'category' not in item or item.get('category') in (None, ""):
                if 'Category' in item and item.get('Category') not in (None, ""):
                    item['category'] = item['Category']
                else:
                    item['category'] = "General Investor Info & Clarifications"
        return items