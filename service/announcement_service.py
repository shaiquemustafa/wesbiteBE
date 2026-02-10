import pandas as pd
from typing import List
from datetime import datetime, timedelta
from pydantic import ValidationError

from database import db_mongo
from entity.filtered_announcements import FilteredAnnouncement
from entity.prediction import Prediction

from typing import Union # Or Optional

class AnnouncementService:
    """
    Service class to handle all database operations related to announcements.
    """

    def get_predictions_by_date(self, target_date: datetime) -> pd.DataFrame:
        """
        Fetches prediction records from MongoDB for a specific date.

        Args:
            target_date: The date for which to fetch predictions.

        Returns:
            A pandas DataFrame containing the fetched predictions.
            Returns an empty DataFrame if no records are found.
        """
        if db_mongo.db is None:
            raise ConnectionError("Database connection is not established.")

        collection_name = "predictions"
        collection = db_mongo.db[collection_name]

        # Define the date range for the query.
        # target_date is already at the beginning of the day (00:00:00).
        start_of_day = target_date
        end_of_day = start_of_day + timedelta(days=1)

        # Construct the query to find documents where News_submission_dt is within the target day.
        query = {
            "Impact": {"$in": ["STRONGLY POSITIVE", "POSITIVE", "NEGATIVE", "STRONGLY NEGATIVE"]},
            "News_submission_dt": {"$gte": start_of_day, "$lt": end_of_day}
        }
        
        predictions_cursor = collection.find(query, {'_id': 0})
        
        # Convert cursor to a list of dictionaries, then to a DataFrame
        return pd.DataFrame(list(predictions_cursor))

    def create_predictions(self, predictions_df: pd.DataFrame, collection_name: str) -> List[dict]:
        """
        Validates a DataFrame of predictions, filters out duplicates based on SCRIP_CD
        and PDF_Link, and inserts new records into the specified MongoDB collection.

        Args:
            predictions_df: A pandas DataFrame containing the final analysis results.
            collection_name: The name of the MongoDB collection to insert data into.

        Returns:
            A list of dictionary objects representing the newly inserted predictions.
        """
        if db_mongo.db is None:
            raise ConnectionError("Database connection is not established.")

        collection = db_mongo.db[collection_name]

        # Create a unique identifier for each prediction based on SCRIP_CD and PDF_Link
        predictions_df['unique_id'] = predictions_df['SCRIP_CD'].astype(str) + '|' + predictions_df['PDF_Link']

        # Efficiently find which combinations already exist in the DB
        or_query = [
            {'SCRIP_CD': row['SCRIP_CD'], 'PDF_Link': row['PDF_Link']}
            for _, row in predictions_df.iterrows()
        ]
        if not or_query:
            return []

        existing_preds_cursor = collection.find({'$or': or_query}, {'SCRIP_CD': 1, 'PDF_Link': 1})
        existing_ids = {f"{item['SCRIP_CD']}|{item['PDF_Link']}" for item in existing_preds_cursor}

        # Filter the DataFrame to only include new predictions
        new_predictions_df = predictions_df[~predictions_df['unique_id'].isin(existing_ids)].drop(columns=['unique_id'])
        skipped_count = len(predictions_df) - len(new_predictions_df)
        if skipped_count > 0:
            print(f"{skipped_count} predictions already present in the DB and were skipped.")

        valid_records = []
        errors = []
        for _, row in new_predictions_df.iterrows():
            try:
                # Convert row to dict and handle NaN values for Pydantic validation
                record_data = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
                validated_prediction = Prediction.model_validate(record_data)
                valid_records.append(validated_prediction.model_dump(by_alias=True))
            except ValidationError as e:
                errors.append({"record": row.to_dict(), "error": str(e)})

        if not valid_records:
            return []
            
        collection.insert_many(valid_records)

        # Convert the in-place added '_id' (ObjectId) to a string for JSON serialization.
        for record in valid_records:
            record['_id'] = str(record['_id'])

        # Final check to replace any remaining NaN values with None for JSON compliance.
        for record in valid_records:
            for key, value in record.items():
                if isinstance(value, float) and pd.isna(value):
                    record[key] = None

        return valid_records

    def create_announcements(self, announcements_df: pd.DataFrame, collection_name: str) -> List[dict]:
        """
        Validates a DataFrame of announcements, filters out duplicates based on NEWSID,
        and inserts new records into the specified MongoDB collection.

        Args:
            announcements_df: A pandas DataFrame containing the announcements to be saved.
            collection_name: The name of the MongoDB collection to insert data into.

        Returns:
            A list of dictionary objects representing the newly inserted announcements.
        """
        if db_mongo.db is None:
            raise ConnectionError("Database connection is not established.")

        collection = db_mongo.db[collection_name]

        # Efficiently find which NEWSIDs from the DataFrame already exist in the DB
        incoming_news_ids = announcements_df['NEWSID'].tolist()
        existing_ids_cursor = collection.find({'NEWSID': {'$in': incoming_news_ids}}, {'NEWSID': 1})
        existing_ids = {item['NEWSID'] for item in existing_ids_cursor}

        # Filter the DataFrame to only include new announcements
        new_announcements_df = announcements_df[~announcements_df['NEWSID'].isin(existing_ids)]
        
        skipped_count = len(announcements_df) - len(new_announcements_df)
        if skipped_count > 0:
            print(f"{skipped_count} announcements already present in the DB and were skipped.")
        
        valid_records = []
        errors = []
        for _, row in new_announcements_df.iterrows():
            try:
                announcement_data = row.to_dict()
                # Pydantic expects None for Optional fields, not NaN
                record_data = {k: (None if pd.isna(v) else v) for k, v in announcement_data.items()}
                validated_announcement = FilteredAnnouncement.model_validate(record_data)
                valid_records.append(validated_announcement.model_dump(by_alias=True)) # Use model_dump for Pydantic v2
            except ValidationError as e:
                errors.append({"record": row.to_dict(), "error": str(e)})
            except Exception as e:
                errors.append({"record": row.to_dict(), "error": f"Unexpected error: {str(e)}"})

        if not valid_records:
            return []
            
        collection.insert_many(valid_records)
        #print(valid_records)
        return valid_records

    def get_latest_announcements(self) -> Union[dict, None]:
        """
        Fetches the single most recent raw announcement from the 'raw_bse_announcements' collection
        based on the 'News_submission_dt' field.

        Returns:
            A dictionary representing the latest announcement, or None if no announcements are found.
        """
        if db_mongo.db is None:
            raise ConnectionError("Database connection is not established.")

        collection = db_mongo.db["raw_bse_announcements"]

        # Sort by 'News_submission_dt' in descending order and get the first document,
        # excluding the '_id' field to prevent serialization errors.
        latest_announcement = collection.find_one(sort=[("News_submission_dt", -1)])

        # Manually handle the ObjectId to ensure the response is JSON-serializable.
        # This is more robust than relying on projection alone.
        if latest_announcement and '_id' in latest_announcement:
            # Convert ObjectId to string and remove it, or re-assign it.
            # Here, we'll just remove it as it's not typically needed in the response.
            del latest_announcement['_id']

        return latest_announcement