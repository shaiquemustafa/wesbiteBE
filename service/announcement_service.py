import pandas as pd
from typing import List, Union
from datetime import datetime, timedelta
from pydantic import ValidationError
from psycopg2.extras import execute_values, Json

from database import get_conn
from entity.filtered_announcements import FilteredAnnouncement
from entity.prediction import Prediction


def _normalize_for_json(value):
    if isinstance(value, dict):
        return {k: _normalize_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize_for_json(v) for v in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return value

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
        # Define the date range for the query.
        # target_date is already at the beginning of the day (00:00:00).
        start_of_day = target_date
        end_of_day = start_of_day + timedelta(days=1)

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT data
                    FROM predictions
                    WHERE impact = ANY(%s)
                      AND news_submission_dt >= %s
                      AND news_submission_dt < %s
                    """,
                    (["STRONGLY POSITIVE", "POSITIVE", "NEGATIVE", "STRONGLY NEGATIVE"], start_of_day, end_of_day),
                )
                rows = [row[0] for row in cur.fetchall()]

        return pd.DataFrame(rows)

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
        valid_records = []
        errors = []
        for _, row in predictions_df.iterrows():
            try:
                # Convert row to dict and handle NaN values for Pydantic validation
                record_data = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
                validated_prediction = Prediction.model_validate(record_data)
                valid_records.append(validated_prediction.model_dump(by_alias=True))
            except ValidationError as e:
                errors.append({"record": row.to_dict(), "error": str(e)})

        if not valid_records:
            return []

        rows = []
        for record in valid_records:
            rows.append(
                (
                    str(record.get("SCRIP_CD")),
                    record.get("PDF_Link"),
                    record.get("Impact"),
                    record.get("News_submission_dt"),
                    Json(_normalize_for_json(record)),
                )
            )

        with get_conn() as conn:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO predictions (scrip_cd, pdf_link, impact, news_submission_dt, data)
                    VALUES %s
                    ON CONFLICT (scrip_cd, pdf_link) DO NOTHING
                    RETURNING id
                    """,
                    rows,
                )
                inserted_ids = [row[0] for row in cur.fetchall()]

        if len(inserted_ids) < len(rows):
            skipped_count = len(rows) - len(inserted_ids)
            print(f"{skipped_count} predictions already present in the DB and were skipped.")

        return [{"inserted_id": inserted_id} for inserted_id in inserted_ids]

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
        valid_records = []
        errors = []
        for _, row in announcements_df.iterrows():
            try:
                announcement_data = row.to_dict()
                # Pydantic expects None for Optional fields, not NaN
                record_data = {k: (None if pd.isna(v) else v) for k, v in announcement_data.items()}
                validated_announcement = FilteredAnnouncement.model_validate(record_data)
                valid_records.append(validated_announcement.model_dump(by_alias=True))
            except ValidationError as e:
                errors.append({"record": row.to_dict(), "error": str(e)})
            except Exception as e:
                errors.append({"record": row.to_dict(), "error": f"Unexpected error: {str(e)}"})

        if not valid_records:
            return []

        rows = []
        for record in valid_records:
            rows.append(
                (
                    record.get("NEWSID"),
                    record.get("News_submission_dt"),
                    Json(_normalize_for_json(record)),
                )
            )

        with get_conn() as conn:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO raw_bse_announcements (newsid, news_submission_dt, data)
                    VALUES %s
                    ON CONFLICT (newsid) DO NOTHING
                    RETURNING data
                    """,
                    rows,
                )
                inserted = [row[0] for row in cur.fetchall()]

        if len(inserted) < len(rows):
            skipped_count = len(rows) - len(inserted)
            print(f"{skipped_count} announcements already present in the DB and were skipped.")

        return inserted

    def get_latest_announcements(self) -> Union[dict, None]:
        """
        Fetches the single most recent raw announcement from the 'raw_bse_announcements' collection
        based on the 'News_submission_dt' field.

        Returns:
            A dictionary representing the latest announcement, or None if no announcements are found.
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT data
                    FROM raw_bse_announcements
                    ORDER BY news_submission_dt DESC NULLS LAST
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
        return row[0] if row else None