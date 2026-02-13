import logging
import pandas as pd
import numpy as np
from typing import List, Union
from datetime import datetime, timedelta
from pydantic import ValidationError
from psycopg2.extras import execute_values, Json

from database import get_conn
from entity.prediction import Prediction

logger = logging.getLogger("uvicorn.error")


def _normalize_for_json(value):
    """Recursively convert Python objects to JSON-safe types."""
    if isinstance(value, dict):
        return {k: _normalize_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize_for_json(v) for v in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value) if not np.isnan(value) else None
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, float) and np.isnan(value):
        return None
    return value


class AnnouncementService:
    """Service class to handle all database operations related to announcements."""

    # ------------------------------------------------------------------
    # RAW ANNOUNCEMENTS (no Pydantic – store raw BSE dicts as JSONB)
    # ------------------------------------------------------------------
    def create_announcements(self, announcements_df: pd.DataFrame, collection_name: str) -> List[dict]:
        """
        Stores raw BSE announcement rows in PostgreSQL.
        Skips strict Pydantic validation – just extracts NEWSID and
        News_submission_dt for indexed columns and stores the full row as JSONB.
        Returns a list of JSONB dicts for newly inserted rows.
        """
        rows = []
        for _, row in announcements_df.iterrows():
            record = {}
            for k, v in row.to_dict().items():
                record[k] = _normalize_for_json(v)

            newsid = str(record.get("NEWSID", ""))
            if not newsid:
                continue

            # Extract News_submission_dt for the indexed column
            news_sub_dt = record.get("News_submission_dt")
            if isinstance(news_sub_dt, str):
                try:
                    news_sub_dt = datetime.fromisoformat(news_sub_dt)
                except Exception:
                    news_sub_dt = None

            rows.append((newsid, news_sub_dt, Json(record)))

        if not rows:
            return []

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

        skipped = len(rows) - len(inserted)
        if skipped:
            logger.info("%s announcements already present in the DB and were skipped.", skipped)

        return inserted

    # ------------------------------------------------------------------
    # RAW ANNOUNCEMENTS – window query
    # ------------------------------------------------------------------
    def get_raw_announcements_by_window(
        self,
        start_dt: datetime | None,
        end_dt: datetime | None,
    ) -> pd.DataFrame:
        """Fetch raw announcements within a datetime window."""
        if not start_dt or not end_dt:
            return pd.DataFrame()

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT data
                    FROM raw_bse_announcements
                    WHERE news_submission_dt >= %s
                      AND news_submission_dt <= %s
                    """,
                    (start_dt, end_dt),
                )
                rows = [row[0] for row in cur.fetchall()]

        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # UNANALYZED announcements (for scheduler catch-up)
    # ------------------------------------------------------------------
    def get_unanalyzed_announcements(
        self,
        start_dt: datetime,
        end_dt: datetime,
        limit: int = 0,
    ) -> pd.DataFrame:
        """
        Fetch raw announcements that have NOT been analyzed yet
        within a datetime window.  Used by the scheduler to pick up
        announcements that were stored but never processed (e.g. due
        to service restarts).
        limit=0 means no limit (fetch all).
        """
        query = """
            SELECT data
            FROM raw_bse_announcements
            WHERE analyzed = FALSE
              AND news_submission_dt >= %s
              AND news_submission_dt <= %s
            ORDER BY news_submission_dt DESC
        """
        params: list = [start_dt, end_dt]
        if limit > 0:
            query += " LIMIT %s"
            params.append(limit)

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = [row[0] for row in cur.fetchall()]

        return pd.DataFrame(rows)

    def mark_as_analyzed(self, newsids: list[str]):
        """Mark a batch of announcements as analyzed (processed)."""
        if not newsids:
            return
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE raw_bse_announcements
                    SET analyzed = TRUE
                    WHERE newsid = ANY(%s)
                    """,
                    (newsids,),
                )
                updated = cur.rowcount
        return updated

    # ------------------------------------------------------------------
    # PREDICTIONS
    # ------------------------------------------------------------------
    def get_predictions_by_date(self, target_date: datetime) -> pd.DataFrame:
        """Fetch predictions for a given calendar date."""
        start_of_day = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = start_of_day + timedelta(days=1)

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT data
                    FROM predictions
                    WHERE news_submission_dt >= %s
                      AND news_submission_dt < %s
                    ORDER BY (data->>'Impact_Score')::int DESC NULLS LAST
                    """,
                    (start_of_day, end_of_day),
                )
                rows = [row[0] for row in cur.fetchall()]

        return pd.DataFrame(rows)

    def create_predictions(
        self, predictions_df: pd.DataFrame, collection_name: str, force: bool = False
    ) -> List[dict]:
        """
        Validates predictions with Pydantic, then upserts into PostgreSQL.
        When force=True, existing predictions are updated.
        """
        valid_records = []
        errors = []
        for _, row in predictions_df.iterrows():
            try:
                record_data = {k: _normalize_for_json(v) for k, v in row.to_dict().items()}
                # Normalize impact to uppercase
                if "Impact" in record_data and record_data["Impact"] is not None:
                    record_data["Impact"] = str(record_data["Impact"]).upper()
                validated = Prediction.model_validate(record_data)
                valid_records.append(validated.model_dump(by_alias=True))
            except ValidationError as e:
                errors.append({"record": row.to_dict(), "error": str(e)})
                logger.warning("Prediction validation error for SCRIP_CD=%s: %s", row.get('SCRIP_CD'), e)

        if errors:
            logger.warning("%s predictions failed validation.", len(errors))

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

        conflict_clause = (
            "ON CONFLICT (scrip_cd, pdf_link) DO UPDATE SET "
            "impact = EXCLUDED.impact, news_submission_dt = EXCLUDED.news_submission_dt, data = EXCLUDED.data"
            if force
            else "ON CONFLICT (scrip_cd, pdf_link) DO NOTHING"
        )

        with get_conn() as conn:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    f"""
                    INSERT INTO predictions (scrip_cd, pdf_link, impact, news_submission_dt, data)
                    VALUES %s
                    {conflict_clause}
                    RETURNING id
                    """,
                    rows,
                )
                inserted_ids = [row[0] for row in cur.fetchall()]

        skipped = len(rows) - len(inserted_ids)
        if skipped:
            logger.info("%s predictions already present in the DB and were skipped.", skipped)

        return [{"inserted_id": iid} for iid in inserted_ids]

    # ------------------------------------------------------------------
    # LATEST ANNOUNCEMENT
    # ------------------------------------------------------------------
    def get_latest_announcements(self) -> Union[dict, None]:
        """Fetch the single most recent raw announcement."""
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
