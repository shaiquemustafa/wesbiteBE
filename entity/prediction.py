from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class Prediction(BaseModel):
    """
    Pydantic model for a single prediction record (final output of analysis).
    """
    Rank: int
    File: str
    PDF_Link: str
    Company: str
    SCRIP_CD: str
    Impact: str
    Summary: str
    Price_Range: str
    Rationale: str
    Impact_Score: int = 0
    Mid_percentage: Optional[float] = Field(None, alias="Mid_%")
    Category: Optional[str] = None
    News_submission_dt: Optional[datetime] = None

    class Config:
        from_attributes = True
        populate_by_name = True
        arbitrary_types_allowed = True
