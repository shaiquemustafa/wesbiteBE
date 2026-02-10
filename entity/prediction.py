from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field

class Prediction(BaseModel):
    """
    Pydantic model representing the structure of a single prediction record,
    which is the final output of the analysis pipeline.
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
    Impact_Score: int
    Mid_percentage: float = Field(..., alias="Mid_%")
    Category: str
    News_submission_dt: Optional[datetime] = None

    class Config:
        # Allow creating model from DataFrame rows and handle field aliases
        from_attributes = True
        populate_by_name = True
        arbitrary_types_allowed = True