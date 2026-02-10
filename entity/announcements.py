from datetime import datetime
from typing import Optional, Any
from pydantic import BaseModel, Field

class Announcement(BaseModel):
    """
    Pydantic model representing the structure of a stock announcement
    as fetched from BSE and stored in MongoDB.
    """
    NEWSID: str
    SCRIP_CD: int # Changed from str to int for better type consistency with BSE data
    XML_NAME: str
    NEWSSUB: str
    DT_TM: datetime
    NEWS_DT: datetime
    CRITICALNEWS: int
    ANNOUNCEMENT_TYPE: str
    QUARTER_ID: Optional[Any] = None # Changed from str to Any to handle potential NaN/None
    FILESTATUS: str
    ATTACHMENTNAME: str
    MORE: Optional[Any] = None # Changed from str to Any
    HEADLINE: str
    CATEGORYNAME: str
    OLD: int
    RN: int
    PDFFLAG: int
    NSURL: str
    SLONGNAME: str
    AGENDA_ID: float # Changed from int to float to handle potential 1.0 values
    TotalPageCnt: int
    News_submission_dt: datetime
    DissemDT: datetime
    TimeDiff: str
    Fld_Attachsize: float # Changed from int to float
    SUBCATNAME: str
    AUDIO_VIDEO_FILE: Optional[Any] = None # Changed from str to Any

    class Config:
        # Allow creating model from attributes and handle field aliases
        from_attributes = True
        populate_by_name = True
        # Allow arbitrary types like NaN from pandas
        arbitrary_types_allowed = True
        json_encoders = {
            datetime: lambda dt: dt.isoformat()
        }
