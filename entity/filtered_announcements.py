from datetime import datetime
from typing import Optional, Any
from pydantic import BaseModel, Field

class FilteredAnnouncement(BaseModel):
    """
    Pydantic model representing the structure of a daily filtered stock announcement,
    designed to be stored in MongoDB. This schema is based on the output of
    the `fetch_and_filter_announcements` function.
    """
    NEWSID: str
    SCRIP_CD: int
    XML_NAME: str
    NEWSSUB: str
    DT_TM: datetime
    NEWS_DT: datetime
    CRITICALNEWS: int
    ANNOUNCEMENT_TYPE: str
    QUARTER_ID: Optional[Any] = None
    FILESTATUS: str
    ATTACHMENTNAME: str
    MORE: Optional[Any] = None
    HEADLINE: str
    CATEGORYNAME: str
    OLD: int
    RN: int
    PDFFLAG: int
    NSURL: str
    SLONGNAME: str
    AGENDA_ID: float
    TotalPageCnt: int
    News_submission_dt: datetime
    DissemDT: datetime
    TimeDiff: str
    Fld_Attachsize: float
    SUBCATNAME: str
    AUDIO_VIDEO_FILE: Optional[Any] = None
    FinInstrmId: Optional[float] = None
    market_cap: Optional[float] = Field(None, alias="Market Cap")

    class Config:
        from_attributes = True
        populate_by_name = True
        arbitrary_types_allowed = True
