from datetime import datetime
from typing import Optional, Dict, Any, List, Union
from pydantic import BaseModel, Field

class UIDataItem(BaseModel):
    """
    Pydantic model for a single company's UI-ready data.
    """
    target_price_mean: Optional[Union[int, float, str]] = None
    number_of_estimate: Optional[Union[int, str]] = None
    pdf_link: str
    company_name: str
    scrip_id: str
    price_range: str
    impact: str = Field(..., alias="imapct")  # Corrected typo and added alias for mapping
    impact_score: int
    mid_percentage: float
    sales: Optional[Dict[str, float]] = None
    operating_profit: Optional[Dict[str, float]] = None
    profit_before_tax: Optional[Dict[str, float]] = None
    net_profit: Optional[Dict[str, float]] = None
    current_price_bse: Optional[Union[str, int, float]] = None
    current_price_nse: Optional[Union[str, int, float]] = None
    percentageChange: Optional[str] = None
    year_high: Optional[str] = None
    year_low: Optional[str] = None
    summary: str
    rationale: str
    marketCap: Optional[Union[str, int, float]] = None
    news_time: datetime
    myNewField: Optional[Any] = None
    category: Optional[str] = None

    class Config:
        from_attributes = True
        populate_by_name = True
        arbitrary_types_allowed = True