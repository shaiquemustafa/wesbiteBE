from datetime import datetime
from pydantic import BaseModel

class PDFData(BaseModel):
    """
    Pydantic model for storing PDF data and its associated metadata in MongoDB.
    The 'pdf_buffer' is intended to hold the raw binary content of the PDF file.
    """
    pdf_buffer: bytes
    pdf_name: str
    scrip_id: str
    news_timestamp: datetime
    pdf_url: str

    class Config:
        # Pydantic needs this to handle custom data types like 'bytes'.
        arbitrary_types_allowed = True