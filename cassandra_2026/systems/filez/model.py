from pydantic import BaseModel
from uuid import UUID
from datetime import datetime

class StoredFile(BaseModel):
    file_id: UUID
    author_id: UUID
    filename: str
    created_at: datetime
    content: bytes