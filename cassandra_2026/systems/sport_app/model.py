from pydantic import BaseModel


class UserRecords(BaseModel):
    username: str
    best_5km: float
    best_10km: float


