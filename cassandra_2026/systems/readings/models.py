import math
from datetime import date, datetime
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator, ConfigDict, computed_field, AwareDatetime


class Reading(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: UUID = Field(default_factory=uuid4)
    city: str = Field(min_length=1)
    created: datetime  # AwareDatetime
    value: float = Field(ge=0.0, le=1.0)

    @property
    def day(self) -> date:
        return self.created.date()

    @property
    # @computed_field
    def value_bucket(self) -> int:
        # 10 buckets: 0→[0.0,0.1), 1→[0.1,0.2), …, 9→[0.9,1.0]
        return min(int(math.floor(self.value * 10)), 9)
