from typing import Optional

from pydantic import BaseModel


class InfoStoreDTO(BaseModel):
    """DTO for paper ingredient values."""

    key: str 
    data: Optional[dict]

