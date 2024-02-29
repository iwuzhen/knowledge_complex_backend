from typing import Optional

from pydantic import BaseModel


class PaperIngredientDTO(BaseModel):
    """DTO for paper ingredient values."""
    mode: str # national_academic_disciplines, national_between_countries
    flow: str # paper, linsIn, linsOut
    countries: Optional[list[str]]
    years: Optional[list[int]]
    
class PatentIngredientTrendDTO(BaseModel):
    """DTO patent ingredient values."""
    mode: str # national_ipc, national_between_countries
    flow: str # patent, export, import
    countries: Optional[list[str]]
    
    class Config:
        schema_extra = {
            "example": {
                "mode": "national_ipc",
                "flow": "patent",
                "countries": ["US", "CN"],
            }
        }