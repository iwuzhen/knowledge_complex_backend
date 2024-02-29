from typing import Optional

from pydantic import BaseModel


class PaperIngredientDTO(BaseModel):
    """DTO for paper ingredient values."""

    mode: str  # national_academic_disciplines, national_between_countries
    flow: str  # paper, linsIn, linsOut
    countries: Optional[list[str]]
    years: Optional[list[int]]


class SubjectIngredientDTO(BaseModel):
    """DTO for paper ingredient values."""

    mode: str  # national_academic_disciplines, national_between_countries
    flow: str  # paper, linsIn, linsOut
    subjects: Optional[list[str]]
    years: Optional[list[int]]


class PaperIngredientTrendDTO(BaseModel):
    """DTO for paper ingredient values."""

    mode: str  # national_academic_disciplines, national_between_countries
    flow: str  # paper, export, import
    countries: Optional[list[str]]

    class Config:
        schema_extra = {
            "example": {
                "mode": "national_academic_disciplines",
                "flow": "paper",
                "countries": ["US", "CN"],
            },
        }


class SubjectIngredientTrendDTO(BaseModel):
    """DTO for Subject ingredient values."""

    mode: str  # national_academic_disciplines, national_between_countries
    flow: str  # Subject, export, import
    subjects: Optional[list[str]]

    class Config:
        schema_extra = {
            "example": {
                "mode": "national_academic_disciplines",
                "flow": "paper",
                "subjects": ["Chip"],
            },
        }


class PatentIngredientDTO(BaseModel):
    """DTO for paper ingredient values."""

    mode: str  # national_ipc, national_between_countries
    flow: str  # patent, linsIn, linsOut
    countries: Optional[list[str]]
    year: Optional[int]


class PatentIngredientTrendDTO(BaseModel):
    """DTO patent ingredient values."""

    mode: str  # national_ipc, national_between_countries
    flow: str  # patent, export, import
    countries: Optional[list[str]]

    class Config:
        schema_extra = {
            "example": {
                "mode": "national_ipc",
                "flow": "patent",
                "countries": ["US", "CN"],
            },
        }
