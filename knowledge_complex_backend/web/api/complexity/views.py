from fastapi import APIRouter
import logging
from knowledge_complex_backend.db.dao.complexity_dao import ComplexityDAO
from fastapi.param_functions import Depends
from knowledge_complex_backend.web.api.complexity.schema import \
PaperIngredientDTO,PaperIngredientTrendDTO,PatentIngredientDTO, PatentIngredientTrendDTO,SubjectIngredientDTO,SubjectIngredientTrendDTO

router = APIRouter()

@router.get("/test")
async def test(
    complex_dao: ComplexityDAO = Depends(),) -> None:
    """
    Checks the health of a project.

    It returns 200 if the project is healthy.
    """
    return await complex_dao.test()

@router.get("/number_of_papers_per_year_by_country_dx")
async def number_of_papers_per_year_by_country_dx(
    flow = "paper",
    complex_dao: ComplexityDAO = Depends(),) -> None:
    """
    flow in str elem: release, import, export
    """
    return await complex_dao.number_of_papers_per_year_by_country_dx(flow)

@router.post("/paper_ingredient")
async def paper_ingredient_country_to_country(
    dto: PaperIngredientDTO,
    complex_dao: ComplexityDAO = Depends(),):
    return await complex_dao.paper_ingredient(**dto.dict())

@router.post("/subject_ingredient")
async def subject_ingredient_country_to_country(
    dto: SubjectIngredientDTO,
    complex_dao: ComplexityDAO = Depends(),):
    return await complex_dao.subject_ingredient(**dto.dict())

@router.get("/country_eci")
async def country_eci(
    complex_dao: ComplexityDAO = Depends(),):
    return await complex_dao.country_eci()

@router.get("/subject_pci")
async def subject_pci(
    complex_dao: ComplexityDAO = Depends(),):
    return await complex_dao.subject_pci()

@router.post("/paper_ingredient_trend")
async def paper_ingredient_country_to_country_trend(
    dto: PaperIngredientTrendDTO,
    complex_dao: ComplexityDAO = Depends(),):
    return await complex_dao.country_academic_trend(**dto.dict())

@router.post("/subject_ingredient_trend")
async def subject_ingredient_country_to_country_trend(
    dto: SubjectIngredientTrendDTO,
    complex_dao: ComplexityDAO = Depends(),):
    return await complex_dao.subject_academic_trend(**dto.dict())

@router.get("/github_country_eci")
async def github_country_eci(
    filter_cat: int,
    complex_dao: ComplexityDAO = Depends(),):
    return await complex_dao.github_country_eci(filter_cat)


@router.get("/github_tag_pci")
async def github_tag_pci(
    filter_cat:int,
    complex_dao: ComplexityDAO = Depends(),):
    return await complex_dao.github_tag_pci(filter_cat)

@router.post("/patent_ingredient")
async def patent_ingredient_country_to_country(
    dto: PatentIngredientDTO,
    complex_dao: ComplexityDAO = Depends(),):
    return await complex_dao.patent_ingredient(**dto.dict())


@router.post("/patent_ingredient_trend")
async def patent_ingredient_country_to_country_trend(
    dto: PatentIngredientTrendDTO,
    complex_dao: ComplexityDAO = Depends(),):
    return await complex_dao.country_ipc_trend(**dto.dict())
