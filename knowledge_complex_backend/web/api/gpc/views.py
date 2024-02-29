import json
import logging

from fastapi import APIRouter
from fastapi.param_functions import Depends

from knowledge_complex_backend.db.dao.gpc_dao import GpcDAO

# from knowledge_complex_backend.web.api.gpc.schema import \

router = APIRouter()


@router.get("/test")
async def test(
    gpc_dao: GpcDAO = Depends(),
):
    logging.info("gpc test info")
    return json.dumps(await gpc_dao.test())


@router.get("/suggestion")
async def suggestion(
    query: str = "",
    gpc_dao: GpcDAO = Depends(),
):
    """搜索建议"""

    return await gpc_dao.query_suggestion(query)


@router.get("/last_token")
async def last_token(
    token: str = "",
    tokenID: int = 0,
    distanceStart: float = 0,
    distanceEnd: float = 1,
    gpc_dao: GpcDAO = Depends(),
):
    """搜索最近的 token"""

    return await gpc_dao.get_last_token_by_page_id(
        token, tokenID, distanceStart, distanceEnd
    )


@router.get("/two_token")
async def two_token(
    token_a: str = "",
    token_b: str = "",
    gpc_dao: GpcDAO = Depends(),
):

    return await gpc_dao.get_two_token_by_page_id(token_a, token_b)


@router.get("/abxy_token")
async def abxy_token(
    token_a: str = "",
    token_b: str = "",
    token_x: str = "",
    floatRange: float = 0.1,
    gpc_dao: GpcDAO = Depends(),
):

    return await gpc_dao.get_abxy_token_by_page_id_v2(
        token_a, token_b, token_x, floatRange
    )


# @router.get("/number_of_papers_per_year_by_country_dx")
# async def number_of_papers_per_year_by_country_dx(
#     flow = "paper",
#     complex_dao: ComplexityDAO = Depends(),) -> None:
#     """
#     flow in str elem: release, import, export
#     """
#     return await complex_dao.number_of_papers_per_year_by_country_dx(flow)

# @router.post("/paper_ingredient")
# async def paper_ingredient_country_to_country(
#     dto: PaperIngredientDTO,
#     complex_dao: ComplexityDAO = Depends(),):
#     return await complex_dao.paper_ingredient(**dto.dict())

# @router.post("/subject_ingredient")
# async def subject_ingredient_country_to_country(
#     dto: SubjectIngredientDTO,
#     complex_dao: ComplexityDAO = Depends(),):
#     return await complex_dao.subject_ingredient(**dto.dict())

# @router.get("/country_eci")
# async def country_eci(
#     complex_dao: ComplexityDAO = Depends(),):
#     return await complex_dao.country_eci()

# @router.get("/subject_pci")
# async def subject_pci(
#     complex_dao: ComplexityDAO = Depends(),):
#     return await complex_dao.subject_pci()

# @router.post("/paper_ingredient_trend")
# async def paper_ingredient_country_to_country_trend(
#     dto: PaperIngredientTrendDTO,
#     complex_dao: ComplexityDAO = Depends(),):
#     return await complex_dao.country_academic_trend(**dto.dict())

# @router.post("/subject_ingredient_trend")
# async def subject_ingredient_country_to_country_trend(
#     dto: SubjectIngredientTrendDTO,
#     complex_dao: ComplexityDAO = Depends(),):
#     return await complex_dao.subject_academic_trend(**dto.dict())

# @router.get("/github_country_eci")
# async def github_country_eci(
#     filter_cat: int,
#     complex_dao: ComplexityDAO = Depends(),):
#     return await complex_dao.github_country_eci(filter_cat)


# @router.get("/github_tag_pci")
# async def github_tag_pci(
#     filter_cat:int,
#     complex_dao: ComplexityDAO = Depends(),):
#     return await complex_dao.github_tag_pci(filter_cat)

# @router.post("/patent_ingredient")
# async def patent_ingredient_country_to_country(
#     dto: PatentIngredientDTO,
#     complex_dao: ComplexityDAO = Depends(),):
#     return await complex_dao.patent_ingredient(**dto.dict())


# @router.post("/patent_ingredient_trend")
# async def patent_ingredient_country_to_country_trend(
#     dto: PatentIngredientTrendDTO,
#     complex_dao: ComplexityDAO = Depends(),):
#     return await complex_dao.country_ipc_trend(**dto.dict())
