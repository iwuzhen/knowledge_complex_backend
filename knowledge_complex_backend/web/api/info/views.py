from fastapi import APIRouter
from fastapi.param_functions import Depends

from knowledge_complex_backend.db.dao.info_dao import InfoDAO
from knowledge_complex_backend.web.api.info.schema import InfoStoreDTO

router = APIRouter()


@router.post("/put_info_store")
async def put_info_store(
    dto: InfoStoreDTO,
    info_dao: InfoDAO = Depends(),
):
    await info_dao.put_info_store(dto.key, dto.data)
    return {"states": "ok"}


@router.post("/rm_info_store")
async def rm_info_store(
    dto: InfoStoreDTO,
    info_dao: InfoDAO = Depends(),
):
    await info_dao.rm_info_store(dto.key, dto.data)
    return {"states": "ok"}


@router.get("/all_info_store")
async def all_info_store(
    key: str,
    info_dao: InfoDAO = Depends(),
):
    ret = await info_dao.get_all_info_store(key)
    return ret
