import json
import logging

from fastapi import APIRouter
from fastapi.param_functions import Depends

from knowledge_complex_backend.db.dao.site_dao import SiteDAO

# from knowledge_complex_backend.web.api.gpc.schema import \

router = APIRouter()


@router.get("/put_info_store")
async def put_info_store(
    key: str,
    data: dict,
    site_dao: SiteDAO = Depends(),
):
    await site_dao.put_info_store(key, data)
    return {"states": "ok"}


@router.get("/rm_info_store")
async def rm_info_store(
    key: str,
    data: dict,
    site_dao: SiteDAO = Depends(),
):
    await site_dao.rm_info_store(key, data)
    return {"states": "ok"}



@router.get("/all_info_store")
async def all_info_store(
    key: str,
    site_dao: SiteDAO = Depends(),
):
    ret = await site_dao.get_all_info_store(key)
    return ret
