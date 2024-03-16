
from fastapi import Depends

from knowledge_complex_backend.db.dependencies import get_mongodb_database


class InfoDAO:
    """
    存储网站的个性化信息
    """

    def __init__(
        self,
        mongodb_database=Depends(get_mongodb_database),
    ):
        self.mongodb_database = mongodb_database
        # await self.mongodb_database['info_store'].create_index(('key',1),background=True)

    async def put_info_store(self, key, data):
        data["key"] = key
        await self.mongodb_database["info_store"].insert_one(data)

    async def get_all_info_store(self, key):
        ret = []
        async for doc in self.mongodb_database["info_store"].find(
            {"key": key}, {"_id": 0, "key": 0},
        ):
            ret.append(doc)
        return ret

    async def rm_info_store(self, key, data):
        data["key"] = key
        await self.mongodb_database["info_store"].delete_one(data)
