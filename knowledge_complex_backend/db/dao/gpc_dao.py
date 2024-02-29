import logging
import re
import time

from fastapi import Depends
from fastapi_cache.decorator import cache

from knowledge_complex_backend.db.dependencies import (
    get_gpc_db_pool,
    get_neo4j_driver,
    get_wikipedia_es_client,
)


def contains_chinese(s):
    if re.search("[\u4e00-\u9fa5]", s):
        return True
    else:
        return False


@cache(expire=24 * 60 * 60)
async def get_neo4j_relationships_weight(node_a, node_b, driver):
    async with driver.session(database="neo4j") as session:
        result = await session.run(
            "MATCH (start:P {Id: $source})<-[r:D]->(end:P {Id: $target})" "RETURN r",
            source=int(node_a),
            target=int(node_b),
        )
        record = await result.single()
    if record:
        return record.get("r", {}).get("weight", 1)
    return 1


class GpcDAO:
    """
    GPC 测试代码，开发要求：
    NYB:
    wiki art google距离： mysql 222:3329/gpc/    7个表：art_distance
    查询可以用union（select a.* from art_distance0 where... union select a.* from art_distance1 where ... order by distance asc limit 100）
    page表和redirect表也导入了

    做下4个查询页面
    1.给定x，找出和x最相关的topk短语
    2.给定x和y，判断x和y有没有关系（根据距离大小）
    3.找出一组短语中，和x最相似的一个
    4.给定x，语义距离D，找出和x距离小于D的所有短语

    NYB:
    结果都返回个列表展示下啊就可以了，按距离从小到大排序的列表。
    """

    def __init__(
        self,
        pool=Depends(get_gpc_db_pool),
        es=Depends(get_wikipedia_es_client),
        neo4j_driver=Depends(get_neo4j_driver),
    ):
        self.pool = pool
        self.ES = es
        self.neo4j_driver = neo4j_driver

    async def test(self):
        result = []
        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute("SELECT COUNT(*) FROM art_distance0;")
            # print(cur.description)
            (r,) = await cur.fetchone()
            logging.info(r)
            result.append(r)

        response = await self.ES.search(
            index="en_page",
            body={
                "_source": ["title", "id", "redirect"],
                "query": {
                    "match": {
                        "title": "Google",
                    },
                },
                "highlight": {
                    "fragment_size": 40,
                    "fields": {
                        "title": {},
                    },
                },
                "size": 5,
            },
        )
        ret = []
        for hit in response["hits"]["hits"]:
            doc = hit["_source"]
            doc["highlight"] = hit.get("highlight", {})
            doc["title"] = doc["title"]
            doc["id"] = doc["id"]
            ret.append(doc)
            logging.info(doc)
        result.append(ret)
        return result

    @cache(expire=60 * 60)
    async def query_suggestion_zh(self, query):
        """对于查询中包含中文字符的内容，使用中文查询"""
        logging.info("query: %s", query)
        ret_list = []

        # query from mysql
        # sql = "SELECT ll_title, ll_from FROM langlinks WHERE ll_title LIKE %s AND ll_lang = 'zh' LIMIT 10"
        # async with self.pool.acquire() as conn:
        #     cur = await conn.cursor()
        #     await cur.execute(sql, (f"{query.replace(' ','_')}%", ))
        #     for item in await cur.fetchall():
        #         ret_list.append({'token':item[0].replace('_',' '), 'id': item[1]})

        # result = []
        # for item in ret_list:
        #     if await self.check_last_token_by_page_id(item['id']):
        #         result.append(item)

        response = await self.ES.search(
            index="wikipedia_title",
            body={
                "_source": ["zh_title", "id", "redirect"],
                "query": {
                    "bool": {
                        "should": [
                            {
                                "match": {
                                    "zh_title.prefix": {"query": query, "boost": 20}
                                }
                            },
                            {
                                "match": {
                                    "zh_title.standard": {"query": query, "boost": 15}
                                }
                            },
                            {
                                "match": {
                                    "zh_title.standard": {
                                        "query": query,
                                        "fuzziness": 2,
                                    }
                                }
                            },
                        ],
                    },
                },
                "highlight": {
                    "fragment_size": 40,
                    "fields": {
                        "title": {},
                    },
                },
                "size": 15,
            },
        )

        for hit in response["hits"]["hits"]:
            doc = hit["_source"]
            doc["id"] = int(hit["_id"])
            doc["highlight"] = hit.get("highlight", {})

            doc["token"] = doc["zh_title"]
            del doc["zh_title"]
            ret_list.append(doc)

        return ret_list

    @cache(expire=60 * 60)
    async def query_suggestion(self, query):
        logging.info("query: %s", query)

        if contains_chinese(query):
            return await self.query_suggestion_zh(query)

        ret_list = []

        # query mysql page equal
        # sql = "SELECT page_title, page_id FROM page WHERE page_title = %s AND page_namespace = 0 LIMIT 10"
        # async with self.pool.acquire() as conn:
        #     cur = await conn.cursor()
        #     await cur.execute(sql, (f"{query.replace(' ','_')}", ))
        #     for item in await cur.fetchall():
        #         ret_list.append({'token':item[0].replace('_',' '), 'id': item[1]})
        #         idSet.add(item[1])

        # # query mysql redirect
        # redirect_titles = []
        # sql = "SELECT title, redirect FROM redirect WHERE title = %s AND namespace = 0 LIMIT 10"
        # async with self.pool.acquire() as conn:
        #     cur = await conn.cursor()
        #     await cur.execute(sql, (f"{query.replace(' ','_')}", ))
        #     for item in await cur.fetchall():
        #         redirect_titles.append(item[1])

        # if redirect_titles:
        #     sql = "SELECT page_title, page_id FROM page WHERE page_title IN %s AND page_namespace = 0 LIMIT 10"
        #     async with self.pool.acquire() as conn:
        #         cur = await conn.cursor()
        #         await cur.execute(sql, (redirect_titles, ))
        #         for item in await cur.fetchall():
        #             if item[1] not in idSet:
        #                 ret_list.append({'token':item[0].replace('_',' '), 'id': item[1]})
        #                 idSet.add(item[1])

        # # query from mysql
        # sql = "SELECT page_title, page_id FROM page WHERE page_title LIKE %s AND page_namespace = 0 LIMIT 10"
        # async with self.pool.acquire() as conn:
        #     cur = await conn.cursor()
        #     await cur.execute(sql, (f"{query.replace(' ','_')}%", ))
        #     for item in await cur.fetchall():
        #         if item[1] not in idSet:
        #             ret_list.append({'token':item[0].replace('_',' '), 'id': item[1]})
        #             idSet.add(item[1])

        # query from elasticsearch
        response = await self.ES.search(
            index="wikipedia_title",
            body={
                "_source": ["title", "id", "redirect"],
                "query": {
                    "bool": {
                        "should": [
                            {
                                "match": {
                                    "title.standard": {"query": query, "boost": 100}
                                }
                            },
                            {
                                "match": {
                                    "redirect.standard": {"query": query, "boost": 50}
                                }
                            },
                            {"match": {"title.prefix": {"query": query, "boost": 20}}},
                            {
                                "match": {
                                    "redirect.prefix": {"query": query, "boost": 19}
                                }
                            },
                            {
                                "match": {
                                    "title.standard": {"query": query, "fuzziness": 2}
                                }
                            },
                            {
                                "match": {
                                    "redirect.standard": {
                                        "query": query,
                                        "fuzziness": 2,
                                    }
                                }
                            },
                        ],
                    },
                },
                "highlight": {
                    "fragment_size": 40,
                    "fields": {
                        "title": {},
                    },
                },
                "size": 15,
            },
        )

        for hit in response["hits"]["hits"]:
            doc = hit["_source"]
            doc["id"] = int(hit["_id"])
            doc["highlight"] = hit.get("highlight", {})

            doc["token"] = doc["title"]
            del doc["title"]
            ret_list.append(doc)

        return ret_list

    async def get_title_id(self, title):
        """
        从 es 中查找 page id, 没有找到就返回 None"""
        response = await self.ES.search(
            index="en_page",
            body={
                "_source": ["title", "id"],
                "query": {
                    "match_phrase": {
                        "title": title,
                    },
                },
                "size": 1,
            },
        )
        if response["hits"]["hits"]:
            return response["hits"]["hits"][0]
        return None

    async def get_last_token(self, tokenID, token):
        # get token id
        sql = """
        SELECT page_id FROM page WHERE page_title = %s AND page_namespace = 0
        """
        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute(sql, (token,))
            doc = await cur.fetchone()
            if not doc:
                return []
            page_id = doc[0]
        logging.info("page id: %s", page_id)
        return await self.get_last_token_by_page_id(page_id)

    @cache(expire=60 * 60)
    async def check_last_token_by_page_id(self, page_id):
        """
        检查是否存在该 page_id 的距离"""
        logging.info("page id: %s", page_id)
        sql = f"""SELECT EXISTS (
  SELECT 1 FROM art_distance0 WHERE artA={page_id} OR artB={page_id}
  UNION ALL
  SELECT 1 FROM art_distance1 WHERE artA={page_id} OR artB={page_id}
  UNION ALL
  SELECT 1 FROM art_distance2 WHERE artA={page_id} OR artB={page_id}
  UNION ALL
  SELECT 1 FROM art_distance3 WHERE artA={page_id} OR artB={page_id}
  UNION ALL
  SELECT 1 FROM art_distance4 WHERE artA={page_id} OR artB={page_id}
  UNION ALL
  SELECT 1 FROM art_distance5 WHERE artA={page_id} OR artB={page_id}
  UNION ALL
  SELECT 1 FROM art_distance6 WHERE artA={page_id} OR artB={page_id}
) AS result
"""
        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute(sql)
            # print(cur.description)
            doc = await cur.fetchone()
            if doc:
                return True
            else:
                return False

    async def filter_zh_result(self, data_list):
        page_id_list = [doc[2] for doc in data_list]
        sql = "SELECT t.ll_from, t.ll_title FROM langlinks t WHERE t.ll_from IN %s AND t.ll_lang = 'zh'"
        name_dict = {}
        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute(sql, (page_id_list,))
            # print(cur.description)
            for row in await cur.fetchall():
                name_dict[row[0]] = row[1]
        result = []
        for doc in data_list:
            if doc[2] in name_dict:
                doc[3] = name_dict[doc[2]]
                result.append(doc)
        return result

    @cache(expire=60 * 60)
    async def get_last_token_by_page_id(
        self, title, page_id, distanceStart, distanceEnd
    ):

        logging.info("page id: %s - %s - %s", page_id, distanceStart, distanceEnd)
        # plan 2
        sql = f"""select b.artA,c.page_title as title_a,b.artB,d.page_title as title_b,b.distance from(select a.* from art_distance0 a
where a.artA={page_id} or a.artB={page_id}
UNION
select a.* from art_distance1 a
where a.artA={page_id} or a.artB={page_id}
UNION
select a.* from art_distance2 a
where a.artA={page_id} or a.artB={page_id}
UNION
select a.* from art_distance3 a
where a.artA={page_id} or a.artB={page_id}
UNION
select a.* from art_distance4 a
where a.artA={page_id} or a.artB={page_id}
UNION
select a.* from art_distance5 a
where a.artA={page_id} or a.artB={page_id}
UNION
select a.* from art_distance6 a
where a.artA={page_id} or a.artB={page_id}
) b
inner join page c on b.artA=c.page_id
inner join page d on b.artB=d.page_id
WHERE b.distance >= {distanceStart} AND b.distance <= {distanceEnd}
ORDER BY b.distance asc
limit 1000"""
        result = []
        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute(sql)
            # print(cur.description)
            query_results = await cur.fetchall()

            for doc in query_results:
                doc = list(doc)
                if int(doc[0]) != int(page_id):
                    doc[0], doc[2] = doc[2], doc[0]
                    doc[1], doc[3] = title, doc[1]
                else:
                    doc[1] = title
                result.append(doc)

        if contains_chinese(title):
            # 对外文进行一次翻译，过滤
            return await self.filter_zh_result(result)

        if result:
            return result[:100]
        # no answer
        return []
        # 反向查

    @cache(expire=60 * 60)
    async def get_two_token_by_page_id(self, page_a_id, page_b_id):

        logging.info("page id: %s,%s", page_a_id, page_b_id)

        # plan 2
        sql = f"""select b.artA,c.page_title as title_a,b.artB,d.page_title as title_b,b.distance from(select a.* from art_distance0 a
where (a.artA={page_a_id} AND a.artB={page_b_id}) OR (a.artA={page_b_id} AND a.artB={page_a_id})
UNION
select a.* from art_distance1 a
where (a.artA={page_a_id} AND a.artB={page_b_id}) OR (a.artA={page_b_id} AND a.artB={page_a_id})
UNION
select a.* from art_distance2 a
where (a.artA={page_a_id} AND a.artB={page_b_id}) OR (a.artA={page_b_id} AND a.artB={page_a_id})
UNION
select a.* from art_distance3 a
where (a.artA={page_a_id} AND a.artB={page_b_id}) OR (a.artA={page_b_id} AND a.artB={page_a_id})
UNION
select a.* from art_distance4 a
where (a.artA={page_a_id} AND a.artB={page_b_id}) OR (a.artA={page_b_id} AND a.artB={page_a_id})
UNION
select a.* from art_distance5 a
where (a.artA={page_a_id} AND a.artB={page_b_id}) OR (a.artA={page_b_id} AND a.artB={page_a_id})
UNION
select a.* from art_distance6 a
where (a.artA={page_a_id} AND a.artB={page_b_id}) OR (a.artA={page_b_id} AND a.artB={page_a_id})
) b
inner join page c on b.artA=c.page_id
inner join page d on b.artB=d.page_id
ORDER BY b.distance asc
limit 2"""
        result = []
        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute(sql)
            # print(cur.description)
            query_results = await cur.fetchall()

            for doc in query_results:
                if int(doc[0]) != int(page_a_id):
                    doc = list(doc)
                    doc[0], doc[2] = doc[2], doc[0]
                    doc[1], doc[3] = doc[3], doc[1]
                    result.append(doc)
                else:
                    result.append(doc)
        if result:
            return result
        # no answer
        return []

    @cache(expire=60 * 60)
    async def get_abxy_token_by_page_id_v0(
        self, page_a_id, page_b_id, page_x_id, floatRange
    ):
        # await self.test(page_a_id, page_b_id, page_x_id, floatRange)

        logging.info("page id: %s,%s,%s", page_a_id, page_b_id, page_x_id)

        d1 = await get_neo4j_relationships_weight(
            page_a_id, page_b_id, self.neo4j_driver
        )
        d2 = await get_neo4j_relationships_weight(
            page_a_id, page_x_id, self.neo4j_driver
        )

        # 查询交集
        # 找到集合1
        query = f"""
MATCH (a1:P)<-[r1:D]->(b1:P)
WHERE a1.Id = $a1_ID AND r1.weight >= $r1_weight_min AND r1.weight <= $r1_weight_max
WITH collect(b1) AS b1Set, a1

MATCH (a2:P)<-[r2:D]->(b2:P)
WHERE a2.Id = $a2_ID AND r2.weight >=  $r2_weight_min AND r2.weight <= $r2_weight_max
WITH b1Set, collect(b2) AS b2Set, a1,a2
WITH b1Set, b2Set, [n IN b1Set WHERE n IN b2Set] AS b3Set, a1, a2

UNWIND b3Set AS b3
MATCH (b3)<-[r3:D]->(a1)
WITH b3, r3, a2
MATCH (b3)<-[r4:D]->(a2)
RETURN b3,r3,r4,a2 LIMIT 1000
"""
        ret = []
        async with self.neo4j_driver.session(database="neo4j") as session:
            result = await session.run(
                query,
                a1_ID=int(page_b_id),
                r1_weight_min=max(d2 - floatRange, 0),
                r1_weight_max=min(d2 + floatRange, 1),
                a2_ID=int(page_x_id),
                r2_weight_min=max(d1 - floatRange, 0),
                r2_weight_max=min(d1 + floatRange, 1),
            )
            async for record in result:
                ret.append(
                    {
                        "title": record.get("b3", {}).get("Title"),
                        "weight_1": round(record.get("r3", {}).get("weight"), 3),
                        "weight_2": round(record.get("r4", {}).get("weight"), 3),
                    }
                )
        # if record:
        #     return record.get('r',{}).get('weight',1)
        # print("weight",d1,d2)
        logging.info("result count :%s", len(ret))
        return {
            "d1": round(d1, 3),
            "d2": round(d2, 3),
            "data": ret,
        }

    @cache(expire=60 * 60)
    async def get_abxy_token_by_page_id_v1(
        self, page_a_id, page_b_id, page_x_id, floatRange
    ):

        start_time = time.time()
        logging.info("page id: %s,%s,%s", page_a_id, page_b_id, page_x_id)

        d1 = await get_neo4j_relationships_weight(
            page_a_id, page_b_id, self.neo4j_driver
        )
        d2 = await get_neo4j_relationships_weight(
            page_a_id, page_x_id, self.neo4j_driver
        )

        query = """
        MATCH (a1:P)<-[r1:D]->(b1:P)
WHERE a1.Id = $a1_ID AND r1.weight >= $r1_weight_min AND r1.weight <= $r1_weight_max
WITH collect(b1) AS b1Set,collect(r1) AS r1Set, a1

MATCH (a2:P)<-[r2:D]->(b2:P)
WHERE a2.Id = $a2_ID AND r2.weight >=  $r2_weight_min AND r2.weight <= $r2_weight_max
WITH b1Set, collect(b2) AS b2Set,collect(r2) AS r2Set, a1,a2, r1Set
WITH [n IN b1Set WHERE n IN b2Set] AS b3Set, r1Set, r2Set, a1, a2
RETURN b3Set,r1Set,r2Set,a1,a2"""
        async with self.neo4j_driver.session(database="neo4j") as session:
            result = await session.run(
                query,
                a1_ID=int(page_b_id),
                r1_weight_min=max(d2 - floatRange, 0),
                r1_weight_max=min(d2 + floatRange, 1),
                a2_ID=int(page_x_id),
                r2_weight_min=max(d1 - floatRange, 0),
                r2_weight_max=min(d1 + floatRange, 1),
            )

            result_dict = {}
            async for record in result:
                a1_id = record.get("a1").element_id
                a2_id = record.get("a2").element_id
                b3Set = record.get("b3Set")
                r1Set = record.get("r1Set")
                r2Set = record.get("r2Set")

                b3_id_set = set()
                for item in b3Set:
                    b3_id_set.add(item.element_id)
                    key = item.get("Title")
                    result_dict[key] = {
                        "weight_1": 0,
                        "weight_2": 0,
                        "title": key,
                    }

                for item in r1Set:
                    nodes = item.nodes
                    if nodes[0].element_id in b3_id_set:
                        if nodes[1].element_id == a1_id:
                            result_dict[nodes[0].get("Title")]["weight_1"] = round(
                                item.get("weight"), 3
                            )
                    if nodes[1].element_id in b3_id_set:
                        if nodes[0].element_id == a1_id:
                            result_dict[nodes[1].get("Title")]["weight_1"] = round(
                                item.get("weight"), 3
                            )

                for item in r2Set:
                    nodes = item.nodes
                    if nodes[0].element_id in b3_id_set:
                        if nodes[1].element_id == a2_id:
                            result_dict[nodes[0].get("Title")]["weight_2"] = round(
                                item.get("weight"), 3
                            )
                    if nodes[1].element_id in b3_id_set:
                        if nodes[0].element_id == a2_id:
                            result_dict[nodes[1].get("Title")]["weight_2"] = round(
                                item.get("weight"), 3
                            )

            logging.info(f"Execution time: {time.time()-start_time} seconds")
            return {
                "d1": round(d1, 3),
                "d2": round(d2, 3),
                "data": list(result_dict.values()),
            }
            # logging.info('result_dict: %s',result_dict)
            # logging.info("a1: %s",a1)
            # logging.info("b3Set: %s",len(b3Set))
            #     # break
            # ret.ap*/-------
        # if record:
        #     return record.get('r',{}).get('weight',1)
        # print("weight",d1,d2)
        # logging.info("result count :%s",len(ret))
        # return {
        #     "d1":round(d1,3),
        #     "d2":round(d2,3),
        #     "data": ret
        # }

    @cache(expire=60 * 60)
    async def get_abxy_token_by_page_id_v2(
        self, page_a_id, page_b_id, page_x_id, floatRange
    ):
        start_time = time.time()
        logging.info("page id: %s,%s,%s", page_a_id, page_b_id, page_x_id)

        d1 = await get_neo4j_relationships_weight(
            page_a_id, page_b_id, self.neo4j_driver
        )
        d2 = await get_neo4j_relationships_weight(
            page_a_id, page_x_id, self.neo4j_driver
        )

        query = """MATCH (a1:P)<-[r1:D]->(b1:P)
WHERE a1.Id = $a1_ID AND r1.weight >= $r1_weight_min AND r1.weight <= $r1_weight_max
WITH collect(b1) AS b1Set,collect(r1) AS r1Set, a1

MATCH (a2:P)<-[r2:D]->(b2:P)
WHERE a2.Id = $a2_ID AND r2.weight >=  $r2_weight_min AND r2.weight <= $r2_weight_max
WITH b1Set, collect(b2) AS b2Set,collect(r2) AS r2Set, a1,a2, r1Set
WITH [n IN b1Set WHERE n IN b2Set] AS b3Set, r1Set, r2Set, a1, a2

UNWIND r1Set as r
WITH r2Set,b3Set,
CASE
  WHEN endNode(r) IN b3Set THEN {weight: r.weight,Title: endNode(r).Title}
  WHEN startNode(r) IN b3Set THEN {weight: r.weight,Title: startNode(r).Title}
END AS result1
WHERE result1 IS NOT null
WITH collect(result1) as distance_1,r2Set,b3Set

UNWIND r2Set as r
WITH distance_1,
CASE
  WHEN endNode(r) IN b3Set THEN {weight: r.weight,Title: endNode(r).Title}
  WHEN startNode(r) IN b3Set THEN {weight: r.weight,Title: startNode(r).Title}
END AS result2
WHERE result2 IS NOT null
WITH collect(result2) as distance_2,distance_1
RETURN distance_2,distance_1
"""
        async with self.neo4j_driver.session(database="neo4j") as session:
            result = await session.run(
                query,
                a1_ID=int(page_b_id),
                r1_weight_min=max(d2 - floatRange, 0),
                r1_weight_max=min(d2 + floatRange, 1),
                a2_ID=int(page_x_id),
                r2_weight_min=max(d1 - floatRange, 0),
                r2_weight_max=min(d1 + floatRange, 1),
            )

            result_dict = {}
            async for record in result:
                distance_2 = record.get("distance_2")
                distance_1 = record.get("distance_1")

                for item in distance_1:
                    key = item.get("Title")
                    result_dict[key] = {
                        "weight_1": round(item.get("weight"), 3),
                        "weight_2": 0,
                        "title": key,
                    }

                for item in distance_2:
                    key = item.get("Title")
                    if key in result_dict:
                        result_dict[key]["weight_2"] = round(item.get("weight"), 3)

            logging.info(f"Execution time: {time.time()-start_time} seconds")
            return {
                "d1": round(d1, 3),
                "d2": round(d2, 3),
                "data": list(result_dict.values()),
            }
            # logging.info('result_dict: %s',result_dict)
            # logging.info("a1: %s",a1)
            # logging.info("b3Set: %s",len(b3Set))
            #     # break
            # ret.ap*/-------
        # if record:
        #     return record.get('r',{}).get('weight',1)
        # print("weight",d1,d2)
        # logging.info("result count :%s",len(ret))
        # return {
        #     "d1":round(d1,3),
        #     "d2":round(d2,3),
        #     "data": ret
        # }

    # @cache(expire=60*60)
    # async def number_of_papers_per_year_by_country_dx(self, flow: str):
    #     """国家历年的新增的论文数,只返回 top 20 的国家"""
    #     table_name = {
    #         "paper": "artSizeByCtryAndYear",
    #         "import": "importByCtryAndYear",
    #         "export": "exportByCtryAndYear",
    #     }.get(flow,"")

    #     if not table_name:
    #         return ""

    #     start_year = 1990
    #     end_year = 2022
    #     sql = f"""
    #         SELECT country, year, SUM(count) as total_count
    #         FROM {table_name} WHERE year >= {start_year} AND year <= {end_year}
    #         GROUP BY country, year
    #         ORDER BY country, year;
    #         """
    #     data_collect = collections.defaultdict(lambda: [0]*(end_year - start_year+1))
    #     data_collect_rank = collections.defaultdict(int)
    #     async with self.pool.acquire() as conn:
    #         cur = await conn.cursor()
    #         await cur.execute(sql)
    #         # print(cur.description)
    #         query_results = await cur.fetchall()

    #         for c,y,t in query_results:
    #             data_collect[c][int(y)-start_year] = int(t)
    #             if y == str(end_year):
    #                 data_collect_rank[c] += int(t)

    #         # top20 [('US', 20099300), ('CN', 10463941), ('GB', 4937061), ('DE', 4034381),...]

    #         # name top 20
    #         # top_end_year = sorted(data_collect_rank.items(), key=lambda x:x[1], reverse=True)

    #         # header = ['year', *[year for year in range(start_year, end_year+1)]]
    #         # logging.info(data_collect)
    #         # tmp = [[v[0],*data_collect[v[0]]] for v in top20]
    #         data = []
    #         data_collect_list = [[k,v] for k,v in data_collect.items()]
    #         data_collect_list = sorted(data_collect_list, key=lambda x:data_collect_rank.get(x[0],float('-inf')), reverse=True)

    #         legend = []
    #         for k,v in data_collect_list:
    #             data.append(v)
    #             legend.append(k)

    #         array = numpy.array(data)
    #         ranks = array.shape[0] - numpy.argsort(numpy.argsort(array, axis=0), axis=0)
    #         ranks_list = ranks.tolist()
    #         return  {
    #             'legend': legend,
    #             'year': [year for year in range(start_year, end_year+1)],
    #             'rank': ranks_list,
    #             'data': data,
    #         }

    # async def paper_ingredient(self, mode: str,flow: str, countries: list[str], years: list[int]):
    #     if mode == "national_academic_disciplines":
    #         return await self.paper_ingredient_national_academic_disciplines(flow,countries,years)
    #     if mode == "national_between_countries":
    #         return await self.paper_ingredient_national_between_countries(flow,countries,years)

    # @cache(expire=60*60)
    # async def paper_ingredient_national_between_countries(self, flow: str, countries: list[str], years: list[int]):
    #     """tree map, 从国家找国家,  """
    #     table_name = {
    #         "export": "exportBetweenCtryAndYear",
    #         "import": "importBetweenCtryAndYear",
    #     }.get(flow,"")

    #     if not table_name:
    #         logging.info("unKnow flow %s", flow)
    #         return ""

    #     sql = f"""
    #         SELECT c.countryB, c.count
    #         FROM {table_name} as c
    #         WHERE c.countryA IN %s AND c.year IN %s;
    #         """
    #     result_map = collections.defaultdict(int)
    #     total = 0
    #     async with self.pool.acquire() as conn:
    #         cur = await conn.cursor()
    #         await cur.execute(sql, (countries, years))
    #         # print(cur.description)
    #         query_results = await cur.fetchall()

    #         for cat,count in query_results:
    #             total += count
    #             # result_list.append((cat, count))
    #             result_map[cat] += count

    #     return [{
    #         'name': name, 'value': value
    #     }  for name,value in result_map.items()]

    # @cache(expire=60*60)
    # async def paper_ingredient_national_academic_disciplines(self, flow: str, countries: list[str], years: list[int]):
    #     """tree map, 从国家找产品, 将原来的3级分类上升到 1,2 级分类，构建树结构，结果中不再包含3成结构
    #     关于跨学科的学科统计办法，l1内l2, 是不重复的，l0 内，对l2去重"""
    #     # 查询学科引用
    #     table_name = {
    #         "paper": "artSizeByCatAndCtryAndYear",
    #         "export": "exportByCtryAndCatAndYear",
    #         "import": "importByCtryAndCatAndYear",
    #     }.get(flow,"")

    #     if not table_name:
    #         logging.info("unKnow flow %s", flow)
    #         return ""

    #     sql = f"""
    #         SELECT c.cat, c.count
    #         FROM {table_name} as c
    #         WHERE c.country IN %s AND c.year IN %s;
    #         """
    #     result_map = collections.defaultdict(int)
    #     total = 0
    #     async with self.pool.acquire() as conn:
    #         cur = await conn.cursor()
    #         await cur.execute(sql, (countries, years))
    #         # print(cur.description)
    #         query_results = await cur.fetchall()

    #         for cat,count in query_results:
    #             total += count
    #             # result_list.append((cat, count))
    #             result_map[cat] += count

    #     result_list = [(key,value) for key,value in result_map.items()]
    #     result_list.sort(key=lambda x:-x[1])

    #     # 查询学科的父类，从1级直接到3级
    #     # sub_cat_dict = collections.defaultdict(list)
    #     # parent_cat_set = set()
    #     # sql = """
    #     #     SELECT c.cat, c.parent_cat
    #     #     FROM cat_ancestor as c
    #     #     WHERE c.cat IN %s AND c.parent_level = 0;
    #     # """
    #     # async with self.pool.acquire() as conn:
    #     #     cur = await conn.cursor()
    #     #     await cur.execute(sql, ([row[0] for row in result_list],))
    #     #     query_results = await cur.fetchall()

    #     #     for cat,par_cat in query_results:
    #     #         sub_cat_dict[cat].append(par_cat)
    #     #         parent_cat_set.add(par_cat)

    #     # result_dict = {}
    #     # for name,value in result_list:
    #     #     if parent_subject := sub_cat_dict.get(name, []):
    #     #         for parent_name in parent_subject:
    #     #             result_dict.setdefault(parent_name, {
    #     #                 'name':parent_name,
    #     #                 'value':0,
    #     #                 'children': []
    #     #             })
    #     #             result_dict[parent_name]['value'] += value
    #     #             result_dict[parent_name]['children'].append(
    #     #                 {'name':name, 'value': value}
    #     #             )

    #     # 查询学科的父类，3级不保留，留存1级和2级
    #     # level 1
    #     l1_cat_dict = collections.defaultdict(list)
    #     sql = """
    #         SELECT c.cat, c.parent_cat
    #         FROM cat_ancestor as c
    #         WHERE c.cat IN %s AND c.parent_level = 1;
    #     """
    #     async with self.pool.acquire() as conn:
    #         cur = await conn.cursor()
    #         await cur.execute(sql, ([row[0] for row in result_list],))
    #         query_results = await cur.fetchall()

    #         for cat,par_cat in query_results:
    #             l1_cat_dict[par_cat].append(cat)

    #     # level 0
    #     l0_cat_dict = collections.defaultdict(list)
    #     sql = """
    #         SELECT c.cat, c.parent_cat
    #         FROM cat_ancestor as c
    #         WHERE c.cat IN %s AND c.parent_level = 0;
    #     """
    #     async with self.pool.acquire() as conn:
    #         cur = await conn.cursor()
    #         await cur.execute(sql, ([row for row in l1_cat_dict.keys()],))
    #         query_results = await cur.fetchall()
    #         for cat,par_cat in query_results:
    #             l0_cat_dict[par_cat].append(cat)

    #     result_dict = {}
    #     for l0_name, l1_cats in l0_cat_dict.items():
    #         l1_value_set = set()
    #         result_dict.setdefault(l0_name, {
    #             'name':l0_name,
    #             'value':0,
    #             'children': []
    #         })
    #         for l1_name in l1_cats:
    #             l1_item = {'name':l1_name, 'value': 0}
    #             for l3_name in l1_cat_dict.get(l1_name,[]):
    #                 l1_item['value'] += result_map.get(l3_name, 0)
    #                 if l3_name not in l1_value_set:
    #                     result_dict[l0_name]['value'] += result_map.get(l3_name, 0)
    #                     l1_value_set.add(l3_name)

    #             result_dict[l0_name]['children'].append(l1_item)

    #     return list(result_dict.values())

    # @cache(expire=60*60)
    # async def country_eci(self):
    #     """国家的eci"""

    #     start_year = 1990
    #     end_year = 2022
    #     sql = f"""
    #     SELECT c.year, c.country, c.eci, c.rank
    #     FROM rank_eci as c
    #     WHERE c.year >= {start_year} AND c.year <= {end_year};
    #     """
    #     data_collect_rank = collections.defaultdict(lambda: [9999]*(end_year - start_year+1))
    #     data_collect_eci = collections.defaultdict(lambda: [0]*(end_year - start_year+1))
    #     data_collect_total_rank = {}
    #     async with self.pool.acquire() as conn:
    #         cur = await conn.cursor()
    #         await cur.execute(sql)
    #         # print(cur.description)
    #         query_results = await cur.fetchall()

    #         for year,country,eci,rank in query_results:
    #             data_collect_eci[country][int(year)-start_year] = round(float(eci),3)
    #             data_collect_rank[country][int(year)-start_year] = int(rank)
    #             if year == str(end_year):
    #                 data_collect_total_rank[country] = float(eci)

    #         data,eci_matrix,rank_matrix = [],[],[]
    #         for k, v in data_collect_eci.items():
    #             eci_matrix.append([k,v])
    #             rank_matrix.append([k,data_collect_rank[k]])

    #         eci_matrix = sorted(eci_matrix, key=lambda x:data_collect_total_rank.get(x[0],float('-inf')), reverse=True)
    #         rank_matrix = sorted(rank_matrix, key=lambda x:data_collect_total_rank.get(x[0],float('-inf')), reverse=True)

    #         legend = []
    #         for k,v in eci_matrix:
    #             data.append(v)
    #             legend.append(k)
    #         rank = []
    #         for k,v in rank_matrix:
    #             rank.append(v)

    #         # array = numpy.array(data)
    #         # ranks = array.shape[0] - numpy.argsort(numpy.argsort(array, axis=0), axis=0)
    #         # ranks_list = ranks.tolist()
    #         return  {
    #             'legend': legend,
    #             'year': [year for year in range(start_year, end_year+1)],
    #             'rank': rank,
    #             'data': data,
    #         }

    # @cache(expire=60*60)
    # async def subject_pci(self):
    #     """学科的pci"""

    #     start_year = 1990
    #     end_year = 2022

    #     # 获得 top 50 的所有学科，因为每年学科变动都比较大，所以这些学科需要重点关注
    #     sql = f"""
    #     SELECT c.year, c.cat, c.pci, c.rank
    #     FROM rank_pci as c
    #     WHERE c.year >= {start_year} AND c.year <= {end_year} AND c.rank < 50;
    #     """
    #     subject_map = set()
    #     async with self.pool.acquire() as conn:
    #         cur = await conn.cursor()
    #         await cur.execute(sql)
    #         # print(cur.description)
    #         query_results = await cur.fetchall()
    #         for year,subject,pci,rank in query_results:
    #             subject_map.add(subject)

    #     # 对学科进行补全

    #     sql = f"""
    #     SELECT c.year, c.cat, c.pci, c.rank
    #     FROM rank_pci as c
    #     WHERE c.year >= {start_year} AND c.year <= {end_year} AND c.cat IN %s;
    #     """
    #     data_collect_rank = collections.defaultdict(lambda: [9999]*(end_year - start_year+1))
    #     data_collect_pci = collections.defaultdict(lambda: [0]*(end_year - start_year+1))
    #     data_collect_total_rank = {}
    #     async with self.pool.acquire() as conn:
    #         cur = await conn.cursor()
    #         await cur.execute(sql, [list(subject_map),])
    #         # print(cur.description)
    #         query_results = await cur.fetchall()

    #         for year,country,pci,rank in query_results:
    #             data_collect_pci[country][int(year)-start_year] = round(float(pci),5)
    #             data_collect_rank[country][int(year)-start_year] = int(rank)
    #             if year == str(end_year):
    #                 data_collect_total_rank[country] = float(pci)

    #         data,pci_matrix,rank_matrix = [],[],[]
    #         for k, v in data_collect_pci.items():
    #             pci_matrix.append([k,v])
    #             rank_matrix.append([k,data_collect_rank[k]])

    #         pci_matrix = sorted(pci_matrix, key=lambda x:data_collect_total_rank.get(x[0],float('-inf')), reverse=True)
    #         rank_matrix = sorted(rank_matrix, key=lambda x:data_collect_total_rank.get(x[0],float('-inf')), reverse=True)

    #         legend = []
    #         for k,v in pci_matrix:
    #             data.append(v)
    #             legend.append(k)
    #         rank = []
    #         for k,v in rank_matrix:
    #             rank.append(v)

    #         # 补全缺失的 rank 和 value

    #         return  {
    #             'legend': legend,
    #             'year': [year for year in range(start_year, end_year+1)],
    #             'rank': rank,
    #             'data': data,
    #         }

    # async def country_academic_trend(self, mode: str,flow: str, countries: list[str],):
    #     # 1990-2022 年的数据趋势
    #     if mode == "national_academic_disciplines":
    #         return await self.paper_ingredient_national_academic_disciplines_trend(flow,countries)
    #     if mode == "national_between_countries":
    #         return await self.paper_ingredient_national_between_countries_trend(flow,countries)

    # @cache(expire=60*60)
    # async def paper_ingredient_national_academic_disciplines_trend(self, flow: str, countries: list[str]):
    #     # 一级学科的年度趋势
    #     """tree map, 从国家找产品, 将原来的3级分类上升到 1,2 级分类，构建树结构，结果中不再包含3成结构 """
    #     # 查询学科引用
    #     table_name = {
    #         "paper": "artSizeByCatAndCtryAndYear",
    #         "export": "exportByCtryAndCatAndYear",
    #         "import": "importByCtryAndCatAndYear",
    #     }.get(flow,"")

    #     if not table_name:
    #         logging.info("unKnow flow %s", flow)
    #         return ""

    #     sql = f"""
    #         SELECT c.cat, c.count, c.year
    #         FROM {table_name} as c
    #         WHERE c.country IN %s AND c.year IN %s;
    #         """

    #     start_year = 1990
    #     end_year = 2020
    #     year_range = [year for year in range(start_year,end_year+1)]
    #     result_map = collections.defaultdict(lambda: numpy.zeros(len(year_range), dtype=int))

    #     async with self.pool.acquire() as conn:
    #         cur = await conn.cursor()
    #         await cur.execute(sql, (countries, year_range))
    #         # print(cur.description)
    #         query_results = await cur.fetchall()

    #         for cat,count,year in query_results:
    #             result_map[cat][int(year)-start_year] += count

    #     # result_list = [(key,value) for key,value in result_map.items()]
    #     # result_list.sort(key=lambda x:-x[1][-1])

    #     # 把 lv2 映射到 lv0
    #     # level 0
    #     l0_cat_dict = collections.defaultdict(lambda: numpy.zeros(len(year_range), dtype=int))
    #     sql = """
    #         SELECT c.cat, c.parent_cat
    #         FROM cat_ancestor as c
    #         WHERE c.cat IN %s AND c.parent_level = 0;
    #     """
    #     async with self.pool.acquire() as conn:
    #         cur = await conn.cursor()
    #         await cur.execute(sql, (list(result_map.keys()),))
    #         query_results = await cur.fetchall()

    #         for cat,par_cat in query_results:
    #             if cat in result_map:
    #                 l0_cat_dict[par_cat] += result_map[cat]

    #     result_list = [(key,value) for key,value in l0_cat_dict.items()]
    #     result_list.sort(key=lambda x:-x[1][-1])

    #     legend = []
    #     data = [year_range,]
    #     for key,value in result_list:
    #         legend.append(key)
    #         data.append(value.tolist())

    #     return {'legend':legend, 'data':data}

    # @cache(expire=60*60)
    # async def paper_ingredient_national_between_countries_trend(self, flow: str, countries: list[str]):
    #     """堆叠层次图 """
    #     table_name = {
    #         "export": "exportBetweenCtryAndYear",
    #         "import": "importBetweenCtryAndYear",
    #     }.get(flow,"")

    #     if not table_name:
    #         logging.info("unKnow flow %s", flow)
    #         return ""

    #     start_year = 1990
    #     end_year = 2020
    #     year_range = [year for year in range(start_year,end_year+1)]
    #     sql = f"""
    #         SELECT c.countryB, c.count, c.year
    #         FROM {table_name} as c
    #         WHERE c.countryA IN %s AND c.year IN %s;
    #         """
    #     result_map = collections.defaultdict(lambda: numpy.zeros(len(year_range), dtype=int))

    #     async with self.pool.acquire() as conn:
    #         cur = await conn.cursor()
    #         await cur.execute(sql, (countries,year_range))
    #         # print(cur.description)
    #         query_results = await cur.fetchall()

    #         for cat,count,year in query_results:
    #             result_map[cat][int(year)-start_year] += count

    #     result_list = [(key,value) for key,value in result_map.items()]
    #     result_list.sort(key=lambda x:-x[1][-1])
    #     result_list = result_list[:20]

    #     legend = []
    #     data = [year_range,]
    #     for key,value in result_list:
    #         legend.append(key)
    #         data.append(value.tolist())

    #     return {'legend': legend, 'data': data}

    # # @cache(expire=60*60)
    # async def github_country_eci(self,filter_cat:int):
    #     """github 国家的 eci"""

    #     start_year = 2008
    #     end_year = 2022
    #     quarter_range =  [ f'{year}Q1' for year in range(start_year, end_year+1)]

    #     sql = f"""
    #     SELECT c.year, c.country, c.eci, c.rank
    #     FROM rank_github_eci as c
    #     WHERE c.year IN %s AND filter_cat=%s;
    #     """
    #     data_collect_rank = collections.defaultdict(lambda: [9999]*(end_year - start_year+1))
    #     data_collect_eci = collections.defaultdict(lambda: [0]*(end_year - start_year+1))
    #     data_collect_total_rank = {}
    #     async with self.pool.acquire() as conn:
    #         cur = await conn.cursor()
    #         await cur.execute(sql, (quarter_range,filter_cat))

    #         query_results = await cur.fetchall()
    #         for year,country,eci,rank in query_results:
    #             data_collect_eci[country][int(year[:4])-start_year] = round(float(eci),3)
    #             data_collect_rank[country][int(year[:4])-start_year] = int(rank)
    #             if year[:4] == str(end_year):
    #                 data_collect_total_rank[country] = float(eci)

    #         data,eci_matrix,rank_matrix = [],[],[]
    #         for k, v in data_collect_eci.items():
    #             eci_matrix.append([k,v])
    #             rank_matrix.append([k,data_collect_rank[k]])

    #         # eci_matrix = sorted(eci_matrix, key=lambda x:data_collect_total_rank.get(x[0],float('-inf')), reverse=True)
    #         # rank_matrix = sorted(rank_matrix, key=lambda x:data_collect_total_rank.get(x[0],float('-inf')), reverse=True)

    #         legend = []
    #         for k,v in eci_matrix:
    #             data.append(v)
    #             legend.append(k)
    #         rank = []
    #         for k,v in rank_matrix:
    #             rank.append(v)

    #         return  {
    #             'legend': legend,
    #             'year': [year for year in range(start_year, end_year+1)],
    #             'rank': rank,
    #             'data': data,
    #         }

    # # @cache(expire=60*60)
    # async def github_tag_pci(self,filter_cat:int):
    #     """学科的pci"""

    #     start_year = 2008
    #     end_year = 2022
    #     quarter_range =  [ f'{year}Q1' for year in range(start_year, end_year+1)]

    #     # 获得 top 50 的所有学科，因为每年学科变动都比较大，所以这些学科需要重点关注
    #     sql = f"""
    #     SELECT c.year, c.cat, c.pci, c.rank
    #     FROM rank_github_pci as c
    #     WHERE c.year IN %s AND c.rank < 50 AND filter_cat=%s;
    #     """
    #     subject_map = set()
    #     async with self.pool.acquire() as conn:
    #         cur = await conn.cursor()
    #         await cur.execute(sql, (quarter_range,filter_cat))
    #         # print(cur.description)
    #         query_results = await cur.fetchall()
    #         for year,subject,pci,rank in query_results:
    #             subject_map.add(subject)

    #     # 对学科进行补全

    #     sql = f"""
    #     SELECT c.year, c.cat, c.pci, c.rank
    #     FROM rank_github_pci as c
    #     WHERE c.year IN %s AND c.cat IN %s AND filter_cat=%s;
    #     """
    #     data_collect_rank = collections.defaultdict(lambda: [9999]*(end_year - start_year+1))
    #     data_collect_pci = collections.defaultdict(lambda: [0]*(end_year - start_year+1))
    #     data_collect_total_rank = {}
    #     async with self.pool.acquire() as conn:
    #         cur = await conn.cursor()
    #         await cur.execute(sql, (quarter_range,list(subject_map),filter_cat))
    #         # print(cur.description)
    #         query_results = await cur.fetchall()

    #         for year,country,pci,rank in query_results:
    #             data_collect_pci[country][int(year[:4])-start_year] = round(float(pci),5)
    #             data_collect_rank[country][int(year[:4])-start_year] = int(rank)
    #             if year[:4] == str(end_year):
    #                 data_collect_total_rank[country] = float(pci)

    #         data,pci_matrix,rank_matrix = [],[],[]
    #         for k, v in data_collect_pci.items():
    #             pci_matrix.append([k,v])
    #             rank_matrix.append([k,data_collect_rank[k]])

    #         pci_matrix = sorted(pci_matrix, key=lambda x:data_collect_total_rank.get(x[0],float('-inf')), reverse=True)
    #         rank_matrix = sorted(rank_matrix, key=lambda x:data_collect_total_rank.get(x[0],float('-inf')), reverse=True)

    #         legend = []
    #         for k,v in pci_matrix:
    #             data.append(v)
    #             legend.append(k)
    #         rank = []
    #         for k,v in rank_matrix:
    #             rank.append(v)

    #         # 补全缺失的 rank 和 value

    #         return  {
    #             'legend': legend,
    #             'year': [year for year in range(start_year, end_year+1)],
    #             'rank': rank,
    #             'data': data,
    #         }

    # async def patent_ingredient(self, mode: str,flow: str, countries: list[str], year: int):
    #     if mode == "national_ipc":
    #         return await self.patent_ingredient_national_ipc(flow,countries,year)
    #     if mode == "national_between_countries":
    #         return await self.patent_ingredient_national_between_countries(flow,countries,year)

    # @cache(expire=60*60)
    # async def patent_ingredient_national_between_countries(self, flow: str, countries: list[str], year: int):
    #     """tree map, 从国家找国家,  """

    #     # 年份校验
    #     year_range = [1990, 2020]
    #     if year < year_range[0] or year > year_range[1]:
    #         return
    #     data_index = year - 1990

    #     sql = {
    #         "export":"""
    #         SELECT c.country_code_a, c.data
    #         FROM patents_country_citations_trend as c
    #         WHERE c.country_code_b IN %s;
    #         """,
    #         "import": """
    #         SELECT c.country_code_b, c.data
    #         FROM patents_country_citations_trend as c
    #         WHERE c.country_code_a IN %s;
    #         """,
    #     }.get(flow,"")

    #     if not sql:
    #         logging.info("unKnow flow %s", flow)
    #         return ""

    #     BAN_SET = set(["WO","EP"])

    #     result_map = collections.defaultdict(int)
    #     async with self.pool.acquire() as conn:
    #         cur = await conn.cursor()
    #         await cur.execute(sql, (countries, ))
    #         query_results = await cur.fetchall()

    #         for cat,data in query_results:
    #             if cat in BAN_SET:
    #                 continue

    #             result_map[cat] += json.loads(data)[data_index]

    #     return [{
    #         'name': name, 'value': value
    #     }  for name,value in result_map.items()]

    # @cache(expire=60*60)
    # async def patent_ingredient_national_ipc(self, flow: str, countries: list[str], year: int):
    #     """tree map, 从国家 ipc 的分类的量
    #     flow: patent, linsIn, linsOut
    #     """
    #     # 年份校验
    #     year_range = [1990, 2020]
    #     IPC_PREFIX_SET = set(["A","B","C","D","E","F","G","H"])
    #     if year < year_range[0] or year > year_range[1]:
    #         return
    #     data_index = year - 1990

    #     # 查询 patent
    #     if flow == "patent":
    #         sql = f"""
    #             SELECT c.ipc_prefix, c.data
    #             FROM patents_country_ipc_trend as c
    #             WHERE c.country_code IN %s AND c.ipc_level = %s;
    #             """
    #         result_map = collections.defaultdict(int)
    #         async with self.pool.acquire() as conn:
    #             cur = await conn.cursor()
    #             await cur.execute(sql, (countries,1 ))
    #             query_results = await cur.fetchall()

    #             for ipc_prefix,data in query_results:
    #                 if ipc_prefix[0] not in IPC_PREFIX_SET:
    #                     continue
    #                 result_map[ipc_prefix] += json.loads(data)[data_index]

    #         result_dict = {}
    #         for l0_name, total in result_map.items():
    #             result_dict.setdefault(l0_name, {
    #                 'name':l0_name,
    #                 'value':total,
    #             })
    #         return list(result_dict.values())

    #     # 查询附加方向
    #     else:
    #         if flow == "export":
    #             sql = f"""
    #                 SELECT c.ipc_prefix, c.data
    #                 FROM patents_country_ipc_citations_trend as c
    #                 WHERE c.country_code_a IN %s AND c.ipc_level = %s AND c.direction = 'o';
    #                 """
    #         elif flow == "import":
    #             sql = f"""
    #                 SELECT c.ipc_prefix, c.data
    #                 FROM patents_country_ipc_citations_trend as c
    #                 WHERE c.country_code_b IN %s AND c.ipc_level = %s AND c.direction = 'i';
    #                 """

    #         result_map = collections.defaultdict(int)
    #         async with self.pool.acquire() as conn:
    #             cur = await conn.cursor()
    #             await cur.execute(sql, (countries,1))
    #             query_results = await cur.fetchall()

    #             for ipc_prefix,data in query_results:
    #                 if ipc_prefix[0] not in IPC_PREFIX_SET:
    #                     continue
    #                 result_map[ipc_prefix] += json.loads(data)[data_index]

    #         result_dict = {}
    #         for l0_name, total in result_map.items():
    #             result_dict.setdefault(l0_name, {
    #                 'name':l0_name,
    #                 'value':total,
    #             })
    #         return list(result_dict.values())

    # async def country_ipc_trend(self, mode: str,flow: str, countries: list[str],):
    #     # 1990-2022 年的数据趋势
    #     if mode == "national_ipc":
    #         return await self.patent_ingredient_national_ipc_trend(flow,countries)
    #     if mode == "national_between_countries":
    #         return await self.patent_ingredient_national_between_countries_trend(flow,countries)

    # @cache(expire=60*60)
    # async def patent_ingredient_national_ipc_trend(self, flow: str, countries: list[str]):
    #     # 一级学科的年度趋势
    #     """tree map, 从国家找产品, 将原来的3级分类上升到 1,2 级分类，构建树结构，结果中不再包含3成结构 """

    #     IPC_PREFIX_SET = set(["A","B","C","D","E","F","G","H"])
    #     year_range = [year for year in range(1990,2021)]
    #     # 查询 patent
    #     if flow == "patent":
    #         sql = f"""
    #             SELECT c.ipc_prefix, c.data
    #             FROM patents_country_ipc_trend as c
    #             WHERE c.country_code IN %s AND c.ipc_level = %s;
    #             """
    #         result_map = collections.defaultdict(lambda: numpy.zeros(2020-1990+1,dtype=int))
    #         async with self.pool.acquire() as conn:
    #             cur = await conn.cursor()
    #             await cur.execute(sql, (countries,1 ))
    #             query_results = await cur.fetchall()

    #             for ipc_prefix,data in query_results:
    #                 if ipc_prefix[0] not in IPC_PREFIX_SET:
    #                     continue
    #                 result_map[ipc_prefix] += numpy.array(json.loads(data),dtype=int)

    #     # 查询附加方向
    #     else:
    #         if flow == "export":
    #             sql = f"""
    #                 SELECT c.ipc_prefix, c.data
    #                 FROM patents_country_ipc_citations_trend as c
    #                 WHERE c.country_code_a IN %s AND c.ipc_level = %s AND c.direction = 'o';
    #                 """
    #         elif flow == "import":
    #             sql = f"""
    #                 SELECT c.ipc_prefix, c.data
    #                 FROM patents_country_ipc_citations_trend as c
    #                 WHERE c.country_code_b IN %s AND c.ipc_level = %s AND c.direction = 'i';
    #                 """

    #         result_map = collections.defaultdict(lambda: numpy.zeros(2020-1990+1,dtype=int))
    #         async with self.pool.acquire() as conn:
    #             cur = await conn.cursor()
    #             await cur.execute(sql, (countries,1))
    #             query_results = await cur.fetchall()

    #             for ipc_prefix,data in query_results:
    #                 if ipc_prefix[0] not in IPC_PREFIX_SET:
    #                     continue
    #                 result_map[ipc_prefix] += numpy.array(json.loads(data),dtype=int)

    #     result_list = [(key,value) for key,value in result_map.items()]
    #     result_list.sort(key=lambda x:-x[1][-1])

    #     legend = []
    #     data = [year_range,]
    #     for key,value in result_list:
    #         legend.append(key)
    #         data.append(value.tolist())

    #     return {'legend':legend, 'data':data}

    # @cache(expire=60*60)
    # async def patent_ingredient_national_between_countries_trend(self, flow: str, countries: list[str]):
    #     """堆叠层次图 """

    #     year_range = [year for year in range(1990,2021)]

    #     sql = {
    #         "export":"""
    #         SELECT c.country_code_a, c.data
    #         FROM patents_country_citations_trend as c
    #         WHERE c.country_code_b IN %s;
    #         """,
    #         "import": """
    #         SELECT c.country_code_b, c.data
    #         FROM patents_country_citations_trend as c
    #         WHERE c.country_code_a IN %s;
    #         """,
    #     }.get(flow,"")

    #     if not sql:
    #         logging.info("unKnow flow %s", flow)
    #         return ""

    #     BAN_SET = set(["WO","EP"])

    #     result_map = collections.defaultdict(lambda: numpy.zeros(2020-1990+1,dtype=int))
    #     async with self.pool.acquire() as conn:
    #         cur = await conn.cursor()
    #         await cur.execute(sql, (countries, ))
    #         query_results = await cur.fetchall()

    #         for cat,data in query_results:
    #             if cat in BAN_SET:
    #                 continue

    #             result_map[cat] += json.loads(data)

    #     result_list = [(key,value) for key,value in result_map.items()]
    #     result_list.sort(key=lambda x:-x[1][-1])
    #     result_list = result_list[:20]

    #     legend = []
    #     data = [year_range,]
    #     for key,value in result_list:
    #         legend.append(key)
    #         data.append(value.tolist())

    #     return {'legend': legend, 'data': data}

    # async def subject_ingredient(self, mode: str,flow: str, subjects: list[str], years: list[int]):
    #     """提供 subject， 按照引用数，依赖，被依赖，计算一个分布"""
    #     if mode == "national_academic_disciplines":
    #         return await self.paper_subject_ingredient_national_academic_disciplines(flow,subjects,years)
    #     if mode == "national_between_countries":
    #         # todo
    #         return await self.paper_subject_ingredient_national_between_countries(flow,subjects,years)

    # @cache(expire=60*60)
    # async def paper_subject_ingredient_national_academic_disciplines(self, flow: str, subjects: list[str], years: list[int]):
    #     """tree map, 从国家找产品, 将原来的3级分类上升到 1,2 级分类，构建树结构，结果中不再包含3成结构
    #     关于跨学科的学科统计办法，l1内l2, 是不重复的，l0 内，对l2去重"""
    #     # 查询学科引用
    #     table_name = {
    #         "paper": "artSizeByCatAndCtryAndYear",
    #         "export": "exportByCtryAndCatAndYear",
    #         "import": "importByCtryAndCatAndYear",
    #     }.get(flow,"")

    #     if not table_name:
    #         logging.info("unKnow flow %s", flow)
    #         return ""

    #     sql = f"""
    #         SELECT c.country, c.count
    #         FROM {table_name} as c
    #         WHERE c.cat IN %s AND c.year IN %s;
    #         """
    #     result_map = collections.defaultdict(int)

    #     async with self.pool.acquire() as conn:
    #         cur = await conn.cursor()
    #         await cur.execute(sql, (subjects, years))
    #         # print(cur.description)
    #         query_results = await cur.fetchall()
    #         for cat,count in query_results:
    #             result_map[cat] += count

    #     result_list = [(key,value) for key,value in result_map.items()]
    #     result_list.sort(key=lambda x:-x[1])

    #     result = []
    #     for name, value in result_list:
    #         result.append({
    #             'name':name,
    #             'value':value,
    #             'children': []
    #         })
    #     return result[:20]

    # @cache(expire=60*60)
    # async def paper_subject_ingredient_national_between_countries(self, flow: str, subjects: list[str], years: list[int]):
    #     """tree map, 从国家找国家,  """
    #     # todo
    #     table_name = {
    #         "export": "exportBetweenCtryAndYear",
    #         "import": "importBetweenCtryAndYear",
    #     }.get(flow,"")

    #     if not table_name:
    #         logging.info("unKnow flow %s", flow)
    #         return ""

    #     sql = f"""
    #         SELECT c.countryB, c.count
    #         FROM {table_name} as c
    #         WHERE c.countryA IN %s AND c.year IN %s;
    #         """
    #     result_map = collections.defaultdict(int)
    #     total = 0
    #     async with self.pool.acquire() as conn:
    #         cur = await conn.cursor()
    #         await cur.execute(sql, (subjects, years))
    #         # print(cur.description)
    #         query_results = await cur.fetchall()

    #         for cat,count in query_results:
    #             total += count
    #             # result_list.append((cat, count))
    #             result_map[cat] += count

    #     return [{
    #         'name': name, 'value': value
    #     }  for name,value in result_map.items()]

    # async def subject_academic_trend(self, mode: str,flow: str, subjects: list[str],):
    #     # 1990-2022 年的数据趋势
    #     if mode == "national_academic_disciplines":
    #         return await self.subject_ingredient_national_academic_disciplines_trend(flow,subjects)
    #     if mode == "national_between_countries":
    #         # todo
    #         return await self.paper_ingredient_national_between_countries_trend(flow,subjects)

    # @cache(expire=60*60)
    # async def subject_ingredient_national_academic_disciplines_trend(self, flow: str, subjects: list[str]):
    #     # 一级学科的年度趋势
    #     """tree map, 从国家找产品, 将原来的3级分类上升到 1,2 级分类，构建树结构，结果中不再包含3成结构 """
    #     # 查询学科引用
    #     table_name = {
    #         "paper": "artSizeByCatAndCtryAndYear",
    #         "export": "exportByCtryAndCatAndYear",
    #         "import": "importByCtryAndCatAndYear",
    #     }.get(flow,"")

    #     if not table_name:
    #         logging.info("unKnow flow %s", flow)
    #         return ""

    #     sql = f"""
    #         SELECT c.country, c.count, c.year
    #         FROM {table_name} as c
    #         WHERE c.cat IN %s AND c.year IN %s;
    #         """

    #     start_year = 1990
    #     end_year = 2020
    #     year_range = [year for year in range(start_year,end_year+1)]
    #     result_map = collections.defaultdict(lambda: numpy.zeros(len(year_range), dtype=int))

    #     async with self.pool.acquire() as conn:
    #         cur = await conn.cursor()
    #         await cur.execute(sql, (subjects, year_range))
    #         # print(cur.description)
    #         query_results = await cur.fetchall()

    #         for cat,count,year in query_results:
    #             result_map[cat][int(year)-start_year] += count

    #     result_list = [(key,value) for key,value in result_map.items()]
    #     result_list.sort(key=lambda x:-x[1][-1])

    #     # result_list = [(key,value) for key,value in l0_cat_dict.items()]
    #     # result_list.sort(key=lambda x:-x[1][-1])

    #     legend = []
    #     data = [year_range,]
    #     for key,value in result_list[:20]:
    #         legend.append(key)
    #         data.append(value.tolist())

    #     return {'legend':legend, 'data':data}
