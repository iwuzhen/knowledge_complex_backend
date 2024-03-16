import logging
import re
import time

from fastapi import Depends
from fastapi_cache.decorator import cache

from knowledge_complex_backend.db.dependencies import (  # get_gpc_db_pool,
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
        # pool=Depends(get_gpc_db_pool),
        es=Depends(get_wikipedia_es_client),
        neo4j_driver=Depends(get_neo4j_driver),
    ):
        # self.pool = pool
        self.ES = es
        self.neo4j_driver = neo4j_driver

    # async def test(self):
    #     result = []
    #     async with self.pool.acquire() as conn:
    #         cur = await conn.cursor()
    #         await cur.execute("SELECT COUNT(*) FROM art_distance0;")
    #         (r,) = await cur.fetchone()
    #         logging.info(r)
    #         result.append(r)

    #     response = await self.ES.search(
    #         index="en_page",
    #         body={
    #             "_source": ["title", "id", "redirect"],
    #             "query": {
    #                 "match": {
    #                     "title": "Google",
    #                 },
    #             },
    #             "highlight": {
    #                 "fragment_size": 40,
    #                 "fields": {
    #                     "title": {},
    #                 },
    #             },
    #             "size": 5,
    #         },
    #     )
    #     ret = []
    #     for hit in response["hits"]["hits"]:
    #         doc = hit["_source"]
    #         doc["highlight"] = hit.get("highlight", {})
    #         doc["title"] = doc["title"]
    #         doc["id"] = doc["id"]
    #         ret.append(doc)
    #         logging.info(doc)
    #     result.append(ret)
    #     return result

    @cache(expire=60 * 60)
    async def query_suggestion_zh(self, query):
        """对于查询中包含中文字符的内容，使用中文查询"""
        logging.info("query: %s", query)
        ret_list = []

        response = await self.ES.search(
            index="wikipedia_title",
            body={
                "_source": ["zh_title", "id", "redirect"],
                "query": {
                    "bool": {
                        "should": [
                            {
                                "match": {
                                    "zh_title.prefix": {"query": query, "boost": 20},
                                },
                            },
                            {
                                "match": {
                                    "zh_title.standard": {"query": query, "boost": 15},
                                },
                            },
                            {
                                "match": {
                                    "zh_title.standard": {
                                        "query": query,
                                        "fuzziness": 2,
                                    },
                                },
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
                                    "title.standard": {"query": query, "boost": 100},
                                },
                            },
                            {
                                "match": {
                                    "redirect.standard": {"query": query, "boost": 5},
                                },
                            },
                            {"match": {"title.prefix": {"query": query, "boost": 20}}},
                            {
                                "match": {
                                    "redirect.prefix": {"query": query, "boost": 4},
                                },
                            },
                            {
                                "match": {
                                    "title.standard": {"query": query, "fuzziness": 2},
                                },
                            },
                            {
                                "match": {
                                    "redirect.standard": {
                                        "query": query,
                                        "fuzziness": 2,
                                    },
                                },
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

    #     @cache(expire=60 * 60)
    #     async def check_last_token_by_page_id(self, page_id):
    #         """
    #         检查是否存在该 page_id 的距离"""
    #         logging.info("page id: %s", page_id)
    #         sql = f"""SELECT EXISTS (
    #   SELECT 1 FROM art_distance0 WHERE artA={page_id} OR artB={page_id}
    #   UNION ALL
    #   SELECT 1 FROM art_distance1 WHERE artA={page_id} OR artB={page_id}
    #   UNION ALL
    #   SELECT 1 FROM art_distance2 WHERE artA={page_id} OR artB={page_id}
    #   UNION ALL
    #   SELECT 1 FROM art_distance3 WHERE artA={page_id} OR artB={page_id}
    #   UNION ALL
    #   SELECT 1 FROM art_distance4 WHERE artA={page_id} OR artB={page_id}
    #   UNION ALL
    #   SELECT 1 FROM art_distance5 WHERE artA={page_id} OR artB={page_id}
    #   UNION ALL
    #   SELECT 1 FROM art_distance6 WHERE artA={page_id} OR artB={page_id}
    # ) AS result
    # """
    #         async with self.pool.acquire() as conn:
    #             cur = await conn.cursor()
    #             await cur.execute(sql)
    #             # print(cur.description)
    #             doc = await cur.fetchone()
    #             if doc:
    #                 return True
    #             else:
    #                 return False

    # async def filter_zh_result(self, data_list):
    #     page_id_list = [doc[2] for doc in data_list]
    #     sql = "SELECT t.ll_from, t.ll_title FROM langlinks t WHERE t.ll_from IN %s AND t.ll_lang = 'zh'"
    #     name_dict = {}
    #     async with self.pool.acquire() as conn:
    #         cur = await conn.cursor()
    #         await cur.execute(sql, (page_id_list,))
    #         # print(cur.description)
    #         for row in await cur.fetchall():
    #             name_dict[row[0]] = row[1]
    #     result = []
    #     for doc in data_list:
    #         if doc[2] in name_dict:
    #             doc[3] = name_dict[doc[2]]
    #             result.append(doc)
    #     return result

    @cache(expire=60 * 60)
    async def get_last_token_by_page_id(
        self,
        title,
        page_id,
        distanceStart,
        distanceEnd,
    ):
        # await self.test(page_a_id, page_b_id, page_x_id, floatRange)

        logging.info(
            "get_last_token_by_page_id: %s,%s,%s,%s",
            title,
            page_id,
            distanceStart,
            distanceEnd,
        )

        zh_flag = False
        if contains_chinese(title):
            zh_flag = True

        # 查询交集
        # 找到集合1
        query = """
MATCH (start:P {Id: $startID})<-[r:D]->(end:P)
WHERE r.weight >= $distanceStart AND r.weight <= $distanceEnd
RETURN r,end
ORDER BY r.weight ASC;
"""
        result_last_token_list = []
        async with self.neo4j_driver.session(database="neo4j") as session:
            result = await session.run(
                query,
                startID=int(page_id),
                distanceStart=distanceStart,
                distanceEnd=distanceEnd,
            )
            async for record in result:
                node_id = record.get("end", {}).get("Id")
                if zh_flag:
                    node_title = record.get("end", {}).get("zh_Title")
                    if not node_title:
                        node_title = record.get("end", {}).get("Title")
                else:
                    node_title = record.get("end", {}).get("Title")

                weight = round(record.get("r", {}).get("weight"), 3)
                doc = [page_id, title, node_id, node_title, weight]
                result_last_token_list.append(doc)

        logging.info("result count :%s", len(result_last_token_list))
        return result_last_token_list

    #         logging.info("page id: %s - %s - %s", page_id, distanceStart, distanceEnd)
    #         # plan 2
    #         sql = f"""select b.artA,c.page_title as title_a,b.artB,d.page_title as title_b,b.distance from(select a.* from art_distance0 a
    # where a.artA={page_id} or a.artB={page_id}
    # UNION
    # select a.* from art_distance1 a
    # where a.artA={page_id} or a.artB={page_id}
    # UNION
    # select a.* from art_distance2 a
    # where a.artA={page_id} or a.artB={page_id}
    # UNION
    # select a.* from art_distance3 a
    # where a.artA={page_id} or a.artB={page_id}
    # UNION
    # select a.* from art_distance4 a
    # where a.artA={page_id} or a.artB={page_id}
    # UNION
    # select a.* from art_distance5 a
    # where a.artA={page_id} or a.artB={page_id}
    # UNION
    # select a.* from art_distance6 a
    # where a.artA={page_id} or a.artB={page_id}
    # ) b
    # inner join page c on b.artA=c.page_id
    # inner join page d on b.artB=d.page_id
    # WHERE b.distance >= {distanceStart} AND b.distance <= {distanceEnd}
    # ORDER BY b.distance asc
    # limit 1000"""
    #         result = []
    #         async with self.pool.acquire() as conn:
    #             cur = await conn.cursor()
    #             await cur.execute(sql)
    #             # print(cur.description)
    #             query_results = await cur.fetchall()

    #             for doc in query_results:
    #                 doc = list(doc)
    #                 if int(doc[0]) != int(page_id):
    #                     doc[0], doc[2] = doc[2], doc[0]
    #                     doc[1], doc[3] = title, doc[1]
    #                 else:
    #                     doc[1] = title
    #                 result.append(doc)

    #         if contains_chinese(title):
    #             # 对外文进行一次翻译，过滤
    #             return await self.filter_zh_result(result)

    #         if result:
    #             return result[:100]
    #         # no answer
    #         return []
    #         # 反向查

    @cache(expire=60 * 60)
    async def get_two_token_by_page_id(self, page_a_id, page_b_id):

        logging.info("get_two_token_by_page_id: %s,%s", page_a_id, page_b_id)

        # 查询交集
        # 找到集合1
        query = """
MATCH (start:P {Id: $startID})<-[r:D]->(end:P {Id: $endID})
RETURN r,end,start
"""
        result_last_token_list = []
        async with self.neo4j_driver.session(database="neo4j") as session:
            result = await session.run(
                query,
                startID=int(page_a_id),
                endID=int(page_b_id),
            )
            async for record in result:
                start_node_id = record.get("start", {}).get("Id")
                start_node_title = record.get("start", {}).get("Title")

                end_node_id = record.get("end", {}).get("Id")
                end_node_title = record.get("end", {}).get("Title")

                weight = round(record.get("r", {}).get("weight"), 3)
                doc = [
                    start_node_id,
                    start_node_title,
                    end_node_id,
                    end_node_title,
                    weight,
                ]
                result_last_token_list.append(doc)

        logging.info("result count :%s", len(result_last_token_list))
        return result_last_token_list

    #         # plan 2
    #         sql = f"""select b.artA,c.page_title as title_a,b.artB,d.page_title as title_b,b.distance from(select a.* from art_distance0 a
    # where (a.artA={page_a_id} AND a.artB={page_b_id}) OR (a.artA={page_b_id} AND a.artB={page_a_id})
    # UNION
    # select a.* from art_distance1 a
    # where (a.artA={page_a_id} AND a.artB={page_b_id}) OR (a.artA={page_b_id} AND a.artB={page_a_id})
    # UNION
    # select a.* from art_distance2 a
    # where (a.artA={page_a_id} AND a.artB={page_b_id}) OR (a.artA={page_b_id} AND a.artB={page_a_id})
    # UNION
    # select a.* from art_distance3 a
    # where (a.artA={page_a_id} AND a.artB={page_b_id}) OR (a.artA={page_b_id} AND a.artB={page_a_id})
    # UNION
    # select a.* from art_distance4 a
    # where (a.artA={page_a_id} AND a.artB={page_b_id}) OR (a.artA={page_b_id} AND a.artB={page_a_id})
    # UNION
    # select a.* from art_distance5 a
    # where (a.artA={page_a_id} AND a.artB={page_b_id}) OR (a.artA={page_b_id} AND a.artB={page_a_id})
    # UNION
    # select a.* from art_distance6 a
    # where (a.artA={page_a_id} AND a.artB={page_b_id}) OR (a.artA={page_b_id} AND a.artB={page_a_id})
    # ) b
    # inner join page c on b.artA=c.page_id
    # inner join page d on b.artB=d.page_id
    # ORDER BY b.distance asc
    # limit 2"""
    #         result = []
    #         async with self.pool.acquire() as conn:
    #             cur = await conn.cursor()
    #             await cur.execute(sql)
    #             # print(cur.description)
    #             query_results = await cur.fetchall()

    #             for doc in query_results:
    #                 if int(doc[0]) != int(page_a_id):
    #                     doc = list(doc)
    #                     doc[0], doc[2] = doc[2], doc[0]
    #                     doc[1], doc[3] = doc[3], doc[1]
    #                     result.append(doc)
    #                 else:
    #                     result.append(doc)
    #         if result:
    #             return result
    #         # no answer
    #         return []

    @cache(expire=60 * 60)
    async def get_abxy_token_by_page_id_v0(
        self,
        page_a_id,
        page_b_id,
        page_x_id,
        floatRange,
    ):
        # await self.test(page_a_id, page_b_id, page_x_id, floatRange)

        logging.info("page id: %s,%s,%s", page_a_id, page_b_id, page_x_id)

        d1 = await get_neo4j_relationships_weight(
            page_a_id,
            page_b_id,
            self.neo4j_driver,
        )
        d2 = await get_neo4j_relationships_weight(
            page_a_id,
            page_x_id,
            self.neo4j_driver,
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
                    },
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
        self,
        page_a_id,
        page_b_id,
        page_x_id,
        floatRange,
    ):

        start_time = time.time()
        logging.info("page id: %s,%s,%s", page_a_id, page_b_id, page_x_id)

        d1 = await get_neo4j_relationships_weight(
            page_a_id,
            page_b_id,
            self.neo4j_driver,
        )
        d2 = await get_neo4j_relationships_weight(
            page_a_id,
            page_x_id,
            self.neo4j_driver,
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
                r1_weight_max=min(d2 + floatRange, 0.9999),
                a2_ID=int(page_x_id),
                r2_weight_min=max(d1 - floatRange, 0),
                r2_weight_max=min(d1 + floatRange, 0.9999),
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
                                item.get("weight"),
                                3,
                            )
                    if nodes[1].element_id in b3_id_set:
                        if nodes[0].element_id == a1_id:
                            result_dict[nodes[1].get("Title")]["weight_1"] = round(
                                item.get("weight"),
                                3,
                            )

                for item in r2Set:
                    nodes = item.nodes
                    if nodes[0].element_id in b3_id_set:
                        if nodes[1].element_id == a2_id:
                            result_dict[nodes[0].get("Title")]["weight_2"] = round(
                                item.get("weight"),
                                3,
                            )
                    if nodes[1].element_id in b3_id_set:
                        if nodes[0].element_id == a2_id:
                            result_dict[nodes[1].get("Title")]["weight_2"] = round(
                                item.get("weight"),
                                3,
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
        self,
        page_a_id,
        page_b_id,
        page_x_id,
        floatRange,
    ):
        start_time = time.time()
        logging.info("page id: %s,%s,%s", page_a_id, page_b_id, page_x_id)
        except_id_set = set([int(page_a_id), int(page_b_id), int(page_x_id)])
        d1 = await get_neo4j_relationships_weight(
            page_a_id,
            page_b_id,
            self.neo4j_driver,
        )
        d2 = await get_neo4j_relationships_weight(
            page_a_id,
            page_x_id,
            self.neo4j_driver,
        )

        if d1 >= 1 or d2 >= 1:
            return {
                "d1": round(d1, 3),
                "d2": round(d2, 3),
                "data": [],
            }

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
  WHEN endNode(r) IN b3Set THEN {weight: r.weight, Title: endNode(r).Title, Id: endNode(r).Id}
  WHEN startNode(r) IN b3Set THEN {weight: r.weight,Title: startNode(r).Title, Id: endNode(r).Id}
END AS result1
WHERE result1 IS NOT null
WITH collect(result1) as distance_1,r2Set,b3Set

UNWIND r2Set as r
WITH distance_1,
CASE
  WHEN endNode(r) IN b3Set THEN {weight: r.weight,Title: endNode(r).Title, Id: endNode(r).Id}
  WHEN startNode(r) IN b3Set THEN {weight: r.weight,Title: startNode(r).Title, Id: endNode(r).Id}
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
                r1_weight_max=min(d2 + floatRange, 0.9999),
                a2_ID=int(page_x_id),
                r2_weight_min=max(d1 - floatRange, 0),
                r2_weight_max=min(d1 + floatRange, 0.9999),
            )

            result_dict = {}
            async for record in result:
                distance_2 = record.get("distance_2")
                distance_1 = record.get("distance_1")

                for item in distance_1:
                    if item.get("Id") in except_id_set:
                        continue

                    key = item.get("Title")
                    result_dict[key] = {
                        "weight_1": round(item.get("weight"), 3),
                        "weight_2": 0,
                        "title": key,
                    }

                for item in distance_2:

                    if item.get("Id") in except_id_set:
                        continue

                    key = item.get("Title")
                    if key in result_dict:
                        result_dict[key]["weight_2"] = round(item.get("weight"), 3)

            logging.info(f"Execution time: {time.time()-start_time} seconds")
            return {
                "d1": round(d1, 3),
                "d2": round(d2, 3),
                "data": list(result_dict.values()),
            }
