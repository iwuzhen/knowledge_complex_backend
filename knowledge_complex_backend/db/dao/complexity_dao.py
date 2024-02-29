import numpy
import collections
import logging
from fastapi import Depends
from knowledge_complex_backend.db.dependencies import get_db_pool
from fastapi_cache.decorator import cache
import json

class ComplexityDAO:
    def __init__(self, pool = Depends(get_db_pool)):
        self.pool = pool

    async def test(self):
        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute("SELECT COUNT(*) FROM artSizeByCatAndYear;")
            # print(cur.description)
            (r,) = await cur.fetchone()
            logging.info(r)
            return r

    @cache(expire=60*60)
    async def number_of_papers_per_year_by_country_dx(self, flow: str):
        """国家历年的新增的论文数,只返回 top 20 的国家"""
        table_name = {
            "paper": "artSizeByCtryAndYear",
            "import": "importByCtryAndYear",
            "export": "exportByCtryAndYear",
        }.get(flow,"")

        if not table_name:
            return ""

        start_year = 1980
        end_year = 2022
        sql = f"""
            SELECT country, year, SUM(count) as total_count
            FROM {table_name} WHERE year >= {start_year} AND year <= {end_year}
            GROUP BY country, year
            ORDER BY country, year;
            """
        data_collect = collections.defaultdict(lambda: [0]*(end_year - start_year+1))
        data_collect_rank = collections.defaultdict(int)
        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute(sql)
            # print(cur.description)
            query_results = await cur.fetchall()

            for c,y,t in query_results:
                data_collect[c][int(y)-start_year] = int(t)
                if y == str(end_year):
                    data_collect_rank[c] += int(t)

            # top20 [('US', 20099300), ('CN', 10463941), ('GB', 4937061), ('DE', 4034381),...]

            # name top 20
            # top_end_year = sorted(data_collect_rank.items(), key=lambda x:x[1], reverse=True)

            # header = ['year', *[year for year in range(start_year, end_year+1)]]
            # logging.info(data_collect)
            # tmp = [[v[0],*data_collect[v[0]]] for v in top20]
            data = []
            data_collect_list = [[k,v] for k,v in data_collect.items()]
            data_collect_list = sorted(data_collect_list, key=lambda x:data_collect_rank.get(x[0],float('-inf')), reverse=True)

            legend = []
            for k,v in data_collect_list:
                data.append(v)
                legend.append(k)

            array = numpy.array(data)
            ranks = array.shape[0] - numpy.argsort(numpy.argsort(array, axis=0), axis=0)
            ranks_list = ranks.tolist()
            return  {
                'legend': legend,
                'year': [year for year in range(start_year, end_year+1)],
                'rank': ranks_list,
                'data': data,
            }

    async def paper_ingredient(self, mode: str,flow: str, countries: list[str], years: list[int]):
        if mode == "national_academic_disciplines":
            return await self.paper_ingredient_national_academic_disciplines(flow,countries,years)
        if mode == "national_between_countries":
            return await self.paper_ingredient_national_between_countries(flow,countries,years)

    @cache(expire=60*60)
    async def paper_ingredient_national_between_countries(self, flow: str, countries: list[str], years: list[int]):
        """tree map, 从国家找国家,  """
        table_name = {
            "export": "exportBetweenCtryAndYear_1",
            "import": "importBetweenCtryAndYear",
        }.get(flow,"")

        if not table_name:
            logging.info("unKnow flow %s", flow)
            return ""

        sql = f"""
            SELECT c.countryB, c.count
            FROM {table_name} as c
            WHERE c.countryA IN %s AND c.year IN %s;
            """
        result_map = collections.defaultdict(int)
        total = 0
        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute(sql, (countries, years))
            # print(cur.description)
            query_results = await cur.fetchall()

            for cat,count in query_results:
                total += count
                # result_list.append((cat, count))
                result_map[cat] += count

        return [{
            'name': name, 'value': value
        }  for name,value in result_map.items()]

    @cache(expire=60*60)
    async def paper_ingredient_national_academic_disciplines(self, flow: str, countries: list[str], years: list[int]):
        """tree map, 从国家找产品, 将原来的3级分类上升到 1,2 级分类，构建树结构，结果中不再包含3成结构
        关于跨学科的学科统计办法，l1内l2, 是不重复的，l0 内，对l2去重"""
        # 查询学科引用
        table_name = {
            "paper": "artSizeByCatAndCtryAndYear",
            "export": "exportByCtryAndCatAndYear",
            "import": "importByCtryAndCatAndYear",
        }.get(flow,"")

        if not table_name:
            logging.info("unKnow flow %s", flow)
            return ""

        sql = f"""
            SELECT c.cat, c.count
            FROM {table_name} as c
            WHERE c.country IN %s AND c.year IN %s;
            """
        result_map = collections.defaultdict(int)
        total = 0
        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute(sql, (countries, years))
            # print(cur.description)
            query_results = await cur.fetchall()

            for cat,count in query_results:
                total += count
                # result_list.append((cat, count))
                result_map[cat] += count

        result_list = [(key,value) for key,value in result_map.items()]
        result_list.sort(key=lambda x:-x[1])

        # 查询学科的父类，从1级直接到3级
        # sub_cat_dict = collections.defaultdict(list)
        # parent_cat_set = set()
        # sql = """
        #     SELECT c.cat, c.parent_cat
        #     FROM cat_ancestor as c
        #     WHERE c.cat IN %s AND c.parent_level = 0;
        # """
        # async with self.pool.acquire() as conn:
        #     cur = await conn.cursor()
        #     await cur.execute(sql, ([row[0] for row in result_list],))
        #     query_results = await cur.fetchall()

        #     for cat,par_cat in query_results:
        #         sub_cat_dict[cat].append(par_cat)
        #         parent_cat_set.add(par_cat)

        # result_dict = {}
        # for name,value in result_list:
        #     if parent_subject := sub_cat_dict.get(name, []):
        #         for parent_name in parent_subject:
        #             result_dict.setdefault(parent_name, {
        #                 'name':parent_name,
        #                 'value':0,
        #                 'children': []
        #             })
        #             result_dict[parent_name]['value'] += value
        #             result_dict[parent_name]['children'].append(
        #                 {'name':name, 'value': value}
        #             )

        # 查询学科的父类，3级不保留，留存1级和2级
        # level 1
        l1_cat_dict = collections.defaultdict(list)
        sql = """
            SELECT c.cat, c.parent_cat
            FROM cat_ancestor as c
            WHERE c.cat IN %s AND c.parent_level = 1;
        """
        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute(sql, ([row[0] for row in result_list],))
            query_results = await cur.fetchall()

            for cat,par_cat in query_results:
                l1_cat_dict[par_cat].append(cat)

        # level 0
        l0_cat_dict = collections.defaultdict(list)
        sql = """
            SELECT c.cat, c.parent_cat
            FROM cat_ancestor as c
            WHERE c.cat IN %s AND c.parent_level = 0;
        """
        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute(sql, ([row for row in l1_cat_dict.keys()],))
            query_results = await cur.fetchall()
            for cat,par_cat in query_results:
                l0_cat_dict[par_cat].append(cat)

        result_dict = {}
        for l0_name, l1_cats in l0_cat_dict.items():
            l1_value_set = set()
            result_dict.setdefault(l0_name, {
                'name':l0_name,
                'value':0,
                'children': []
            })
            for l1_name in l1_cats:
                l1_item = {'name':l1_name, 'value': 0}
                for l3_name in l1_cat_dict.get(l1_name,[]):
                    l1_item['value'] += result_map.get(l3_name, 0)
                    if l3_name not in l1_value_set:
                        result_dict[l0_name]['value'] += result_map.get(l3_name, 0)
                        l1_value_set.add(l3_name)

                result_dict[l0_name]['children'].append(l1_item)

        return list(result_dict.values())

    @cache(expire=60*60)
    async def country_eci(self):
        """国家的eci"""

        start_year = 1980
        end_year = 2022
        sql = f"""
        SELECT c.year, c.country, c.eci, c.rank
        FROM rank_eci as c
        WHERE c.year >= {start_year} AND c.year <= {end_year};
        """
        data_collect_rank = collections.defaultdict(lambda: [9999]*(end_year - start_year+1))
        data_collect_eci = collections.defaultdict(lambda: [0]*(end_year - start_year+1))
        data_collect_total_rank = {}
        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute(sql)
            # print(cur.description)
            query_results = await cur.fetchall()

            for year,country,eci,rank in query_results:
                data_collect_eci[country][int(year)-start_year] = round(float(eci),3)
                data_collect_rank[country][int(year)-start_year] = int(rank)
                if year == str(end_year):
                    data_collect_total_rank[country] = float(eci)

            data,eci_matrix,rank_matrix = [],[],[]
            for k, v in data_collect_eci.items():
                eci_matrix.append([k,v])
                rank_matrix.append([k,data_collect_rank[k]])

            eci_matrix = sorted(eci_matrix, key=lambda x:data_collect_total_rank.get(x[0],float('-inf')), reverse=True)
            rank_matrix = sorted(rank_matrix, key=lambda x:data_collect_total_rank.get(x[0],float('-inf')), reverse=True)

            legend = []
            for k,v in eci_matrix:
                data.append(v)
                legend.append(k)
            rank = []
            for k,v in rank_matrix:
                rank.append(v)

            # array = numpy.array(data)
            # ranks = array.shape[0] - numpy.argsort(numpy.argsort(array, axis=0), axis=0)
            # ranks_list = ranks.tolist()
            return  {
                'legend': legend,
                'year': [year for year in range(start_year, end_year+1)],
                'rank': rank,
                'data': data,
            }

    @cache(expire=60*60)
    async def subject_pci(self):
        """学科的pci"""

        start_year = 1980
        end_year = 2022

        # 获得 top 50 的所有学科，因为每年学科变动都比较大，所以这些学科需要重点关注
        sql = f"""
        SELECT c.year, c.cat, c.pci, c.rank
        FROM rank_pci as c
        WHERE c.year >= {start_year} AND c.year <= {end_year} AND c.rank < 50;
        """
        subject_map = set()
        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute(sql)
            # print(cur.description)
            query_results = await cur.fetchall()
            for year,subject,pci,rank in query_results:
                subject_map.add(subject)

        # 对学科进行补全

        sql = f"""
        SELECT c.year, c.cat, c.pci, c.rank
        FROM rank_pci as c
        WHERE c.year >= {start_year} AND c.year <= {end_year} AND c.cat IN %s;
        """
        data_collect_rank = collections.defaultdict(lambda: [9999]*(end_year - start_year+1))
        data_collect_pci = collections.defaultdict(lambda: [0]*(end_year - start_year+1))
        data_collect_total_rank = {}
        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute(sql, [list(subject_map),])
            # print(cur.description)
            query_results = await cur.fetchall()

            for year,country,pci,rank in query_results:
                data_collect_pci[country][int(year)-start_year] = round(float(pci),5)
                data_collect_rank[country][int(year)-start_year] = int(rank)
                if year == str(end_year):
                    data_collect_total_rank[country] = float(pci)

            data,pci_matrix,rank_matrix = [],[],[]
            for k, v in data_collect_pci.items():
                pci_matrix.append([k,v])
                rank_matrix.append([k,data_collect_rank[k]])

            pci_matrix = sorted(pci_matrix, key=lambda x:data_collect_total_rank.get(x[0],float('-inf')), reverse=True)
            rank_matrix = sorted(rank_matrix, key=lambda x:data_collect_total_rank.get(x[0],float('-inf')), reverse=True)

            legend = []
            for k,v in pci_matrix:
                data.append(v)
                legend.append(k)
            rank = []
            for k,v in rank_matrix:
                rank.append(v)

            # 补全缺失的 rank 和 value

            return  {
                'legend': legend,
                'year': [year for year in range(start_year, end_year+1)],
                'rank': rank,
                'data': data,
            }

    async def country_academic_trend(self, mode: str,flow: str, countries: list[str],):
        # 1980-2022 年的数据趋势
        if mode == "national_academic_disciplines":
            return await self.paper_ingredient_national_academic_disciplines_trend(flow,countries)
        if mode == "national_between_countries":
            return await self.paper_ingredient_national_between_countries_trend(flow,countries)

    @cache(expire=60*60)
    async def paper_ingredient_national_academic_disciplines_trend(self, flow: str, countries: list[str]):
        # 一级学科的年度趋势
        """tree map, 从国家找产品, 将原来的3级分类上升到 1,2 级分类，构建树结构，结果中不再包含3成结构 """
        # 查询学科引用
        table_name = {
            "paper": "artSizeByCatAndCtryAndYear",
            "export": "exportByCtryAndCatAndYear",
            "import": "importByCtryAndCatAndYear",
        }.get(flow,"")

        if not table_name:
            logging.info("unKnow flow %s", flow)
            return ""

        sql = f"""
            SELECT c.cat, c.count, c.year
            FROM {table_name} as c
            WHERE c.country IN %s AND c.year IN %s;
            """

        start_year = 1980
        end_year = 2022
        year_range = [year for year in range(start_year,end_year+1)]
        result_map = collections.defaultdict(lambda: numpy.zeros(len(year_range), dtype=int))

        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute(sql, (countries, year_range))
            # print(cur.description)
            query_results = await cur.fetchall()

            for cat,count,year in query_results:
                result_map[cat][int(year)-start_year] += count

        # result_list = [(key,value) for key,value in result_map.items()]
        # result_list.sort(key=lambda x:-x[1][-1])

        # 把 lv2 映射到 lv0
        # level 0
        l0_cat_dict = collections.defaultdict(lambda: numpy.zeros(len(year_range), dtype=int))
        sql = """
            SELECT c.cat, c.parent_cat
            FROM cat_ancestor as c
            WHERE c.cat IN %s AND c.parent_level = 0;
        """
        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute(sql, (list(result_map.keys()),))
            query_results = await cur.fetchall()

            for cat,par_cat in query_results:
                if cat in result_map:
                    l0_cat_dict[par_cat] += result_map[cat]


        result_list = [(key,value) for key,value in l0_cat_dict.items()]
        result_list.sort(key=lambda x:-x[1][-1])

        legend = []
        data = [year_range,]
        for key,value in result_list:
            legend.append(key)
            data.append(value.tolist())

        return {'legend':legend, 'data':data}

    @cache(expire=60*60)
    async def paper_ingredient_national_between_countries_trend(self, flow: str, countries: list[str]):
        """堆叠层次图 """
        table_name = {
            "export": "exportBetweenCtryAndYear_1",
            "import": "importBetweenCtryAndYear",
        }.get(flow,"")

        if not table_name:
            logging.info("unKnow flow %s", flow)
            return ""

        start_year = 1980
        end_year = 2020
        year_range = [year for year in range(start_year,end_year+1)]
        sql = f"""
            SELECT c.countryB, c.count, c.year
            FROM {table_name} as c
            WHERE c.countryA IN %s AND c.year IN %s;
            """
        result_map = collections.defaultdict(lambda: numpy.zeros(len(year_range), dtype=int))

        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute(sql, (countries,year_range))
            # print(cur.description)
            query_results = await cur.fetchall()

            for cat,count,year in query_results:
                result_map[cat][int(year)-start_year] += count

        result_list = [(key,value) for key,value in result_map.items()]
        result_list.sort(key=lambda x:-x[1][-1])
        result_list = result_list[:20]

        legend = []
        data = [year_range,]
        for key,value in result_list:
            legend.append(key)
            data.append(value.tolist())

        return {'legend': legend, 'data': data}

    # @cache(expire=60*60)
    async def github_country_eci(self,filter_cat:int):
        """github 国家的 eci"""

        start_year = 2008
        end_year = 2023
        quarter_range =  [ f'{year}Q1' for year in range(start_year, end_year+1)]

        sql = f"""
        SELECT c.year, c.country, c.eci, c.rank
        FROM rank_github_eci as c
        WHERE c.year IN %s AND filter_cat=%s;
        """
        data_collect_rank = collections.defaultdict(lambda: [9999]*(end_year - start_year+1))
        data_collect_eci = collections.defaultdict(lambda: [0]*(end_year - start_year+1))
        data_collect_total_rank = {}
        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute(sql, (quarter_range,filter_cat))

            query_results = await cur.fetchall()
            for year,country,eci,rank in query_results:
                data_collect_eci[country][int(year[:4])-start_year] = round(float(eci),3)
                data_collect_rank[country][int(year[:4])-start_year] = int(rank)
                if year[:4] == str(end_year):
                    data_collect_total_rank[country] = float(eci)

            data,eci_matrix,rank_matrix = [],[],[]
            for k, v in data_collect_eci.items():
                eci_matrix.append([k,v])
                rank_matrix.append([k,data_collect_rank[k]])

            # eci_matrix = sorted(eci_matrix, key=lambda x:data_collect_total_rank.get(x[0],float('-inf')), reverse=True)
            # rank_matrix = sorted(rank_matrix, key=lambda x:data_collect_total_rank.get(x[0],float('-inf')), reverse=True)

            legend = []
            for k,v in eci_matrix:
                data.append(v)
                legend.append(k)
            rank = []
            for k,v in rank_matrix:
                rank.append(v)

            return  {
                'legend': legend,
                'year': [year for year in range(start_year, end_year+1)],
                'rank': rank,
                'data': data,
            }

    # @cache(expire=60*60)
    async def github_tag_pci(self,filter_cat:int):
        """学科的pci"""

        start_year = 2008
        end_year = 2023
        quarter_range =  [ f'{year}Q1' for year in range(start_year, end_year+1)]

        # 获得 top 50 的所有学科，因为每年学科变动都比较大，所以这些学科需要重点关注
        sql = f"""
        SELECT c.year, c.cat, c.pci, c.rank
        FROM rank_github_pci as c
        WHERE c.year IN %s AND c.rank < 50 AND filter_cat=%s;
        """
        subject_map = set()
        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute(sql, (quarter_range,filter_cat))
            # print(cur.description)
            query_results = await cur.fetchall()
            for year,subject,pci,rank in query_results:
                subject_map.add(subject)

        # 对学科进行补全

        sql = f"""
        SELECT c.year, c.cat, c.pci, c.rank
        FROM rank_github_pci as c
        WHERE c.year IN %s AND c.cat IN %s AND filter_cat=%s;
        """
        data_collect_rank = collections.defaultdict(lambda: [9999]*(end_year - start_year+1))
        data_collect_pci = collections.defaultdict(lambda: [0]*(end_year - start_year+1))
        data_collect_total_rank = {}
        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute(sql, (quarter_range,list(subject_map),filter_cat))
            # print(cur.description)
            query_results = await cur.fetchall()

            for year,country,pci,rank in query_results:
                data_collect_pci[country][int(year[:4])-start_year] = round(float(pci),5)
                data_collect_rank[country][int(year[:4])-start_year] = int(rank)
                if year[:4] == str(end_year):
                    data_collect_total_rank[country] = float(pci)

            data,pci_matrix,rank_matrix = [],[],[]
            for k, v in data_collect_pci.items():
                pci_matrix.append([k,v])
                rank_matrix.append([k,data_collect_rank[k]])

            pci_matrix = sorted(pci_matrix, key=lambda x:data_collect_total_rank.get(x[0],float('-inf')), reverse=True)
            rank_matrix = sorted(rank_matrix, key=lambda x:data_collect_total_rank.get(x[0],float('-inf')), reverse=True)

            legend = []
            for k,v in pci_matrix:
                data.append(v)
                legend.append(k)
            rank = []
            for k,v in rank_matrix:
                rank.append(v)

            # 补全缺失的 rank 和 value

            return  {
                'legend': legend,
                'year': [year for year in range(start_year, end_year+1)],
                'rank': rank,
                'data': data,
            }

    async def patent_ingredient(self, mode: str,flow: str, countries: list[str], year: int):
        if mode == "national_ipc":
            return await self.patent_ingredient_national_ipc(flow,countries,year)
        if mode == "national_between_countries":
            return await self.patent_ingredient_national_between_countries(flow,countries,year)

    @cache(expire=60*60)
    async def patent_ingredient_national_between_countries(self, flow: str, countries: list[str], year: int):
        """tree map, 从国家找国家,  """

        # 年份校验
        year_range = [1990, 2020]
        if year < year_range[0] or year > year_range[1]:
            return
        data_index = year - 1990

        sql = {
            "export":"""
            SELECT c.country_code_a, c.data
            FROM patents_country_citations_trend as c
            WHERE c.country_code_b IN %s;
            """,
            "import": """
            SELECT c.country_code_b, c.data
            FROM patents_country_citations_trend as c
            WHERE c.country_code_a IN %s;
            """,
        }.get(flow,"")

        if not sql:
            logging.info("unKnow flow %s", flow)
            return ""

        BAN_SET = set(["WO","EP"])

        result_map = collections.defaultdict(int)
        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute(sql, (countries, ))
            query_results = await cur.fetchall()

            for cat,data in query_results:
                if cat in BAN_SET:
                    continue
                if cat in countries:
                    continue

                result_map[cat] += json.loads(data)[data_index]

        return [{
            'name': name, 'value': value
        }  for name,value in result_map.items()]

    @cache(expire=60*60)
    async def patent_ingredient_national_ipc(self, flow: str, countries: list[str], year: int):
        """tree map, 从国家 ipc 的分类的量
        flow: patent, linsIn, linsOut
        """
        # 年份校验
        year_range = [1990, 2020]
        IPC_PREFIX_SET = set(["A","B","C","D","E","F","G","H"])
        if year < year_range[0] or year > year_range[1]:
            return
        data_index = year - 1990

        # 查询 patent
        if flow == "patent":
            sql = f"""
                SELECT c.ipc_prefix, c.data
                FROM patents_country_ipc_trend as c
                WHERE c.country_code IN %s AND c.ipc_level = %s;
                """
            result_map = collections.defaultdict(int)
            async with self.pool.acquire() as conn:
                cur = await conn.cursor()
                await cur.execute(sql, (countries,1 ))
                query_results = await cur.fetchall()

                for ipc_prefix,data in query_results:
                    if ipc_prefix[0] not in IPC_PREFIX_SET:
                        continue
                    result_map[ipc_prefix] += json.loads(data)[data_index]


            result_dict = {}
            for l0_name, total in result_map.items():
                result_dict.setdefault(l0_name, {
                    'name':l0_name,
                    'value':total,
                })
            return list(result_dict.values())


        # 查询附加方向
        else:
            # if flow == "export":
            #     sql = f"""
            #         SELECT c.ipc_prefix, c.data， c.country_code_b
            #         FROM patents_country_ipc_citations_trend as c
            #         WHERE c.country_code_a IN %s AND c.ipc_level = %s AND c.direction = 'o';
            #         """
            # elif flow == "import":
            #     sql = f"""
            #         SELECT c.ipc_prefix, c.data, c.country_code_a
            #         FROM patents_country_ipc_citations_trend as c
            #         WHERE c.country_code_b IN %s AND c.ipc_level = %s AND c.direction = 'i';
            #         """
            if flow == "import":
                sql = f"""
                    SELECT c.ipc_prefix, c.data, c.country_code_b
                    FROM patents_country_ipc_citations_trend as c
                    WHERE c.country_code_a IN %s AND c.ipc_level = %s AND c.direction = 'o';
                    """
            elif flow == "export":
                sql = f"""
                    SELECT c.ipc_prefix, c.data, c.country_code_a
                    FROM patents_country_ipc_citations_trend as c
                    WHERE c.country_code_b IN %s AND c.ipc_level = %s AND c.direction = 'i';
                    """

            result_map = collections.defaultdict(int)
            async with self.pool.acquire() as conn:
                cur = await conn.cursor()
                await cur.execute(sql, (countries,1))
                query_results = await cur.fetchall()

                for ipc_prefix,data, country_code in query_results:
                    if ipc_prefix[0] not in IPC_PREFIX_SET:
                        continue
                    # 排除本国
                    if country_code in countries:
                        continue
                    result_map[ipc_prefix] += json.loads(data)[data_index]

            result_dict = {}
            for l0_name, total in result_map.items():
                result_dict.setdefault(l0_name, {
                    'name':l0_name,
                    'value':total,
                })
            return list(result_dict.values())


    async def country_ipc_trend(self, mode: str,flow: str, countries: list[str],):
        # 1980-2022 年的数据趋势
        if mode == "national_ipc":
            return await self.patent_ingredient_national_ipc_trend(flow,countries)
        if mode == "national_between_countries":
            return await self.patent_ingredient_national_between_countries_trend(flow,countries)

    @cache(expire=60*60)
    async def patent_ingredient_national_ipc_trend(self, flow: str, countries: list[str]):
        # 一级学科的年度趋势
        """tree map, 从国家找产品, 将原来的3级分类上升到 1,2 级分类，构建树结构，结果中不再包含3成结构 """

        IPC_PREFIX_SET = set(["A","B","C","D","E","F","G","H"])
        year_range = [year for year in range(1990,2021)]
        # 查询 patent
        if flow == "patent":
            sql = f"""
                SELECT c.ipc_prefix, c.data
                FROM patents_country_ipc_trend as c
                WHERE c.country_code IN %s AND c.ipc_level = %s;
                """
            result_map = collections.defaultdict(lambda: numpy.zeros(2022-1980+1,dtype=int))
            async with self.pool.acquire() as conn:
                cur = await conn.cursor()
                await cur.execute(sql, (countries,1 ))
                query_results = await cur.fetchall()

                for ipc_prefix,data in query_results:
                    if ipc_prefix[0] not in IPC_PREFIX_SET:
                        continue
                    result_map[ipc_prefix] += numpy.array(json.loads(data), dtype=int)

        # 查询附加方向
        else:
            if flow == "export":
                sql = f"""
                    SELECT c.ipc_prefix, c.data
                    FROM patents_country_ipc_citations_trend as c
                    WHERE c.country_code_a IN %s AND c.ipc_level = %s AND c.direction = 'o';
                    """
            elif flow == "import":
                sql = f"""
                    SELECT c.ipc_prefix, c.data
                    FROM patents_country_ipc_citations_trend as c
                    WHERE c.country_code_b IN %s AND c.ipc_level = %s AND c.direction = 'i';
                    """

            result_map = collections.defaultdict(lambda: numpy.zeros(2020-1990+1,dtype=int))
            async with self.pool.acquire() as conn:
                cur = await conn.cursor()
                await cur.execute(sql, (countries,1))
                query_results = await cur.fetchall()

                for ipc_prefix,data in query_results:
                    if ipc_prefix[0] not in IPC_PREFIX_SET:
                        continue
                    result_map[ipc_prefix] += numpy.array(json.loads(data),dtype=int)

        result_list = [(key,value) for key,value in result_map.items()]
        result_list.sort(key=lambda x:-x[1][-1])

        legend = []
        data = [year_range,]
        for key,value in result_list:
            legend.append(key)
            data.append(value.tolist())

        return {'legend':legend, 'data':data}

    @cache(expire=60*60)
    async def patent_ingredient_national_between_countries_trend(self, flow: str, countries: list[str]):
        """堆叠层次图 """

        year_range = [year for year in range(1990,2021)]

        sql = {
            "export":"""
            SELECT c.country_code_a, c.data
            FROM patents_country_citations_trend as c
            WHERE c.country_code_b IN %s;
            """,
            "import": """
            SELECT c.country_code_b, c.data
            FROM patents_country_citations_trend as c
            WHERE c.country_code_a IN %s;
            """,
        }.get(flow,"")

        if not sql:
            logging.info("unKnow flow %s", flow)
            return ""

        BAN_SET = set(["WO","EP"])

        result_map = collections.defaultdict(lambda: numpy.zeros(2020-1990+1,dtype=int))
        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute(sql, (countries, ))
            query_results = await cur.fetchall()

            for cat,data in query_results:
                if cat in BAN_SET:
                    continue

                result_map[cat] += json.loads(data)

        result_list = [(key,value) for key,value in result_map.items()]
        result_list.sort(key=lambda x:-x[1][-1])
        result_list = result_list[:20]

        legend = []
        data = [year_range,]
        for key,value in result_list:
            legend.append(key)
            data.append(value.tolist())

        return {'legend': legend, 'data': data}


    async def subject_ingredient(self, mode: str,flow: str, subjects: list[str], years: list[int]):
        """提供 subject， 按照引用数，依赖，被依赖，计算一个分布"""
        if mode == "national_academic_disciplines":
            return await self.paper_subject_ingredient_national_academic_disciplines(flow,subjects,years)
        if mode == "national_between_countries":
            # todo
            return await self.paper_subject_ingredient_national_between_countries(flow,subjects,years)


    @cache(expire=60*60)
    async def paper_subject_ingredient_national_academic_disciplines(self, flow: str, subjects: list[str], years: list[int]):
        """tree map, 从国家找产品, 将原来的3级分类上升到 1,2 级分类，构建树结构，结果中不再包含3成结构
        关于跨学科的学科统计办法，l1内l2, 是不重复的，l0 内，对l2去重"""
        # 查询学科引用
        table_name = {
            "paper": "artSizeByCatAndCtryAndYear",
            "export": "exportByCtryAndCatAndYear",
            "import": "importByCtryAndCatAndYear",
        }.get(flow,"")

        if not table_name:
            logging.info("unKnow flow %s", flow)
            return ""

        sql = f"""
            SELECT c.country, c.count
            FROM {table_name} as c
            WHERE c.cat IN %s AND c.year IN %s;
            """
        result_map = collections.defaultdict(int)

        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute(sql, (subjects, years))
            # print(cur.description)
            query_results = await cur.fetchall()
            for cat,count in query_results:
                result_map[cat] += count

        result_list = [(key,value) for key,value in result_map.items()]
        result_list.sort(key=lambda x:-x[1])

        result = []
        for name, value in result_list:
            result.append({
                'name':name,
                'value':value,
                'children': []
            })
        return result[:20]


    @cache(expire=60*60)
    async def paper_subject_ingredient_national_between_countries(self, flow: str, subjects: list[str], years: list[int]):
        """tree map, 从国家找国家,  """
        # todo
        table_name = {
            "export": "exportBetweenCtryAndYear_1",
            "import": "importBetweenCtryAndYear",
        }.get(flow,"")

        if not table_name:
            logging.info("unKnow flow %s", flow)
            return ""

        sql = f"""
            SELECT c.countryB, c.count
            FROM {table_name} as c
            WHERE c.countryA IN %s AND c.year IN %s;
            """
        result_map = collections.defaultdict(int)
        total = 0
        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute(sql, (subjects, years))
            # print(cur.description)
            query_results = await cur.fetchall()

            for cat,count in query_results:
                total += count
                # result_list.append((cat, count))
                result_map[cat] += count

        return [{
            'name': name, 'value': value
        }  for name,value in result_map.items()]


    async def subject_academic_trend(self, mode: str,flow: str, subjects: list[str],):
        # 1980-2022 年的数据趋势
        if mode == "national_academic_disciplines":
            return await self.subject_ingredient_national_academic_disciplines_trend(flow,subjects)
        if mode == "national_between_countries":
            # todo
            return await self.paper_ingredient_national_between_countries_trend(flow,subjects)

    @cache(expire=60*60)
    async def subject_ingredient_national_academic_disciplines_trend(self, flow: str, subjects: list[str]):
        # 一级学科的年度趋势
        """tree map, 从国家找产品, 将原来的3级分类上升到 1,2 级分类，构建树结构，结果中不再包含3成结构 """
        # 查询学科引用
        table_name = {
            "paper": "artSizeByCatAndCtryAndYear",
            "export": "exportByCtryAndCatAndYear",
            "import": "importByCtryAndCatAndYear",
        }.get(flow,"")

        if not table_name:
            logging.info("unKnow flow %s", flow)
            return ""

        sql = f"""
            SELECT c.country, c.count, c.year
            FROM {table_name} as c
            WHERE c.cat IN %s AND c.year IN %s;
            """

        start_year = 1980
        end_year = 2022
        year_range = [year for year in range(start_year,end_year+1)]
        result_map = collections.defaultdict(lambda: numpy.zeros(len(year_range), dtype=int))

        async with self.pool.acquire() as conn:
            cur = await conn.cursor()
            await cur.execute(sql, (subjects, year_range))
            # print(cur.description)
            query_results = await cur.fetchall()

            for cat,count,year in query_results:
                result_map[cat][int(year)-start_year] += count

        result_list = [(key,value) for key,value in result_map.items()]
        result_list.sort(key=lambda x:-x[1][-1])

        # result_list = [(key,value) for key,value in l0_cat_dict.items()]
        # result_list.sort(key=lambda x:-x[1][-1])

        legend = []
        data = [year_range,]
        for key,value in result_list[:20]:
            legend.append(key)
            data.append(value.tolist())

        return {'legend':legend, 'data':data}
