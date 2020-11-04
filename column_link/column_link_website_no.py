#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : column_link_website_no.py
# @Author: Cedar
# @Date  : 2020/4/22
# @Desc  :

import time
import asyncio
import aiohttp
from lxml import etree
from requests.compat import urljoin
import lib.common as common
import warnings
import logging


logger = logging.getLogger()
logger.setLevel('DEBUG')
BASIC_FORMAT = "%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s"
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
formatter = logging.Formatter(BASIC_FORMAT, DATE_FORMAT)
chlr = logging.StreamHandler()  # 输出到控制台的handler
chlr.setFormatter(formatter)
chlr.setLevel('CRITICAL')  # 也可以不设置，不设置就默认用logger的level
fhlr = logging.FileHandler('./log/column_link_website_no.log')  # 输出到文件的handler
fhlr.setFormatter(formatter)
logger.addHandler(chlr)
logger.addHandler(fhlr)

# 忽略mysql插入时主键重复的警告
warnings.filterwarnings('ignore')
extractor_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}


async def get_response(semaphore, url, column_extraction_deep=1, domain_code_source=None, website_no=None):
    try:
        async with semaphore:
            timeout = aiohttp.ClientTimeout(total=20)
            # ValueError: too many file descriptoersin select()报错问题
            # 一般是并发请求数太大导致的，通常通过减少并发数解决。
            #
            # 我遇到的情况：并发量设置的不高，运行一段时间后报该错误。通过搜索、调试，最后看aiohttp文档时发现是因为请求的https站点的服务器没有正确完成ssl连接，需要指定一个叫enable_cleanup_closed的参数为True：
            #
            # session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(enable_cleanup_closed=True)
            # 官方对enable_cleanup_closed参数的解释：
            #
            # Some ssl servers do not properly complete SSL shutdown process, in that case asyncio leaks SSL connections.
            # If this parameter is set to True, aiohttp additionally aborts underlining transport after 2 seconds. It is off by default.
            #
            # 作者：Ovie
            # 链接：https://www.jianshu.com/p/f7af4466f346
            # 来源：简书
            # 著作权归作者所有。商业转载请联系作者获得授权，非商业转载请注明出处。
            connector = aiohttp.TCPConnector(limit=60, verify_ssl=False, enable_cleanup_closed=True)  # 60小于64。也可以改成其他数
            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                headers = {
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36",
                }
                async with session.get(url, headers=headers) as response:
                    text = await response.text()
                    root = etree.HTML(text, parser=etree.HTMLParser(encoding='utf-8'))
                    # print(url)
                    logging.info(url)
                    column_extraction_deep = int(column_extraction_deep) + 1

                    items = root.xpath('//a')
                    column_list = []
                    # print(len(items))
                    for item in items:
                        title = "".join(item.xpath('.//text()'))
                        listpage_url = "".join(item.xpath('./@href'))
                        listpage_url = urljoin(url, listpage_url)
                        # 去掉标点符号
                        title = common.filter_punctuation(title)
                        listpage_url = common.match_url(listpage_url)
                        # 计算url MD5 先去掉http和末尾斜杆
                        md5_source = listpage_url
                        md5_source = md5_source.replace('http://', '')
                        md5_source = md5_source.replace('https://', '')
                        md5_source = md5_source.rstrip('/')
                        record_md5_id = common.get_token(md5_source)
                        domain_code = common.get_domain_code(listpage_url)
                        host_code = common.get_host_code(listpage_url)
                        # domain 要与源域名一致
                        if domain_code_source != domain_code:
                            continue

                        # 垃圾词、垃圾域名过滤
                        level_score, score_detail = common.is_need_filter(title, listpage_url, False)
                        # print(level_score, score_detail)
                        logging.info(str(level_score) + '=' + score_detail)

                        if level_score > 20:
                            column = f"('{title}', '{listpage_url}', '{record_md5_id}', '{website_no}', {column_extraction_deep}, '{domain_code}', '{host_code}', '{level_score}', '{score_detail}')"
                            column_list.append(column)

                    # 批量插入
                    values = ",".join(column_list)
                    insert_column = f"insert ignore into column_link(Title, URL, record_md5_id, website_no, column_extraction_deep, domain_code, host_code, level_score, score_detail) values{values};"
                    # print(insert_column)
                    common.query_mysql(extractor_118, insert_column)
                    return True

    except Exception as e:
        if len(str(e)) > 0:
            logging.error(str(e))
        return False


async def create_task():
    semaphore = asyncio.Semaphore(100)  # 限制并发量为500

    try:
        # 开始大循环，每次调度100个url，异步运行全部完成之后，才再添加
        for j in range(1):
            logging.critical("j:" + str(j) + "----------------------------")
            start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            begin = time.time()

            tasks = []
            answers = []
            # 查询待采集目标
            select_column = f"select Column_Link_ID, Column_Extraction_Deep, URL, Domain_Code, Website_No, Extracted_flag " \
                            f"from column_link where Extracted_flag is null " \
                            f"ORDER BY Column_Extraction_Deep limit 100;"
            # print(select_column)
            results = common.query_mysql(extractor_118, select_column)
            # 更新Extracted_flag
            id_list = [0]
            for result in results:
                id_list.append(result["Column_Link_ID"])
            id_list = tuple(id_list)
            update_flag = f"update column_link set Extracted_flag='S' where Column_Link_ID in {id_list};"
            # print(update_flag)
            common.query_mysql(extractor_118, update_flag)

            # 开始任务
            for result in results:
                # print(result)
                url = result["URL"]
                domain_code = result["Domain_Code"]
                website_no = result["Website_No"]
                column_extraction_deep = result["Column_Extraction_Deep"]
                if not column_extraction_deep:
                    column_extraction_deep = 0
                # 最多采集3层
                if column_extraction_deep <= 3:
                    task = asyncio.ensure_future(
                        get_response(semaphore, url, column_extraction_deep, domain_code, website_no))
                    tasks.append(task)

            # 先等待上面的任务完成
            start_tasks = asyncio.all_tasks()
            logging.critical("start_tasks: " + str(len(start_tasks)))
            if len(start_tasks) > 510:
                for t in tasks:
                    t.cancel()
            try:
                for next_to_complete in asyncio.as_completed(tasks, timeout=100):
                    answer = await next_to_complete
                    if answer:
                        answers.append(answer)
                    # print(answer)
            except asyncio.TimeoutError as e:
                logging.critical("error_tasks: " + str(len(asyncio.all_tasks())))
                for t in tasks:
                    t.cancel()

            logging.critical("end_tasks: " + str(len(asyncio.all_tasks())))
            # result = await asyncio.gather(*tasks)
            # print(result)

            logging.critical("start_time: " + start_time)
            logging.critical("tasks:" + str(len(tasks)))
            logging.critical("answers:" + str(len(answers)))
            end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            logging.critical("end_time: " + end_time)
            finish = time.time()
            duration = int(finish) - int(begin)
            logging.critical("duration: " + str(duration) + "s")

    except Exception as e:
        if len(str(e)) > 0:
            logging.error(str(e))


def run():
    for i in range(1000000):
        try:
            event_loop = asyncio.new_event_loop()
            event_loop.run_until_complete(create_task())
        except Exception as e:
            event_loop.close()
            if len(str(e)) > 0:
                logging.error(str(e))


if __name__ == '__main__':
    run()
