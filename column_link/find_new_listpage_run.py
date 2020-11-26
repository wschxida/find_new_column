#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : find_new_listpage_run.py
# @Author: Cedar
# @Date  : 2020/10/30
# @Desc  :


import os
import configparser
import logging
import redis
import json
import hashlib
import traceback
import random
import lib.common as common
import time
import asyncio
import aiohttp
from lxml import etree
from requests.compat import urljoin
import warnings
from functools import wraps


# 日志记录
logger = logging.getLogger()
logger.setLevel('DEBUG')
BASIC_FORMAT = "%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s"
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
formatter = logging.Formatter(BASIC_FORMAT, DATE_FORMAT)
chlr = logging.StreamHandler()  # 输出到控制台的handler
chlr.setFormatter(formatter)
chlr.setLevel('ERROR')  # 也可以不设置，不设置就默认用logger的level, ERROR
fhlr = logging.FileHandler('./log/find_new_listpage_run.log')  # 输出到文件的handler
fhlr.setFormatter(formatter)
logger.addHandler(chlr)
logger.addHandler(fhlr)

# 忽略mysql插入时主键重复的警告
warnings.filterwarnings('ignore')


def execute_time(fn):
    """
    修饰器，用于记录函数执行时长
    用法 @stat_time
    :param fn:
    :return:
    """
    @wraps(fn)
    def wrap(*args, **kw):
        start_time = time.time()
        ret = fn(*args, **kw)
        ended_time = time.time()
        print("call {}() cost: {} seconds".format(fn.__name__, ended_time - start_time))
        return ret

    return wrap


def parse_html_to_database(database_config, url, column_extraction_deep, domain_code_source, website_no, Is_Need_VPN, text):
    try:
        root = etree.HTML(text, parser=etree.HTMLParser(encoding='utf-8'))
        column_extraction_deep = int(column_extraction_deep) + 1

        items = root.xpath('//a')
        column_list = []
        for num, item in enumerate(items):

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

            # 计算a节点占全部a节点百分比，如果总节点小于50就去前50%的节点；如果在50和200之前，就取前30%；大于200，就取前20%
            len_items = len(items)
            node_percent = num/len(items)
            print(num, 'percent:{:.0%}'.format(node_percent), title)
            if len_items < 50:
                if node_percent > 0.5:
                    continue
            if (len_items >= 50) and (len_items <= 200):
                if node_percent > 0.3:
                    continue
            if len_items > 200:
                if node_percent > 0.2:
                    continue

            # 垃圾词、垃圾域名过滤
            level_score, score_detail = common.is_need_filter(title, listpage_url, True)
            # print(level_score, score_detail)
            logging.info(str(level_score) + '=' + score_detail)

            # 入库分值，新闻要大于等于20，论坛要大于等于10
            valid_score = 20
            media_type = common.get_media_type(listpage_url)
            if media_type == 'forum':
                valid_score = 10
            if level_score >= valid_score:
                column = f"('{title}', '{listpage_url}', '{record_md5_id}', '{website_no}', {column_extraction_deep}, '{domain_code}', '{host_code}', '{level_score}', '{score_detail}')"
                column_list.append(column)

        # 批量插入
        values = ",".join(column_list)
        insert_column = f"insert ignore into column_link(Title, URL, record_md5_id, website_no, column_extraction_deep, domain_code, host_code, level_score, score_detail) values{values};"
        # print(insert_column)
        common.query_mysql(database_config, insert_column)
        return True
    except Exception as e:
        logging.ERROR(traceback.format_exc())


async def get_response(database_config, semaphore, url, column_extraction_deep=1, domain_code_source=None, website_no=None, Is_Need_VPN=0):
    try:
        async with semaphore:
            timeout = aiohttp.ClientTimeout(total=60)
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
                # ERROR: 400, message='Can not decode content-encoding: brotli (br). Please install `brotlipy`', url=URL('https://www.bannedbook.org/')
                headers = {
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36',
                    # 'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/sig"ned-exchange;v=b3;q=0.9',
                    # 'accept-encoding': 'gzip, deflate',
                    # 'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7',
                }
                if Is_Need_VPN:
                    proxy = 'http://127.0.0.1:7777'
                else:
                    proxy = None
                async with session.get(url, headers=headers, proxy=proxy) as response:
                    # errors='ignore',解决'gb2312' codec can't decode
                    text = await response.text(errors='ignore')
                    # print(response.get_encoding(), url)
                    # 有些网站识别编码错误，如https://www.bannedbook.org/，识别是Windows-1254
                    if 'Windows' in response.get_encoding():
                        text = await response.text(errors='ignore', encoding='utf-8')
                    # print(text)
                    parse_html_to_database(database_config, url, column_extraction_deep, domain_code_source, website_no, Is_Need_VPN, text)
                    return len(text)

    except Exception as e:
        if len(str(e)) > 0:
            logging.error(str(e))
        return None


@execute_time
def create_task(loop, database_config):
    semaphore = asyncio.Semaphore(200)  # 限制并发量为500
    try:
        tasks = []
        # 查询待采集目标
        select_column = f"""
            select Column_Link_ID, Column_Extraction_Deep, URL, Domain_Code, Website_No, Extracted_flag 
            from column_link where Extracted_flag is null
            ORDER BY Column_Extraction_Deep limit 200;
            """
        target_items = common.query_mysql(database_config, select_column)
        id_list = [0]
        for item in target_items:
            id_list.append(item["Column_Link_ID"])
            url = item["URL"]
            domain_code = item["Domain_Code"]
            website_no = item["Website_No"]
            column_extraction_deep = item["Column_Extraction_Deep"]
            # 最多采集3层
            if column_extraction_deep <= 3:
                task = asyncio.ensure_future(get_response(database_config, semaphore, url, column_extraction_deep, domain_code, website_no))
                tasks.append(task)

        results = loop.run_until_complete(asyncio.gather(*tasks))
        # print(results)
        # 更新Extracted_flag
        id_list = tuple(id_list)
        update_flag = f"update column_link set Extracted_flag='S' where Column_Link_ID in {id_list};"
        common.query_mysql(database_config, update_flag)
        return len(target_items)

    except Exception as e:
        if len(str(e)) > 0:
            logging.error(str(e))
        return None


def main():
    # 数据库配置
    database_config = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}

    # 异步请求
    # event_loop = asyncio.get_event_loop()
    event_loop = asyncio.ProactorEventLoop()
    asyncio.set_event_loop(event_loop)
    while True:
        try:
            target_count = create_task(event_loop, database_config)
            print(target_count)
            if target_count < 1:
                time.sleep(10)
        except Exception as e:
            logging.error(str(e))
            # break
    # event_loop.close()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logging.ERROR(traceback.format_exc())
