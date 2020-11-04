#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : toutiaohao_search.py
# @Author: Cedar
# @Date  : 2020/5/19
# @Desc  :


import time
import asyncio, aiohttp
from aiohttp import ClientSession
import json
import re
from datetime import datetime
from elasticsearch import Elasticsearch
import hashlib
from lxml import etree
from search_word import search_word_list


tasks = []
# url_template = "https://so.toutiao.com/search?keyword=%E5%BC%A0%E4%B8%80%E9%B8%A3&pd=user&source=input&traffic_source=&original_source=&in_tfs=&in_ogs="
url_template = "https://so.toutiao.com/search?keyword={}&pd=user&source=input&traffic_source=&original_source=&in_tfs=&in_ogs="


def get_token(md5str):
    # md5str = "abc"
    # 生成一个md5对象
    m1 = hashlib.md5()
    # 使用md5对象里的update方法md5转换
    m1.update(md5str.encode("utf-16LE"))
    token = m1.hexdigest()
    return token


async def get_response(url, semaphore):
    async with semaphore:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                headers = {
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36",
                }
                async with session.get(url, headers=headers) as response:
                    text = await response.text()
                    root = etree.HTML(text, parser=etree.HTMLParser(encoding='utf-8'))
                    print(url)

                    items = root.xpath('//div[@class="result"]')
                    # print(items)
                    for item in items:
                        try:
                            title = "".join(item.xpath('.//p[@class="c-author"]/text()'))
                            title = title.strip()
                            title = re.findall(r'(.*) ', title)[0]
                            print(title)
                            src = "".join(item.xpath('.//p[@class="c-author"]/img/@src'))
                            app_id = re.findall(r"_(.+?).jpeg", src)[0]
                            listpage_url = "https://baijiahao.baidu.com/u?app_id=" + app_id
                            print(listpage_url)

                            _id = get_token(listpage_url)

                            es = Elasticsearch("192.168.2.56:9200")
                            data = {
                                "@timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000+0800"),
                                "title": title,
                                "listpage_url": listpage_url
                            }
                            es.index(index="baijiahao_all", doc_type="baijiahao", id=_id, body=data)

                        except Exception as e:
                            print(e)
            # except aiohttp.client.ServerDisconnectedError as e:
            #     return str(e)
            # except aiohttp.client.ClientResponseError as e:
            #     return str(e)
            # except asyncio.TimeoutError as e:
            #     return str(e)
            # except aiohttp.client.ClientConnectorError as e:
            #     return str(e)
            except Exception as e:
                pass


async def create_task(word, start, end):
    semaphore = asyncio.Semaphore(1)  # 限制并发量为500
    for i in range(start, end):
        print(i)
        pn = 50 * i
        task = asyncio.ensure_future(get_response(url_template.format(word, pn), semaphore))
        tasks.append(task)


def run():
    loop = asyncio.get_event_loop()
    for word in search_word_list:
        for j in range(0, 20):
            # print(1*j, 1*j+1)
            global tasks
            tasks = []
            loop.run_until_complete(create_task(word, 1*j, 1*j+1))
            loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()


if __name__ == '__main__':
    run()






