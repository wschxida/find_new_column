#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : instagram_user.py
# @Author: Cedar
# @Date  : 2020/4/17
# @Desc  :

import time
import requests
from requests.adapters import HTTPAdapter
import json
from datetime import datetime
from elasticsearch import Elasticsearch


url_template = "https://i.instagram.com/api/v1/users/{0}/info/"
BASE_URL = 'https://www.instagram.com/'
STORIES_UA = 'Instagram 123.0.0.21.114 (iPhone; CPU iPhone OS 11_4 like Mac OS X; en_US; en-US; scale=2.00; 750x1334) AppleWebKit/605.1.15'
proxy = 'http://127.0.0.1:7777'
proxy_list = {'http': proxy, 'https': proxy}


def run():
    session = requests.session()
    session.mount('http://', HTTPAdapter(max_retries=5))
    session.mount('https://', HTTPAdapter(max_retries=5))
    headers = {
        "Referer": BASE_URL,
        "user-agent": STORIES_UA,
    }
    session.headers.update(headers)
    session.proxies.update(proxy_list)

    for i in range(996313, 123456789):
        time.sleep(0.5)
        url = url_template.format(i)
        print(url)

        try:
            max_retry_block_ip = 10  # 这个重试是在ip被封的时候的重试，跟requests重试是不一样的
            for j in range(max_retry_block_ip):
                response = session.get(url, timeout=10)
                text = response.text
                text_json = json.loads(text)
                print(text_json)
                status = text_json["status"]
                if status == 'ok':
                    username = text_json["user"]["username"]
                    profile_pic_url = text_json["user"]["profile_pic_url"]
                    _id = int(text_json["user"]["pk"])

                    # 全入库
                    es = Elasticsearch("192.168.2.56:9200")
                    data = {
                        "@timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000+0800"),
                        "username": username,
                        "id": _id,
                        "profile_pic_url": profile_pic_url
                    }
                    # es.index(index="baijiahao", doc_type="baijiahao", id=_id, body=data)
                    try:
                        es.create(index="instagram_user", doc_type="instagram_user", id=_id, body=data)
                    except Exception as e:
                        print(str(e))

                    break

                # 如果IP被封了，暂停60秒
                # if status == 'fail':
                else:
                    time.sleep(60)

        except Exception as e:
            print(str(e))


if __name__ == '__main__':
    run()




