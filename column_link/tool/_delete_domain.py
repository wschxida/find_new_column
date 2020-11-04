#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : _delete_domain.py
# @Author: Cedar
# @Date  : 2020/6/15
# @Desc  :


import json
import requests
from lxml import etree
import parsedatetime
from datetime import datetime
import time
import re


word_list = [
    '29nh.cn',
    'xzwyu.com',
    'chmta.com',
]

for i in word_list:
    sql = f"delete from column_link where domain_code='{i}';"
    # sql = f"delete from cloud_listpage_url where domain_code='{i}';"
    # sql = f"delete from column_link where title='{i}';"
    print(sql)
