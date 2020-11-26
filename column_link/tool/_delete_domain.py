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


with open('word.txt', 'r', encoding='utf8') as f:
    word_list = [i.replace('\n', '') for i in f.readlines()]
    print(word_list)


for i in word_list:
    # sql = f"delete from column_link where domain_code='{i}';"
    # sql = f"delete from cloud_listpage_url where domain_code='{i}';"
    # sql = f"delete from column_link where title like'%{i}%';"
    sql = f"delete from cloud_listpage_url where listpage_title like'%{i}%';"
    print(sql)
