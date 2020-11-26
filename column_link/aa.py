#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : aa.py
# @Author: Cedar
# @Date  : 2020/11/18
# @Desc  :

import lib.common as common


title = '新闻'
listpage_url = 'https://www.lilai616.com/'
listpage_url = 'https://woiyu.com/'
level_score, score_detail = common.is_need_filter(title, listpage_url, True)

print(score_detail)
