#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : common.py
# @Author: Cedar
# @Date  : 2020/4/23
# @Desc  :


import os
import time
import json
from datetime import datetime
import hashlib
from requests.compat import urljoin
import pymysql
import re
import tldextract
import sys
sys.path.append('..')
from lib import custom_str_invalid
from lib import custom_keyword_score


cur_path = os.path.dirname(os.path.realpath(__file__))
# junkword
junkword_file = os.path.join(cur_path, 'reject_junkword.txt')
with open(junkword_file, 'r', encoding='utf-8') as jf:
    reject_junkword_list = [i.replace('\n', '') for i in jf.readlines()]
# reject_domain
rj_domain_file = os.path.join(cur_path, 'reject_domain.txt')
rj_domain_file_custom = os.path.join(cur_path, 'custom_reject_domain.txt')
reject_domain_list = []
with open(rj_domain_file, 'r', encoding='utf-8') as rj:
    for i in rj.readlines():
        reject_domain = i.replace('\n', '')
        if reject_domain.startswith('.'):
            reject_domain = reject_domain[1:]
        reject_domain_list.append(reject_domain)
with open(rj_domain_file_custom, 'r', encoding='utf-8') as rjc:
    for i in rjc.readlines():
        reject_domain = i.replace('\n', '')
        if reject_domain.startswith('.'):
            reject_domain = reject_domain[1:]
        reject_domain_list.append(reject_domain)


def get_token(md5str):
    # md5str = "abc"
    # 生成一个md5对象
    m1 = hashlib.md5()
    # 使用md5对象里的update方法md5转换
    m1.update(md5str.encode("utf-16LE"))
    token = m1.hexdigest()
    return token


def query_mysql(config_params, query_sql):
    """
    执行SQL
    :param config_params:
    :param query_sql:
    :return:
    """
    # 连接mysql
    config = {
        'host': config_params["host"],
        'port': config_params["port"],
        'user': config_params["user"],
        'passwd': config_params["passwd"],
        'db': config_params["db"],
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor
    }
    results = None
    try:
        conn = pymysql.connect(**config)
        conn.autocommit(1)
        # 使用cursor()方法获取操作游标
        cur = conn.cursor()
        cur.execute(query_sql)  # 执行sql语句
        results = cur.fetchall()  # 获取查询的所有记录
        conn.close()  # 关闭连接
    except Exception as e:
        pass

    return results


def get_domain_code(url):
    domain_code = ''
    domain_info = tldextract.extract(url)
    # print(domain_info)
    if domain_info.domain:
        if is_ip(domain_info.domain):
            domain_code = domain_info.domain
        elif domain_info.suffix:
            domain_code = f"{domain_info.domain}.{domain_info.suffix}"
            if domain_code.find('%') > -1:
                domain_code = ''
    return domain_code.strip('.')


def get_host_code(url):
    host_code = ''
    domain_info = tldextract.extract(url)
    # print(domain_info)
    if domain_info.domain:
        if is_ip(domain_info.domain):
            host_code = domain_info.domain
        elif domain_info.suffix:
            host_code = f"{domain_info.subdomain}.{domain_info.domain}.{domain_info.suffix}"
            if host_code.find('%') > -1:
                host_code = ''
    return host_code.strip('.')


def is_root_url(url):
    # 取host_code后面的部分,+1是为了末尾是斜杠‘https://new.qq.com/’这种形式的不扣分
    host_code = get_host_code(url)
    _url_remove_host_code = url[url.index(host_code) + len(host_code) + 1:]
    if len(_url_remove_host_code) > 0:
        return False
    else:
        return True


def is_ip(_str):
    p = re.compile('^((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?)$')
    if p.match(_str):
        return True
    else:
        return False


def filter_punctuation(input_str):
    """
    过滤标点符号
    :param input_str:
    :return:
    """
    re_punctuation = re.compile("[`~!@#$^&*()=|｜{}':;',\\[\\].《》<>»/?~！@#￥……&*（）——|{}【】‘；：”“'\"。，、？%+_\r|\n|\\s]")
    result = re_punctuation.sub('', input_str)
    result = result.strip()
    return result


def match_url(input_str):
    """
    匹配url，规范输出
    :param input_str:
    :return:
    """
    pattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')    # 匹配模式
    url = re.findall(pattern, input_str)
    try:
        result = url[0]
    except Exception as e:
        result = ''
    return result


def is_number(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


def is_contains_chinese(in_str):
    for _char in in_str:
        if '\u4e00' <= _char <= '\u9fa5':
            return True
    return False


def get_media_type(url):
    """
    判别 url 的媒体类型
    :param url:
    :return:
    """
    media_type = ''
    _forum_match = re.findall('bbs|forum|forumdisplay|thread|club|focus|discuz', url)
    if len(_forum_match) > 0:
        media_type = 'forum'

    return media_type


def is_need_filter(title=None, url=None, is_filter_reject_domain=False):
    score_message = {
        "status": True,
        "_title_len_score": 0,
        "_title_keyword_score": 0,
        "_url_len_score": 0,
        "_param_count_score": 0,
        "_url_filename_score": 0,
        "_url_keyword_score": 0,
    }
    score = 0
    title = title.lower()
    url = url.lower()
    host_code = get_host_code(url)
    domain_code = get_domain_code(url)

    # ==============================================================
    # title 部分-----------------------------------------------------
    # 标题长度过滤
    _title_len_score = 0
    # 先判断是否为中文标题
    _is_chinese = is_contains_chinese(title)
    if _is_chinese:
        if len(title) < 2:
            score_message = '{"status": False, "message": "title too short"}'
            score = -100
            return score, score_message
        # 看看是不是首页, 是首页则得分100，非首页按规则得分
        if is_root_url(url):
            _title_len_score = 100
        else:
            if len(title) > 10:
                score_message = '{"status": False, "message": "title too long"}'
                score = -100
                return score, score_message
            if 2 <= len(title) <= 10:
                _title_len_score_dict = {2: 100, 3: 80, 4: 80, 5: 50, 6: 40, 7: 30, 8: 20, 9: 10, 10: 0}
                _title_len_score = _title_len_score_dict[len(title)]
    else:
        if len(title) < 2:
            score_message = '{"status": False, "message": "title too short"}'
            score = -100
            return score, score_message
        # 看看是不是首页, 是首页则得分100，非首页按规则得分
        if is_root_url(url):
            _title_len_score = 100
        else:
            if len(title) > 25:
                score_message = '{"status": False, "message": "title too long"}'
                score = -100
                return score, score_message
            if 2 <= len(title) <= 25:
                _title_len_score_dict = {2: 100, 3: 90, 4: 90, 5: 80, 6: 80, 7: 80, 8: 80, 9: 70, 10: 70,
                                         11: 70, 12: 70, 13: 70, 14: 60, 15: 60, 16: 60, 17: 60, 18: 60, 19: 60, 20: 50,
                                         21: 40, 22: 30, 23: 20, 24: 10, 25: 0}
                _title_len_score = _title_len_score_dict[len(title)]

    # 标题评分，按关键词匹配
    # 完全匹配的，如‘新闻’
    _title_keyword_score = 0
    _title_keyword_score_dict = custom_keyword_score.custom_title_keyword_score_dict
    if title in _title_keyword_score_dict.keys():
        _title_keyword_score += _title_keyword_score_dict[title]
    # 部分匹配，或者多个匹配的，如‘招聘新闻’、‘南山新闻’
    # 这里的逻辑是，取一个正分最高值，和一个负分最低值，两者相加。而不是对所有匹配项累加
    if title not in _title_keyword_score_dict.keys():
        _title_keyword_score_pattern = "|".join(_title_keyword_score_dict.keys())
        _title_match = re.findall(_title_keyword_score_pattern, title)
        _re_title_score_p = 0
        _re_title_score_n = 0
        for i in _title_match:
            if i in _title_keyword_score_dict.keys():  # 防止出现找不到值的情况
                if _title_keyword_score_dict[i] > 0 and _title_keyword_score_dict[i] > _re_title_score_p:
                    _re_title_score_p = _title_keyword_score_dict[i] / 2  # 不完全匹配，分值减半
                if _title_keyword_score_dict[i] < 0 and _title_keyword_score_dict[i] < _re_title_score_n:
                    _re_title_score_n = _title_keyword_score_dict[i] / 2  # 不完全匹配，分值减半
        _title_keyword_score += round(_re_title_score_p + _re_title_score_n)

    # 标题有数字，扣分
    if bool(re.search(r'\d', title)):   # 包含数字
        if is_number(title):  # 全为数字
            _title_keyword_score += - 100
        else:
            _title_keyword_score += - 50

    # title 部分总分
    score_message['_title_len_score'] = _title_len_score
    score_message['_title_keyword_score'] = _title_keyword_score
    _title_score = round(_title_len_score/2 + _title_keyword_score/2)    # title得分，长度和关键词各加权50%
    # ==============================================================

    # ==============================================================
    # url 部分-------------------------------------------------------
    # URL长度过滤
    if len(url) < 10:
        score_message = '{"status": False, "message": "URL too short"}'
        score = -100
        return score, score_message
    if len(url) > 150:
        score_message = '{"status": False, "message": "URL too long"}'
        score = -100
        return score, score_message
    # host长度过滤
    if len(host_code) < 5:
        score_message = '{"status": False, "message": "host_code too short"}'
        score = -100
        return score, score_message
    if len(host_code) > 40:
        score_message = '{"status": False, "message": "host_code too long"}'
        score = -100
        return score, score_message
    if '···' in host_code:
        score_message = '{"status": False, "message": "host_code illegal"}'
        score = -100
        return score, score_message
    # url http 个数异常
    _http_count = url.count('http')
    if _http_count != 1:
        score_message = '{"status": False, "message": "http illegal"}'
        score = -100
        return score, score_message

    # URL除去host后的长度，越大分值越低
    # 取host_code后面的部分,+1是为了末尾是斜杠‘https://new.qq.com/’这种形式的不扣分
    _url_remove_host_code = url[url.index(host_code) + len(host_code) + 1:]
    _url_len_score = 100 - round(len(_url_remove_host_code) * 5)

    # url '&' '/'个数
    _param_count = re.findall('[/?&#]', _url_remove_host_code)
    _param_count_score = 50 - len(_param_count) * 10
    # / 数量过多，则要过滤
    _param_count_slash = re.findall('[/]', _url_remove_host_code)
    if len(_param_count_slash) > 5:
        score_message = '{"status": False, "message": "too many /"}'
        score = -100
        return score, score_message

    # url 取文件名，存在就扣分,没有文件名加分
    _url_filename = os.path.basename(url)
    if len(_url_filename) > 0 and '.' in _url_filename:
        _url_filename_score = -20
    else:
        _url_filename_score = 20

    # url 关键词计分
    # 如果是主页，则计算host；如果不是主页，则去掉host后计算
    if is_root_url(url):
        _url_keyword_score_input = host_code
    else:
        _url_keyword_score_input = _url_remove_host_code
    # URL分值计算
    _url_keyword_score_dict = custom_keyword_score.custom_url_keyword_score_dict
    _url_keyword_score_pattern = "|".join(_url_keyword_score_dict.keys())
    _url_match = re.findall(_url_keyword_score_pattern, _url_keyword_score_input)
    _re_url_score_p = 0
    _re_url_score_n = 0
    # 这里的逻辑是，取一个正分最高值，和一个负分最低值，两者相加。而不是对所有匹配项累加
    for i in _url_match:
        if i in _url_keyword_score_dict.keys():      # 防止出现‘/php’找不到值的情况
            if _url_keyword_score_dict[i] > 0 and _url_keyword_score_dict[i] > _re_url_score_p:
                _re_url_score_p = _url_keyword_score_dict[i]
            if _url_keyword_score_dict[i] < 0 and _url_keyword_score_dict[i] < _re_url_score_n:
                _re_url_score_n = _url_keyword_score_dict[i]
    _url_keyword_score = round(_re_url_score_p + _re_url_score_n)

    # url 部分总分
    # url 得分，各加权 25%
    score_message['_url_len_score'] = _url_len_score
    score_message['_param_count_score'] = _param_count_score
    score_message['_url_filename_score'] = _url_filename_score
    score_message['_url_keyword_score'] = _url_keyword_score
    _url_score = round(_url_len_score/4 + _param_count_score/4 + _url_filename_score/4 + _url_keyword_score/4)
    # ==============================================================

    # ==============================================================
    # 计算总分-------------------------------------------------------
    score = round((_title_score/2 + _url_score/2)/10-0.5)*10   # 取整十,-0.5是向下取整
    # ==============================================================

    # ==============================================================
    # 计算量较大的过滤项
    # title & url自定义垃圾词过滤
    invalid_title_pattern = "|".join(custom_str_invalid.custom_title_invalid_list)
    invalid_title = re.findall(invalid_title_pattern, title)
    if len(invalid_title) > 0:
        score_message = '{"status": False, "message": "invalid_title: ' + ','.join(invalid_title) + '"}'
        score = -100
        return score, score_message
    invalid_url_pattern = "|".join(custom_str_invalid.custom_url_invalid_list)
    invalid_url = re.findall(invalid_url_pattern, url)
    if len(invalid_url) > 0:
        score_message = '{"status": False, "message": "invalid_url: ' + ','.join(invalid_url) + '"}'
        score = -100
        return score, score_message

    # 垃圾词过滤
    junkword_pattern = "|".join(reject_junkword_list)
    junkword_match = re.findall(junkword_pattern, title)
    if len(junkword_match) > 0:
        score_message = '{"status": False, "message": "junkword_match: ' + ','.join(junkword_match) + '"}'
        score = -100
        return score, score_message

    # 垃圾域名过滤,速度比较慢，适用于无限制爬虫
    if is_filter_reject_domain:
        # reject_domain_pattern = "|".join(reject_domain_list)
        # reject_domain_match = re.findall(reject_domain_pattern, host_code)
        # if len(reject_domain_match) > 0:
        if domain_code in reject_domain_list:
            score_message = '{"status": False, "message": "reject_domain_match: ' + domain_code + '"}'
            score = -100
            return score, score_message
    # ==============================================================

    # 经过上面处理如果还没被过滤，假如是首页则置100分
    # if is_root_url(url):
    #     score_message = '{"status": True, "message": "root page"}'
    #     score = 100

    score_message = json.dumps(score_message)
    return score, score_message


if __name__ == '__main__':
    _title = '1.22.5L'
    _listpage_url = 'http://jtuniu.com/content/avi/wwwgesgcc.com.cngo.php'

    # level_score, status = is_need_filter(_title, _listpage_url, False)
    print(reject_domain_list)
    print(len(reject_domain_list))



