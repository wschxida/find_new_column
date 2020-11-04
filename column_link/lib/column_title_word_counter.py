#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : column_title_word_counter.py
# @Author: Cedar
# @Date  : 2020/4/23
# @Desc  : 统计listpage_title词频，找出场景栏目标题


import re  # 正则表达式库
import collections  # 词频统计库
import jieba  # 结巴分词
import pymysql
import hashlib


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


def get_word_count(string_data, top_count=10000):
    # 文本预处理
    pattern = re.compile(u'\t|\n|\.|-|:|;|\)|\(|\?|"')  # 定义正则表达式匹配模式
    string_data = re.sub(pattern, '', string_data)  # 将符合模式的字符去除

    # 文本分词
    print("--jieba分词开始--")
    seg_list_exact = jieba.cut(string_data, cut_all=False)  # 精确模式分词
    object_list = []
    remove_words = [u'的', u'，', u'和', u'是', u'随着', u'对于', u'对', u'等', u'能', u'都', u'。', u' ', u'、', u'中', u'在', u'了',
                    u'通常', u'如果', u'我们', u'需要']  # 自定义去除词库

    for word in seg_list_exact:  # 循环读出每个分词
        if word not in remove_words:  # 如果不在去除词库中
            if len(word) > 1:  # 单词长度
                object_list.append(word)  # 分词追加到列表
    print("--分词结束--")

    # 词频统计
    print("--词频统计--")
    word_counts = collections.Counter(object_list)  # 对分词做词频统计
    return word_counts.most_common(top_count)  # 获取前10000最高频的词


def get_listpage_title_word_count():
    # 连接mysql
    config = {
        'host': '192.168.1.119',
        'port': 3306,
        'user': 'root',
        'passwd': 'poms@db',
        'db': 'mymonitor',
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor
    }

    # 1.查询操作
    # 编写sql 查询语句
    start_no = '0'
    offset = '100000'
    sql = "select ListPage_Title from listpage_url where 1=1 " \
          "and Website_No not in (select Website_No from website where Extract_Config_No='CS4235' or Website_Name like'%搜索%') and Website_No in (select Website_No from website where Media_Type_Code='N' and (Website_Name like'%腾讯%' or Website_Name like'%新浪%' or Website_Name like'%搜狐%' or Website_Name like'%网易%')) " \
          f"limit {start_no},{offset}"

    print("--开始从数据库读取数据--")

    results = query_mysql(config, sql)  # 获取查询的所有记录
    # 遍历结果
    string_data = ''
    for row in results:
        string_data = string_data + '\n' + row['ListPage_Title']

    print("--已从数据库读取数据--")

    # 源数据
    # fn = open('listpage_title.txt', 'w', encoding='UTF-8')  # 打开文件
    # fn.write(string_data)
    # fn.close()  # 关闭文件
    # print("--已写入源数据到txt--")

    # 统计词频
    word_count_result = get_word_count(string_data, 1000)
    print(word_count_result)

    # 写入结果
    print("--写入结果--")
    fn = open('word_count_result.txt', 'w', encoding='UTF-8')  # 打开文件
    for i in word_count_result:
        if i[1] > 1:
            fn.write(str(i))
            fn.write('\n')
    fn.close()  # 关闭文件

    fn = open('word.txt', 'w', encoding='UTF-8')  # 打开文件
    for i in word_count_result:
        if i[1] > 1:
            fn.write(i[0])
            fn.write('\n')
    fn.close()  # 关闭文件


if __name__ == '__main__':
    get_listpage_title_word_count()
