#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : output_data_listpage_to_listpage.py
# @Author: Cedar
# @Date  : 2020/4/21
# @Desc  :

import pymysql


# import lib.common as common
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
        print(e)

    return results


def main(start, end):
    extractor_116 = {'host': '192.168.1.116', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
    extractor_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}

    select_column = f"select * from listpage_url where ListPage_URL_ID BETWEEN {start} AND {end};"

    try:
        results = query_mysql(extractor_116, select_column)
        print('116 select count: ', len(results))
        column_list = []
        for item in results:
            ListPage_URL_ID = item['ListPage_URL_ID']
            Client_ID = item['Client_ID']
            District_ID = item['District_ID']
            Country_ID = item['Country_ID']
            Province_ID = item['Province_ID']
            City_ID = item['City_ID']
            ListPage_Group_ID = item['ListPage_Group_ID']
            Website_No = item['Website_No']
            ListPage_URL = item['ListPage_URL']
            ListPage_URL_MD5 = item['ListPage_URL_MD5']
            Record_GUID = item['Record_GUID']
            Input_URL = item['Input_URL']
            Is_Important = item['Is_Important']
            ListPage_Save_Rule = item['ListPage_Save_Rule']
            ListPage_Title = item['ListPage_Title'].replace("'", '"').replace("\\", '')   # 注意这里变换，容易出错
            ListPage_Search_Keyword = item['ListPage_Search_Keyword']
            ListPage_Page_No = item['ListPage_Page_No']
            ListPage_User_Data = item['ListPage_User_Data']
            Order_No = item['Order_No']
            Is_Enabled = item['Is_Enabled']
            LinkURL_Include_Keywords_CommaText = item['LinkURL_Include_Keywords_CommaText']
            LinkURL_Exclude_Keywords_CommaText = item['LinkURL_Exclude_Keywords_CommaText']
            LinkURL_Min_Length = item['LinkURL_Min_Length']
            LinkText_Include_Keywords_CommaText = item['LinkText_Include_Keywords_CommaText']
            LinkText_Exclude_Keywords_CommaText = item['LinkText_Exclude_Keywords_CommaText']
            LinkText_Min_Length = item['LinkText_Min_Length']
            Group_Code = item['Group_Code']
            URL_List_Order_Type = item['URL_List_Order_Type']
            Extracted_Flag = item['Extracted_Flag']
            ReTry_Times = item['ReTry_Times']
            Checked_Flag = item['Checked_Flag']
            Last_Valid_Link_Count = item['Last_Valid_Link_Count']
            Last_Check_Status = item['Last_Check_Status']
            Last_Update_User_ID = item['Last_Update_User_ID']
            Last_Check_HttpCode = item['Last_Check_HttpCode']
            Last_Check_Score_DOM = item['Last_Check_Score_DOM']
            Last_Check_Score_Text = item['Last_Check_Score_Text']
            Domain_Code = item['Domain_Code']
            Host_Code = item['Host_Code']
            column = f"""
            (
                {ListPage_URL_ID},
                {Client_ID},
                {District_ID},
                {Country_ID},
                {Province_ID},
                {City_ID},
                {ListPage_Group_ID},
                '{Website_No}',
                '{ListPage_URL}',
                '{ListPage_URL_MD5}',
                '{Record_GUID}',
                '{Input_URL}',
                {Is_Important},
                {ListPage_Save_Rule},
                '{ListPage_Title}',
                '{ListPage_Search_Keyword}',
                {ListPage_Page_No},
                '{ListPage_User_Data}',
                {Order_No},
                {Is_Enabled},
                '{LinkURL_Include_Keywords_CommaText}',
                '{LinkURL_Exclude_Keywords_CommaText}',
                {LinkURL_Min_Length},
                '{LinkText_Include_Keywords_CommaText}',
                '{LinkText_Exclude_Keywords_CommaText}',
                {LinkText_Min_Length},
                '{Group_Code}',
                '{URL_List_Order_Type}',
                '{Extracted_Flag}',
                {ReTry_Times},
                '{Checked_Flag}',
                {Last_Valid_Link_Count},
                '{Last_Check_Status}',
                {Last_Update_User_ID},
                {Last_Check_HttpCode},
                {Last_Check_Score_DOM},
                {Last_Check_Score_Text},
                '{Domain_Code}',
                '{Host_Code}'
            )
            """
            column_list.append(column)

        # 批量插入
        values = ",".join(column_list)
        values = values.replace("'None'", "''")
        values = values.replace("None", "null")
        insert_column = f"""insert ignore into listpage_url(
                ListPage_URL_ID,
                Client_ID,
                District_ID,
                Country_ID,
                Province_ID,
                City_ID,
                ListPage_Group_ID,
                Website_No,
                ListPage_URL,
                ListPage_URL_MD5,
                Record_GUID,
                Input_URL,
                Is_Important,
                ListPage_Save_Rule,
                ListPage_Title,
                ListPage_Search_Keyword,
                ListPage_Page_No,
                ListPage_User_Data,
                Order_No,
                Is_Enabled,
                LinkURL_Include_Keywords_CommaText,
                LinkURL_Exclude_Keywords_CommaText,
                LinkURL_Min_Length,
                LinkText_Include_Keywords_CommaText,
                LinkText_Exclude_Keywords_CommaText,
                LinkText_Min_Length,
                Group_Code,
                URL_List_Order_Type,
                Extracted_Flag,
                ReTry_Times,
                Checked_Flag,
                Last_Valid_Link_Count,
                Last_Check_Status,
                Last_Update_User_ID,
                Last_Check_HttpCode,
                Last_Check_Score_DOM,
                Last_Check_Score_Text,
                Domain_Code,
                Host_Code
        ) 
        values{values};"""
        # print(insert_column)
        print('118 insert count: ', len(column_list))
        if len(column_list) > 0:
            query_mysql(extractor_118, insert_column)

    except Exception as e:
        print(e)


if __name__ == '__main__':
    for i in range(5765, 11580):
    # for i in range(5765, 5766):
        print(i*10000, (i+1)*10000-1)
        main(i*10000, (i+1)*10000-1)
