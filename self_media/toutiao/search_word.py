#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : search_word.py
# @Author: Cedar
# @Date  : 2020/5/19
# @Desc  :


search_word_list = []
fn = open('word.txt', 'r', encoding='UTF-8')  # 打开文件
for i in fn:
    i = i.replace('\n', '')
    search_word_list.append(i)
fn.close()  # 关闭文件


if __name__ == '__main__':
    print(search_word_list[0:100])
