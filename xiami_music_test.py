#!/usr/bin/env python
# -*- coding:utf-8 -*-

import json
import test_mongo_connector

def read_file(read_path):
    keyList = []
    repeatNum = 0;
    # 读取文件内容
    file = open(read_path, "r")
    for line in file:
        # 解析每行数据
        jsonData = json.loads(line)
        dataKey = jsonData.get("key")
        # 查看mongo中是否含有这个值
        if not dataKey in keyList:
            keyList.append(dataKey)
        else:
            repeatNum += 1
    return repeatNum



if __name__ == '__main__':
    repeatNum = read_file("C:/Users/admin/Desktop/music_recommend_xiami_2018110519")
    print(repeatNum)