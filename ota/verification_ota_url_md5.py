#!/usr/bin/env python
# -*- coding:utf-8 -*-

import csv
import urllib2
import json

"""
读取文件，转成一个集合
"""
def read_csv_file():
    file_path = "D:/workspace/test_scrip/ota/ota_data.csv"
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        result = list(reader)
        f.close()
        return result

"""
校验数据格式是否正确
"""
def check_data_format(ota_url):
    if ota_url == None or ota_url == "" or not ota_url.startswith("http"):
        return False
    return True


def check_url_is_useful(ota_url):
    try:
        res = urllib2.urlopen(urllib2.Request(ota_url))
        code = res.getcode()
        if code == 200:
            return True
        return False
    except Exception,e:
        print e
        return False

# def check_md5_is_correct(md5, ota_url):

def check():
    resources = read_csv_file()
    for line in resources:
        url = line[7]
        if check_data_format(url):
            if not check_url_is_useful(url):
                print ("不可用连接：" + json.dumps(line))
        else :
            print("校验格式错误：" + json.dumps(line))

if __name__ == '__main__':
    check()
    print("校验完成")