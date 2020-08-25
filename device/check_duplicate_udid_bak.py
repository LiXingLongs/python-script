#!/usr/bin/env python
# -*- coding:utf-8 -*-

import csv
import pymysql

# mysql配置
MYSQL_HOST = "10.30.0.249"
MYSQL_USER = "dev"
MYSQL_PASSWORD = "M20131209k"
MYSQL_DATABASE = "device_center"
MYSQL_CHARSET = "utf8"

#日志路径
ERROR_DATA_FILE_PATH = "d:/test/error.txt"

def checkFile():
    # 所有udid
    udids = []
    # 重复元素个数
    repeatnum = 0
    with open('C:\\Users\\admin\\Desktop\\car_tzxing2test_v3_dsn_white-20200731-(1)-40w.csv', 'r') as f:
        reader = csv.reader(f)

        for rowList in reader:
            row = rowList[0].lower()
            if row in udids:
                repeatnum+=1
                print(row)
            else:
                udids.append(row)
            if len(udids) % 10000 == 0:
                print("重复元素个数为:" + str(repeatnum))
                print ("udid总个数为:" + str(len(udids)))

    with open('C:\\Users\\admin\\Desktop\\car_tzxing2test_v3_dsn_white-20200731-(2)-40w.csv', 'r') as f:
        reader = csv.reader(f)

        for rowList in reader:
            row = rowList[0].lower()
            if row in udids:
                repeatnum+=1
                print(row)
            else:
                udids.append(row)
            if len(udids) % 10000 == 0:
                print("重复元素个数为:" + str(repeatnum))
                print ("udid总个数为:" + str(len(udids)))
    print("重复元素个数为:" + str(repeatnum))
    print ("udid总个数为:" + str(len(udids)))

if __name__== '__main__':
    checkFile()
