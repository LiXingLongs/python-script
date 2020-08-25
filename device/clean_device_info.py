#!/usr/bin/env python
# -*- coding:utf-8 -*-

from pymongo import MongoClient
import pymysql
import requests
import json
import time
import sys
#重新加载sys模块
reload(sys)
#重新设置字符集
sys.setdefaultencoding("utf-8")

################## 数据库配置 ####################
# mongos设备中心配置
MONGO_HOST_DEVICE = "10.20.222.63"
MONGO_PORT = 27017
MONGO_USER_DEVICE = "root"
MONGO_PASSWORD_DEVICE = "123456"
# mongo产测授权配置
MONGO_HOST_PAUTH = "10.20.222.63"
MONGO_USER_PAUTH = "root"
MONGO_PASSWORD_PAUTH = "123456"
# mysql配置
MYSQL_HOST = "10.30.0.249"
MYSQL_USER = "dev"
MYSQL_PASSWORD = "M20131209k"
MYSQL_DATABASE = "device_center"
MYSQL_CHARSET = "utf8"

################## 业务配置 ######################
# 数据服务访问地址
DATA_SERVICE_URL = "http://10.20.222.151:8080/data-location/v1/ipinfo/get_ip_info/"
# 错误数据写入的文件路径
ERROR_DATA_FILE_PATH = "d:/test/error.txt"

def conn_device():
    # 连接mongo数据库
    conn = MongoClient(MONGO_HOST_DEVICE, MONGO_PORT)
    # 指定连接数据库
    db = conn.admin
    # 授权
    db.authenticate(MONGO_USER_DEVICE, MONGO_PASSWORD_DEVICE)
    # 返回连接
    return conn

def conn_pauth():
    # 连接mongo数据库
    conn = MongoClient(MONGO_HOST_PAUTH, MONGO_PORT)
    # 指定连接数据库
    db = conn.admin
    # 授权
    db.authenticate(MONGO_USER_PAUTH, MONGO_PASSWORD_PAUTH)
    # 返回连接
    return conn


def connect_mysql():
    # 连接database
    conn = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER,password=MYSQL_PASSWORD,database=MYSQL_DATABASE,charset=MYSQL_CHARSET)
    return conn

# 根据udid查询授权时间
def find_auth_time_by_udid(udid):
    conn = connect_mysql()
    cursor = conn.cursor()
    sql = "select * from activate_rule_value where ruleValue = '" + udid + "'"
    # 执行SQL语句
    cursor.execute(sql)
    result = cursor.fetchone()
    # 关闭光标对象
    cursor.close()
    # 关闭数据库连接
    conn.close()
    return result

def clean_data():
    con_device = conn_device()
    # 连接设备中心库
    db = con_device.device_center
    device_collection = db.device
    device_activate_collection = db.device_activate

    # 连接产测授权库
    con_pauth = conn_pauth()
    db = con_pauth.product_auth
    auth_collection = db.product_auth_info

    # 获取数据条数
    total = device_collection.find({'dr':{'$exists':False}}).count()
    # 计数
    curr = 0
    # 查询设备表数据 增量查询  已经更新的数据不在进行更新
    for device_data in device_collection.find({'dr':{'$exists':False}}):
        try:
            udid = device_data["udid"]
            deviceIp = device_data.get("deviceIp")
            _id = device_data["_id"]
            # 根据udid查询设备激活信息表数据
            activate_data = device_activate_collection.find_one({"udid":udid})
            curr+=1
            if curr % 500 == 0:
                print("清洗进度：" + str(curr) + "/" + str(total))
            if activate_data == None:
                write_error_data(device_data)
                continue
            # 获取激活信息
            appKey = activate_data["appKey"]
            deviceSn = activate_data["deviceSn"]
            activateTime = activate_data["activateTime"]
            if deviceIp == None:
                location = ""
            else:
                # 获取激活地点
                location = get_location(deviceIp) if get_location(deviceIp) != None else ""

            # 获取授权信息
            auth_time = ""
            for auth_date in auth_collection.find({"authFlag":1,"transferAuthFlag":0,"udid":udid}):
                if not auth_date == None:
                    # 授权时间多条时取第一条
                    auth_time_stamp = auth_date["updateTime"]
                    auth_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(auth_time_stamp / 1000))
                    break

            # 获取授权失败 通过查询规则值入库时间进行更新
            if auth_time == "":
                data = find_auth_time_by_udid(udid)
                if not data == None:
                    auth_time = data[3]
            # 拼接mongo语句
            # sentence = '{"firstActivateTime":"' + activateTime + '","appKey":"' + appKey + '","deviceSn":"' \
            #            + deviceSn + '","firstActivateLocation":"' + location + '","authTime":"' + auth_time + '"}'
            # print(sentence)
            device_data["firstActivateTime"] = activateTime
            device_data["appKey"] = appKey
            device_data["deviceSn"] = deviceSn
            device_data["firstActivateLocation"] = location
            device_data["authTime"] = auth_time
            device_data["dr"] = 0
            device_collection.bulk_write({"_id": _id}, device_data)
        except Exception as err:
            write_error_data(device_data)

# 访问数据服务获取ip所属地址
def get_location(ip):
    # ip为空直接返回空
    if len(ip) <= 0:
        return ""
    s = requests.session()
    s.keep_alive = False
    res = requests.get(DATA_SERVICE_URL + ip)
    content = json.loads(res.text)
    res_code = content["code"]
    if res_code == "0":
        city = content["data"]["city"]
        province = content["data"]["province"]
        # 特殊处理直辖市
        return dual_location(province, city)
    else:
        return None

# 将错误的数据写入文件中
def write_error_data(data):
    file_handle = open(ERROR_DATA_FILE_PATH, 'a+')
    try:
        file_handle.write(json.dumps(data) + "\n")
    except :
        print("数据写入错误:" + data)
    finally:
        file_handle.close()

def dual_location(province, city):
    special_city = {u'北京', u'上海', u'天津', u'重庆'}
    if province in special_city:
        return province + "市"
    if city in special_city:
        return city + "市"
    if province == "内网IP":
        return "内网IP"
    if not province == "" or province == None:
        province = province + "省"
    if not city == "" or city == None:
        city = province + "市"
    return province + city
if __name__ == '__main__':
    clean_data()

    print("数据清洗结束")