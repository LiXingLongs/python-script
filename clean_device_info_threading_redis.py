#!/usr/bin/env python
# -*- coding:utf-8 -*-

from pymongo import MongoClient
import pymysql
import redis
import requests
import json
import time
import sys
import traceback
import threading
import logging
#重新加载sys模块0
reload(sys)
#重新设置字符集
sys.setdefaultencoding("utf-8")
success_num = 0

################## 数据库配置 ####################
# mongos设备中心配置
MONGO_HOST_DEVICE = "dds-8vb86b944dbf2c741.mongodb.zhangbei.rds.aliyuncs.com"
MONGO_PORT = 3717
MONGO_USER_DEVICE = "device_user"
MONGO_PASSWORD_DEVICE = "yunzhisheng!@#$"
# mongo产测授权配置
MONGO_HOST_PAUTH = "dds-8vb86b944dbf2c741.mongodb.zhangbei.rds.aliyuncs.com"
MONGO_USER_PAUTH = "product_auth_user"
MONGO_PASSWORD_PAUTH = "authpassw0rd"
# mysql配置
MYSQL_HOST = "rm-8vb9459l04s7p5a0j.mysql.zhangbei.rds.aliyuncs.com"
MYSQL_USER = "dev"
MYSQL_PASSWORD = "M20131209k"
MYSQL_DATABASE = "device_center"
MYSQL_CHARSET = "utf8"
# redis配置
REDIS_HOST = "r-8vbm3czom374xg4i1m.redis.zhangbei.rds.aliyuncs.com"
REDIS_PORT = "6379"
REDIS_EXPIR_TIME =15 * 24* 60 * 60 # 有效时间10天
REDIS_KEY_PREFIX = "redis_dc_clean_data_"

################## 业务配置 ######################
# 数据服务访问地址
DATA_SERVICE_URL = "http://172.18.10.172:8080/data-location/v1/ipinfo/get_ip_info/"
# 错误数据写入的文件路径
ERROR_DATA_FILE_PATH = "/data1/mafang/device_user/log/clean_device_error.txt"
#info日志写入文件路径
LOGGER_PATH = "/data1/mafang/device_user/log/clean_device_info.txt"


con_device = None
con_pauth = None
con_redis = None
# 授权信息字典
auth_dict = {}
# conn_mysql = None

def init_logger(path):
    # 获取logger实例，如果参数为空则返回root log
    log = logging.getLogger(path)
    # 指定logger输出格式
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
    # 文件日志
    file_handler = logging.FileHandler(path)
    file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式
    # 控制台日志
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.formatter = formatter  # 也可以直接给formatter赋值
    # 为logger添加的日志处理器
    log.addHandler(file_handler)
    log.addHandler(console_handler)
    # 指定日志的最低输出级别，默认为WARN级别
    log.setLevel(logging.INFO)
    return log
    pass
# 连接设备中心mongo
def conn_device():
    # 连接mongo数据库
    conn = MongoClient(MONGO_HOST_DEVICE, MONGO_PORT)
    # 指定连接数据库
    db = conn.device_center
    # 授权
    db.authenticate(MONGO_USER_DEVICE, MONGO_PASSWORD_DEVICE)
    return conn

# 连接产测授权mongo
def conn_pauth():
    # 连接mongo数据库
    conn = MongoClient(MONGO_HOST_PAUTH, MONGO_PORT)
    # 指定连接数据库
    db = conn.product_auth
    # 授权
    db.authenticate(MONGO_USER_PAUTH, MONGO_PASSWORD_PAUTH)
    # 返回连接
    return conn

# 连接设备信息mysql
def connect_mysql():
    # 连接database
    conn = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER,password=MYSQL_PASSWORD,database=MYSQL_DATABASE,charset=MYSQL_CHARSET)
    return conn

# 连接redis
def connect_redis():
    # 连接database
    conn = redis.Redis(REDIS_HOST, REDIS_PORT)
    return conn

# 根据udid批量查询授权时间
def find_auth_time_by_udid(param):
    conn_mysql = connect_mysql()
    cursor = conn_mysql.cursor()
    sql = 'select * from activate_rule_value where ruleValue in (%s)' % ','.join(['%s'] * len(param))
    # 执行SQL语句
    cursor.execute(sql, param)
    result = cursor.fetchall()
    # 关闭光标对象
    cursor.close()
    # 关闭数据库连接
    conn_mysql.close()
    return result

# 清洗数据函数 data_list要处理的数据集合
lock = threading.Lock()
def clean_data(data_list):
    # 多线程共享变量 当前成功条数
    global success_num

    # 连接设备中心库
    db = con_device.device_center
    device_activate_collection = db.device_activate

    # 整合要处理数据的udid
    param_udid_list = []
    for device_data in data_list:
        param_udid_list.append(device_data["udid"])

    # 批量查询激活信息数据
    activate_dict = {}
    if not param_udid_list == []:
        for activate_data_te in device_activate_collection.find({"udid":{"$in":param_udid_list}}).sort("activateTime"):
            if activate_data_te == None:
                continue
            activate_dict[activate_data_te["udid"]] = activate_data_te

    # 批量查询mysql授权时间
    mysql_auth_dict = {}
    if not param_udid_list == []:
        for auth_data_mq in find_auth_time_by_udid(param_udid_list):
            if auth_data_mq == None:
                continue
            if auth_data_mq[3] == None or auth_data_mq[2] == None:
                continue
            mysql_auth_dict[auth_data_mq[2]] = auth_data_mq[3].strftime('%Y-%m-%d %H:%M:%S')

    # 获取当前数据条数
    total = len(data_list)
    # 计数
    curr = 0
    # 获取数据库批量操作
    bulk = device_collection.initialize_ordered_bulk_op()
    # 查询设备表数据 增量查询  已经更新的数据不在进行更新
    for device_data in data_list:
        curr+=1
        try:
            udid = device_data["udid"]
            _id = device_data["_id"]
            # 获取授权时间
            auth_time = ""
            # 匹配mongo中的授权时间
            if not auth_dict.get(udid) == None:
                auth_time_stamp = auth_dict.get(udid)
                auth_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(auth_time_stamp / 1000))
            # 匹配mysql中的授权时间
            if auth_time == "":
                auth_time = mysql_auth_dict.get(udid)
            device_data["authTime"] = auth_time

            # 根据udid查询设备激活信息表数据
            activate_data = activate_dict.get(udid)
            if activate_data == None:
                device_data["dr"] = 0
                bulk.find({'_id': _id}).update({'$set': device_data})
                write_error_data(device_data)
            else:
                # 获取激活信息
                appKey = activate_data["appKey"]
                deviceSn = activate_data["deviceSn"]
                activateTime = activate_data["activateTime"]
                # 获取激活地点
                # 1.先取激活信息的激活Ip
                # 2.激活信息不存在，取设备信息的激活IP
                # 3.根据Ip请求数据服务获取具体地点 存放到ip字典中{ip：地点}
                deviceIp = activate_data.get("firstActicateIp")
                if deviceIp == None:
                    deviceIp = device_data.get("deviceIp")
                if deviceIp == None:
                    location = ""
                else:
                    redis_key = REDIS_KEY_PREFIX + deviceIp
                    location = con_redis.get(redis_key)
                    if location == None:
                        location = get_location(deviceIp) if get_location(deviceIp) != None else ""

                # 构造保存的设备信息
                device_data["firstActivateTime"] = activateTime
                device_data["appKey"] = appKey
                device_data["deviceSn"] = deviceSn
                device_data["firstActivateLocation"] = location
                device_data["dr"] = 0
                bulk.find({'_id': _id}).update({'$set': device_data})
            # 五千条数据执行一次  注意：条数需要和每份处理数据条数一样
            if curr % 5000 == 0 or curr >= total:
                lock.acquire()
                bulk.execute()
                lock.release()

        except TypeError as te:
            err_logger.error(traceback.format_exc())
            pass
        except Exception as err:
            err_logger.error(traceback.format_exc())
            write_error_data(device_data)
    lock.acquire()
    success_num += curr
    lock.release()
    logger.info("清洗进度：" + str(success_num))

# 访问数据服务获取ip所属地址
def get_location(ip):
    global con_redis
    # ip为空直接返回空
    if len(ip) <= 0:
        return ""
    s = requests.session()
    s.keep_alive = True
    res = requests.get(DATA_SERVICE_URL + ip)
    content = json.loads(res.text)
    res_code = content["code"]
    if res_code == "0":
        city = content["data"]["city"]
        province = content["data"]["province"]
        # 特殊处理直辖市
        result = dual_location(province, city)
        redis_key = REDIS_KEY_PREFIX + ip
        # 将请求值写入内存
        con_redis.set(redis_key, result, REDIS_EXPIR_TIME)
        return result
    else:
        return ""

# 将错误的数据写入文件中
def write_error_data(data):
    file_handle = open(ERROR_DATA_FILE_PATH, 'a+')
    try:
        file_handle.write(json.dumps(data) + "\n")
    except :
        err_logger.info("数据写入错误:" + data)
    finally:
        file_handle.close()

# 直辖市特殊处理
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
        city = city + "市"
    return province + city

if __name__ == '__main__':
    logger = init_logger(LOGGER_PATH)
    err_logger = init_logger(ERROR_DATA_FILE_PATH)
    logger.info("开始清洗数据--------")
    load_start_time = start = time.time()
    con_device = conn_device()
    con_pauth = conn_pauth()
    # conn_mysql = connect_mysql()
    # 连接设备中心库
    db = con_device.device_center
    device_collection = db.device

    # 连接产测授权库
    db = con_pauth.product_auth
    auth_collection = db.product_auth_info

    # 连接redis
    con_redis = connect_redis()

    # 将授权信息加载到内存中
    auth_total = auth_collection.find({"authFlag":1,"transferAuthFlag":0}).count()
    # 单次查询数据条数
    per_query_num = 50000
    query_auth_count = 0
    while (query_auth_count < auth_total):
        for auth_data in  auth_collection.find({"authFlag":1,"transferAuthFlag":0,"delFlag":0}, {"udid":1,"updateTime":1}).skip(query_auth_count).limit(per_query_num):
            if auth_data == None:
                continue
            auth_dict[auth_data.get("udid")] = auth_data.get("updateTime")
        query_auth_count += per_query_num
    load_end_time = time.time()
    logger.info("加载数据时间：" + str(load_end_time - load_start_time))

    start = time.time()
    # 获取要清洗的数据总条数
    total_count = device_collection.find({'dr':{'$exists':False}}).count()
    logger.info("总数据条数："  + str(total_count))
    # 启动线程个数
    thread_num = 5
    # 每份分隔的数据数量 要和单次批量插入的数据数量保持一致
    split_num = 5000
    # 分批次清洗数据 多线程清洗数据
    while(True):
        # 早上八点到晚上十点  修改为一个线程执行任务
        curr_hour = time.strftime('%H',time.localtime(time.time()))
        if (7< int(curr_hour) <22):
            thread_num = 1
        else:
            thread_num = 5

        logger.info("当前线程数：" + str(thread_num))
        threads = []
        for i in range(0, thread_num):
            # 获取要清洗的数据
            data_list = device_collection.find({'dr':{'$exists':False}}).skip(i*split_num).limit(split_num)
            threads.append(threading.Thread(target=clean_data, args=(list(data_list),)))

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        if success_num >= total_count:
          break
    end = time.time()
    total_time = (end - start)
    logger.info("消耗时间为："  + str(total_time))
    logger.info("清洗结束")
