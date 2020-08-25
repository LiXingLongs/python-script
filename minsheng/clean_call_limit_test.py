#! /usr/bin/evn python
# -*- coding:utf-8 -*-

import pymysql
import logging
import time

################## 数据库配置 ####################
# mysql配置
MYSQL_HOST = "10.20.222.181"
MYSQL_USER = "unisound"
MYSQL_PASSWORD = "unisound@123"
MYSQL_DATABASE = "unisound_cmbc_service_db_cs2"
MYSQL_CHARSET = "utf8"

################## 日志 ####################
#日志写入文件路径
LOGGER_PATH = "D:/test/log.txt"

'''
    初始化日志工具
'''
def init_logger(path):
    # 获取logger实例，如果参数为空则返回root log
    log = logging.getLogger(path)
    # 指定logger输出格式
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
    # 文件日志
    file_handler = logging.FileHandler(path)
    file_handler.setFormatter(formatter)  # 可以通过setFormatter指定输出格式
    # 控制台日志
    # console_handler = logging.StreamHandler(sys.stdout)
    # console_handler.formatter = formatter  # 也可以直接给formatter赋值
    # 为logger添加的日志处理器
    log.addHandler(file_handler)
    # log.addHandler(console_handler)
    # 指定日志的最低输出级别，默认为WARN级别
    log.setLevel(logging.INFO)
    return log

'''
    连接mysql数据库
'''
def connect_mysql():
    try:
        # 连接database
        conn = pymysql.connect(host=MYSQL_HOST,
                               user=MYSQL_USER,
                               password=MYSQL_PASSWORD,
                               database=MYSQL_DATABASE,
                               charset=MYSQL_CHARSET)
    except:
        log.error("连接数据库异常！")
    return conn

'''
    删除满足条件的数据
'''
def delete(time):
    # 参数为空直接返回
    if not time:
        log.error("delete函数参数为空")
        return
    try:
        sql = "delete from phone_call_limit where effective_date <= '%s'" % str(time)
        log.info("delete函数执行语句为:" + sql)
        # 连接数据库
        conn = connect_mysql()
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        log.info("执行delete函数成功")
    except Exception as e:
        log.error("执行delete函数异常")
        log.error(e.args)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

'''
    查询数据总条数
'''
def query_total():
    try:
        sql = "select count(*) from phone_call_limit"
        # 连接数据库
        print("执行语句")
        currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        print(currentTime)
        cursor = conn.cursor()
        cursor.execute(sql)
        endTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        print(endTime)
        result = cursor.fetchone()
        conn.commit()
        return result
    except Exception as e:
        log.error("执行query_total函数异常")
        log.error(e.args)
        conn.rollback()

if __name__=='__main__':
    log = init_logger(LOGGER_PATH)
    conn = connect_mysql()
    # 获取当前时间
    currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    print(currentTime)
    # log.info("开始清理数据，当前时间为：" + currentTime)
    # # 业务逻辑需要的时间
    # busiTime = time.strftime('%Y-%m-%d',time.localtime(time.time()))
    # log.info("业务数据处理时间为：" + busiTime)

    # 数据处理前条数
    preData = query_total()
    # if preData:
    #     cleanPreCount = query_total()[0]
    #
    # # 处理数据库中数据
    # delete(busiTime)
    #
    # # 数据处理后条数
    # postData = query_total()
    # if postData and preData:
    #     cleanPostCount = query_total()[0]
    #     log.info("处理了" + str(cleanPreCount - cleanPostCount) + "条数据")
    # else:
    #     log.error("清理数据失败！")
    endTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    print(endTime)
    print("====================================================================")
    currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    print(currentTime)

    preData = query_total()

    endTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    print(endTime)
    conn.close()
