#! /usr/bin/evn python
# -*- coding:utf-8 -*-

import pymysql
import logging
import time
import threading

################## 数据库配置 ####################
# mysql配置
MYSQL_HOST = "10.30.0.249"
MYSQL_USER = "dev"
MYSQL_PASSWORD = "M20131209k"
MYSQL_DATABASE = "minsheng3"
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
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        log.info("执行delete函数成功")
    except Exception as e:
        log.error("执行delete函数异常")
        log.error(e.args)
        conn.rollback()

'''
    查询满足删除条件的数据
'''
def query_by_time(time):
    # 参数为空直接返回
    if not time:
        log.error("query_by_time函数参数为空")
        return
    try:
        sql = "select * from phone_call_limit where effective_date <= '%s'" % str(time)
        log.info("query_by_time函数执行语句为:" + sql)
        cursor = conn.cursor()
        cursor.execute(sql)
        cursor.close()
        return cursor.fetchall()
    except Exception as e:
        log.error("执行query_by_time函数异常")
        log.error(e.args)
'''
    批量插入数据
'''
def batch_insert_data(detailList):
    try:
        cursor = conn.cursor()
        cursor.executemany("INSERT INTO `dial_detail` (`id`, `task_id`, `phone_num`, `user_name`, `talk_cost_time`, `ring_cost_time`, `record_audio`, `purpose_level`, `dial_time`, `dial_status`, `record_text`, `detail_uuid`, `dial_count`, `sms_status`, `sms_send_date`, `start_time`, `template_id`, `busiType`) "
                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", detailList)
        conn.commit()
        cursor.close()
        log.info("执行batch_insert_data函数成功")
    except Exception as e:
        log.error("执行batch_insert_data函数异常")
        log.error(e.args)
        conn.rollback()

'''
    查询数据总条数
'''
def query_total():
    try:
        sql = "select count(*) from phone_call_limit"
        cursor = conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchone()
        cursor.close()
        return result
    except Exception as e:
        log.error("执行query_total函数异常")
        log.error(e.args)

if __name__=='__main__':
    log = init_logger(LOGGER_PATH)
    try:
        # 获取当前时间
        currentTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        log.info("开始操作时间为：" + currentTime)
        # 连接数据库
        conn = connect_mysql()
        list = []
        for i in range(100001, 200001):
        # for i in range(100001, 100006):
            tuple1 = (i, 1, '1010', '五七六先生', 35, 2, 'http://10.20.222.238:9238/2,05b46aff74eb12', 'intention_A', '2020-08-12 03:53:30', 2, '{"userName":"五七六先生"}', 'fbf0c7a615f84f10ab443a7231f266a3', 1, 2, '2020-08-12 03:54:08', '2020-08-12 03:53:22', 10001, 'bill_installment')
            list.append(tuple1)
            if i % 2000 == 0:
                batch_insert_data(list)
                log.info("当前执行索引到:" + str(i))
                list=[]
        endTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        log.info("结束操作时间为：" + endTime)

    finally:
        conn.close()
