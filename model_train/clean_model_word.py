#!/usr/bin/env python
# -*- coding:utf-8 -*-

import logging
import pymysql
import sys
import datetime

# 数据库配置
# mysql配置
MYSQL_HOST = "10.30.0.249"
MYSQL_USER = "dev"
MYSQL_PASSWORD = "M20131209k"
MYSQL_DATABASE = "audio_train_platform"
MYSQL_CHARSET = "utf8"

#日志写入文件路径
LOGGER_PATH = "D:/test/clean_log.txt"

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

def connect_mysql():
    # 连接database
    conn = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER,password=MYSQL_PASSWORD,database=MYSQL_DATABASE,charset=MYSQL_CHARSET)
    return conn

def query_sql(sql):
    conn = connect_mysql()
    cursor = conn.cursor()
    # 执行SQL语句
    cursor.execute(sql)
    result = cursor.fetchall()
    # 关闭光标对象
    cursor.close()
    # 关闭数据库连接
    conn.close()
    return result

def insert_sql(val):
    conn = connect_mysql()
    cursor = conn.cursor()
    sql = "insert into model_train_snapshot(model_id, word, type, slow_male_count, slow_female_count, middle_male_count," \
          " middle_female_count, quick_male_count, quick_female_count, times, gmt_create, gmt_modified) values " \
          "('%s', '%s', %s, %s, %s, %s, %s, %s, %s, %s, now(), now())"%val
    # 执行SQL语句
    result = cursor.execute(sql)
    conn.commit()
    # 关闭光标对象
    cursor.close()
    # 关闭数据库连接
    conn.close()
    return result

def clean_data():
    logger.info("获取要处理的数据")
    sql = "SELECT mw.model_id,rw.word,rw.type,(SELECT count(*) FROM record_log WHERE task_id = mw.task_id AND sex = 'male') AS maleCount" \
          ",(SELECT count(*) FROM record_log WHERE task_id = mw.task_id AND sex = 'female' ) AS femaleCount " \
          ",rt.times,rt.speed FROM model_word mw " \
          "JOIN record_word rw ON rw.word_id = mw.word_id JOIN record_task rt ON rt.task_id = mw.task_id"
    logger.info(sql)
    model_word_list = query_sql(sql)
    # 获取合并后的数据
    wordDict = mergeModelWord(model_word_list)
    for word in wordDict.values():
        # 数据库中插入数据
        insert_sql(tuple(word))
"""
    合并同一个模型相同词条的数据
"""
def mergeModelWord(model_word_list):
    # 获取按速度处理之后的列表
    newWordList = split_by_speed(model_word_list)
    wordDict = {}
    for word in newWordList:
        wordKey = word[0] + word[1] + str(word[2])
        if wordDict.get(wordKey) == None:
            wordDict[wordKey] = word
        else :
            originWord = wordDict[wordKey]
            originWord[3] = (0 if originWord[3] == None else originWord[3]) + (0 if word[3] == None else word[3])
            originWord[4] = (0 if originWord[4] == None else originWord[4]) + (0 if word[4] == None else word[4])
            originWord[5] = (0 if originWord[5] == None else originWord[5]) + (0 if word[5] == None else word[5])
            originWord[6] = (0 if originWord[6] == None else originWord[6]) + (0 if word[6] == None else word[6])
            originWord[7] = (0 if originWord[7] == None else originWord[7]) + (0 if word[7] == None else word[7])
            originWord[8] = (0 if originWord[8] == None else originWord[8]) + (0 if word[8] == None else word[8])
    return wordDict
"""
    根据速度对数据进行分割
"""
def split_by_speed(model_word_list):
    wordList = []
    for word in model_word_list:
        # 获取词条语速值
        if word[6] == None or word[6] == '':
            continue
        # 按照语速拆分字段
        # 存储数据格式 modelID word type slow_male_count slow_female_count middle_male_count
        # middle_female_count quick_male_count quick_female_count times gmt_create gmt_modified
        newWord = []
        for i in range(10):
            newWord.append(0)
        newWord[0]=(word[0])
        newWord[1]=(word[1])
        newWord[2]=(word[2])
        for speed in str(word[6]).split(","):
            if speed == "1":
                newWord[7]=word[3]
                newWord[8]=word[4]
            if speed == "2":
                newWord[5]=word[3]
                newWord[6]=word[4]
            if speed == "3":
                newWord[3]=word[3]
                newWord[4]=word[4]
        newWord[9]=(word[5])
        wordList.append(newWord)
    return wordList
if __name__ == '__main__':
    logger = init_logger(LOGGER_PATH)
    logger.info("开始清理数据")
    clean_data()
    logger.info("清洗数据完成！")


