#!/usr/bin/env python
# -*- coding:utf-8 -*-

from pymongo import MongoClient
import pymysql

# 数据库配置
# mongo配置
MONGO_HOST = "10.20.222.63"
MONGO_PORT = 27017
MONGO_USER = "nlu_skill_user"
MONGO_PASSWORD = "123456"
# mysql配置
MYSQL_HOST = "10.30.0.249"
MYSQL_USER = "dev"
MYSQL_PASSWORD = "M20131209k"
MYSQL_DATABASE = "usk-platform"
MYSQL_CHARSET = "utf8"

def connect_mongo():
    # 连接mongo数据库
    conn = MongoClient(MONGO_HOST, MONGO_PORT)
    # 指定连接数据库
    db = conn.nlu_skill_platform
    # 授权
    db.authenticate(MONGO_USER, MONGO_PASSWORD)
    # 返回连接
    return conn

def connect_mysql():
    # 连接database
    conn = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER,password=MYSQL_PASSWORD,database=MYSQL_DATABASE,charset=MYSQL_CHARSET)
    return conn

def find_id_by_url(sql):
    conn = connect_mysql()
    cursor = conn.cursor()
    # 执行SQL语句
    cursor.execute(sql)
    result = cursor.fetchone()
    # 关闭光标对象
    cursor.close()
    # 关闭数据库连接
    conn.close()
    return result

def clean_data():
    conn = connect_mongo()
    db = conn.nlu_skill_platform
    collection = db.skill
    findNum = 0
    print("更新数据-----------------------------------")
    for data in collection.find({"skill.intents.apiResourceUrl":{'$exists':'true'}}):
        _id = data["_id"]
        # 获取所有意图
        intents = data["skill"]["intents"]
        # 查找包含apiResourceUrl意图
        for intent in intents:
            try:
                # 同时匹配url以http和https开头的
                subApiResourceUrl = intent["apiResourceUrl"].split("://")[1]
                httpUrl = "http://" + subApiResourceUrl
                httpsUrl = "https://" + subApiResourceUrl
                sql = "select id from resource where api_resource_url='" + httpUrl + "' or api_resource_url='" + httpsUrl + "'"
                # 通过url查询mysql对应的id
                id = find_id_by_url(sql)[0]
                # 添加新的字段 apiResourceId
                intent["apiResourceId"] = id
            except:
                # 不存在key的数据直接跳过 不进行处理
                pass
        # 更新数据
        collection.update_one({"_id":_id}, {'$set':{"skill":data["skill"]}})
        print(data)
        findNum+=1
    print("清洗数据" + str(findNum) + "条")

if __name__ == '__main__':
    clean_data()
    print("清洗数据完成！")


