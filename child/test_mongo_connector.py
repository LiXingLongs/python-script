#!/usr/bin/env python
# -*- coding:utf-8 -*-

from pymongo import MongoClient
import Queue
import time
import threading

def connect_mongo():
    # 连接mongo数据库
    conn = MongoClient("10.20.222.63", 27017)
    # 指定连接数据库
    db = conn.admin
    # 授权
    db.authenticate("root", "123456")
    # 返回连接
    return conn

def findDataCloudServiceData(skipValue, limitValue):
    conn = connect_mongo()  
    db = conn.data_cloud_service
    collection = db.child_audio_info
    return collection.find().skip(skipValue).limit(limitValue)


def saveTestData(datas):
    conn = connect_mongo()
    db = conn.test_mongo
    collection = db.child_audio_info
    for data in datas:
        collection.save(data)

def delTestData():
    conn = connect_mongo()
    db = conn.test_mongo
    collection = db.child_audio_info
    mydict={}
    for data in collection.find().skip(0).limit(1):
        mydict["_id"]=data["_id"]
        collection.delete_one(mydict)

num = 0
def product(name):
    global num
    queryNum = 0
    while num < 50000:
        print("生产执行任务线程" + name)
        data = findDataCloudServiceData(queryNum, 1)
        time.sleep(1)
        saveTestData(data)
        queryNum += 1
        num += 1
        print("queryNum:" + str(queryNum))
        print("globalNum:" + str(num))


# threadLock = threading.Lock()
def consumer(name):
    global num
    while num > 0:
        # threadLock.acquire()
        print("消费执行任务线程" + name + "  globalNum:" + str(num))
        time.sleep(3)
        delTestData()
        num -= 1
        # threadLock.release()



if __name__ == '__main__':

    p1 = threading.Thread(target=product, args=('生产',))
    c1 = threading.Thread(target=consumer, args=('消费1',))
    c2 = threading.Thread(target=consumer, args=('消费2',))
    p1.start()
    # 生产数量大于2 开始消费
    time.sleep(2)
    c1.start()
    c2.start()

    print("数据库写入结束")