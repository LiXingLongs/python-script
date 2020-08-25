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
    db = conn.data_cloud_service
    # 授权
    db.authenticate("dcs_user", "000000")
    # 返回连接
    return conn

def findChildAudioData(code):
    conn = connect_mongo()
    db = conn.data_cloud_service
    collection = db.child_audio_info
    return collection.find({"code":code}).limit(1)

def updateChildPeomData():
    conn = connect_mongo()
    db = conn.data_cloud_service
    collection = db.child_poem_verse
    statictisNum = 0
    statictisFailNum = 0
    for poem in collection.find({}):
        print(poem)
        code = poem['code']
        id = poem['_id']
        for audio in findChildAudioData(code):
            if audio != None:
                dataOrigin = audio['dataOrigin']
                dataPlanCodes = audio['dataPlanCodes']
                collection.update({"_id":id},{'$set':{"dataOrigin":dataOrigin, "dataPlanArr":dataPlanCodes}})
                statictisNum += 1
            else:
                statictisFailNum += 1
    print("更新单句数据成功：" + str(statictisNum) + "条， 失败：" + str(statictisFailNum) + "条")
if __name__ == '__main__':

    updateChildPeomData()