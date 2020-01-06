#!/usr/bin/env python
# -*- coding:utf-8 -*-

import pymongo
import json


################## 数据库配置 ####################
# mongos 儿童资源库配置
MONGO_HOST_DEVICE = "10.20.222.63"
MONGO_PORT = 27017
MONGO_USER_DEVICE = "dcs_user"
MONGO_PASSWORD_DEVICE = "000000"

################## 业务配置 ####################
# 单次处理数据条数
LIMIT_NUM = 10000

# 连接儿童资源数据库
def connect_data_cluod_service():
    # 连接mongo数据库
    conn = pymongo.MongoClient(MONGO_HOST_DEVICE, MONGO_PORT)
    # 指定连接数据库
    db = conn.data_cloud_service
    # 授权
    db.authenticate(MONGO_USER_DEVICE, MONGO_PASSWORD_DEVICE)
    # 返回连接
    return conn

"""
清洗指定集合的数据
collection 代表要执行的集合  
"""
def clean_data(collection):
    # 当前执行进度
    curr = 0
    # 获取总的数据条数
    total = collection.find({}).count()
    print("数据总条数：" + str(total))
    # 所有数据执行完结束
    while(curr < total):
        bulk = collection.initialize_ordered_bulk_op()
        # 获取要处理的数据
        for data in collection.find({}).skip(curr).limit(LIMIT_NUM):
            curr+=1
            if data == None:
                continue

            try:
                # 查询数据替换成json串
                dataStr = json.dumps(data)
                # 清洗需要转换的数据
                dataStr = dataStr.replace("resource.hivoice.cn/dcs-resources", "dcs-resources-oss.hivoice.cn")
                # 转换回对象
                data = json.loads(dataStr)
                # 获取更新数据的主键
                _id = data.get("_id")
                bulk.find({'_id': _id}).update({'$set': data})
            except Exception as e:
                print(e)

            # 批量执行更新操作
            if curr % LIMIT_NUM == 0 or curr >= total:
                bulk.execute()
                print("当前执行条数:" + str(curr))


if __name__ == '__main__':
    print("开始清洗数据")
    db = connect_data_cluod_service()
    data_db = db.data_cloud_service
    # 清洗作品数据
    print("==========================清洗作品数据==========================")
    audio_collection = data_db.child_audio_info
    clean_data(audio_collection)
    # 清洗专辑数据
    print("==========================清洗专辑数据==========================")
    audio_collection = data_db.child_album_info
    clean_data(audio_collection)
    # 清洗单句数据
    print("==========================清洗单句数据==========================")
    audio_collection = data_db.child_poem_verse
    clean_data(audio_collection)
    # 清洗绘本数据
    print("==========================清洗绘本数据==========================")
    audio_collection = data_db.picture_book
    clean_data(audio_collection)