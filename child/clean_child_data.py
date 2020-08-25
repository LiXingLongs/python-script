#!/usr/bin/env python
# -*- coding:utf-8 -*-

import pymongo
import json
import logging
import sys


################## 数据库配置 ####################
# mongos 儿童资源库配置
MONGO_HOST_DEVICE = "dds-8vb86b944dbf2c741.mongodb.zhangbei.rds.aliyuncs.com"
MONGO_PORT = 3717
MONGO_USER_DEVICE = "dcs_user"
MONGO_PASSWORD_DEVICE = "000000"
# MONGO_HOST_DEVICE = "10.20.222.63"
# MONGO_PORT = 27017
# MONGO_USER_DEVICE = "dcs_user"
# MONGO_PASSWORD_DEVICE = "000000"

################## 业务配置 ####################
# 单次处理数据条数
LIMIT_NUM = 5000
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

# 连接儿童资源数据库
def connect_data_cloud_service():
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
    # total = collection.find({'dataOrigin':{'$nin':[4, 2, 6, 12, 20, 19, 21, 7, 10, 1, 15, 3, 27]}}).count()
    # total = collection.find({'dataOrigin':{'$in':[3, 4, 15, 20]}}).count()
    total = collection.find({'dataOrigin':{'$in':[3]}}).count()
    # total = collection.find().count()
    logger.info("数据总条数：" + str(total))
    # 所有数据执行完结束
    while(curr < total):
        bulk = collection.initialize_ordered_bulk_op()
        # 获取要处理的数据
        # for data in collection.find({'dataOrigin':{'$nin':[4, 2, 6, 12, 20, 19, 21, 7, 10, 1, 15, 3, 27]}}).skip(curr).limit(LIMIT_NUM):
        # for data in collection.find({'dataOrigin':{'$in':[3, 4, 15, 20]}}).skip(curr).limit(LIMIT_NUM):
        for data in collection.find({'dataOrigin':{'$in':[3]}}).skip(curr).limit(LIMIT_NUM):
        # for data in collection.find().skip(curr).limit(LIMIT_NUM):
            curr+=1
            if data == None:
                continue

            try:
                # 查询数据替换成json串
                dataStr = json.dumps(data)
                # 清洗需要转换的数据
                # dataStr = dataStr.replace("resource.hivoice.cn/dcs-resources", "dcs-resources-oss.hivoice.cn")
                # dataStr = dataStr.replace("dcs-resources-oss.hivoice.cn", "resource.hivoice.cn/dcs-resources")
                # dataStr = dataStr.replace("dcs-resources-oss.hivoice.cn", "resources.hivoice.cn")
                dataStr = dataStr.replace("audio.ai160.com", "ai-admin-audio.ai160.com")
                # 转换回对象
                data = json.loads(dataStr)
                # 获取更新数据的主键
                _id = data.get("_id")
                bulk.find({'_id': _id}).update({'$set': data})
            except Exception as e:
                logger.error(e)

            # 批量执行更新操作
            if curr % LIMIT_NUM == 0 or curr >= total:
                bulk.execute()
                logger.info("当前执行条数:" + str(curr))


if __name__ == '__main__':
    logger = init_logger(LOGGER_PATH)
    logger.info("开始清洗数据")
    db = connect_data_cloud_service()
    data_db = db.data_cloud_service
    # 清洗作品数据
    logger.info("==========================清洗作品数据==========================")
    audio_collection = data_db.child_audio_info
    clean_data(audio_collection)
    # # 清洗专辑数据
    # logger.info("==========================清洗专辑数据==========================")
    # album_collection = data_db.child_album_info
    # clean_data(album_collection)
    # # 清洗单句数据
    # logger.info("==========================清洗单句数据==========================")
    # poem_collection = data_db.child_poem_verse
    # clean_data(poem_collection)
    # # 清洗绘本数据
    # logger.info("==========================清洗绘本数据==========================")
    # book_collection = data_db.picture_book
    # clean_data(book_collection)
    logger.info("清洗完成")