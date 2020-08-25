#/usr/bin/env python
# -*- coding:utf-8 -*-

import logging
import requests
import json
import time
import random
import threading
import Queue
import pymysql
import schedule

# mysql配置
MYSQL_HOST = "10.30.0.249"
MYSQL_USER = "dev"
MYSQL_PASSWORD = "M20131209k"
MYSQL_DATABASE = "unios-skill-service"
MYSQL_CHARSET = "utf8"

# 数据服务请求路径
DATA_SERVICE_URL = "http://10.128.3.33:8080/data-cloud-service/rest/v1/music/song/search_songs?" \
                   "sid=uni_internal_test_session_id&uni_key=uni_internal_test_appkey&" \
                   "uni_uid=uni_internal_apidoc_test_id&source=7"
# 数据服务选链路径
DATA_SERVICE_GET_URL = "http://10.128.3.33:8080/data-cloud-service/rest/v1/music/song/get_url" \
                       "?scenario=child&sid=sid&uni_uid=uni_uid&userId=&uni_key=unisound&source=7&format=128kmp3"
#日志写入文件路径
LOGGER_PATH = "D:/test/log.txt"
UDID_LOGGER_PATH = "D:/test/udid_log.txt"
lock = threading.Lock()
# 定义查询参数
singer_arr = ["刘德华", "周杰伦", "阿悠悠", "张学友", "毛不易", "林俊杰", "薛之谦", "陈奕迅", "华晨宇", "汪苏泷", "杨小壮", "小阿枫", "王琪", "赵雷", "张杰", "王菲", "李宗盛", "周传雄", "刘若英", "许嵩"]
song_arr = ["忘情水", "我是如此相信", "旧梦一场", "吻别", "入海", "背对背拥抱", "演员", "人来人往", "齐天", "万有引力", "后来", "旅程", "一个人挺好", "我这一生", "和你一样", "借", "风筝误", "不配怀念", "关山酒 ", "慕夏"]
singer_song_arr = ["刘德华 忘情水", "周杰伦 我是如此相信", "阿悠悠 旧梦一场", "张学友 吻别", "毛不易 入海", "林俊杰 背对背拥抱", "薛之谦 演员", "陈奕迅 人来人往", "华晨宇 齐天", "汪苏泷 万有引力"
    , "魏新雨 情花几时开", "周传雄 黄昏", "陈雪凝 你的酒馆对我打了烊", "吴亦凡 大碗宽面", "周深 蜕", "张敬轩 只是太爱你", "阿悠悠 念旧", "杨小壮 孤芳自赏", "周笔畅 最美的期待", "花姐 此情一直在心间"]
# 每个时间段对应处理最大数据量
time_max_num = {
    0:100,
    1:40,
    2:20,
    3:20,
    4:20,
    5:20,
    6:20,
    7:40,
    8:100,
    9:1000,
    10:5000,
    11:10000,
    12:15000,
    13:21000,
    14:13000,
    15:15000,
    16:17000,
    17:23000,
    18:25000,
    19:27000,
    20:35000,
    21:39000,
    22:33000,
    23:23000
}


'''
    每天修改最大执行数量
'''
def change_time_max_num():
    global udids
    udids = []
    init_udids()
    print(len(udids))

'''
    连接mysql
'''
def connect_mysql():
    # 连接database
    conn = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER,password=MYSQL_PASSWORD,database=MYSQL_DATABASE,charset=MYSQL_CHARSET)
    return conn

'''
    查询数据库中的设备udid
'''
def get_udid_by_db():
    conn_mysql = connect_mysql()
    cursor = conn_mysql.cursor()
    sql = 'select * from kuwo_udid'
    # 执行SQL语句
    cursor.execute(sql)
    result = cursor.fetchall()
    # 关闭光标对象
    cursor.close()
    # 关闭数据库连接
    conn_mysql.close()
    return result

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

# 请求数据服务获取数据接口
def request_data_service(udid, keyword):
    udid_log.info(udid)
    url = DATA_SERVICE_URL + "&devId=" + str(udid) + "&key=" + keyword
    # ip为空直接返回空
    s = requests.session()
    s.keep_alive = False
    res = requests.get(url)
    content = json.loads(res.text)
    res_code = content.get("errorCode")
    try:
        if res_code == 0:
            item = content.get("data").get("items")[0]
            return item.get("id")
        else :
            return -1
    except:
        return -1

# 请求数据服务选链接口
def get_url(udid, id):
    url = DATA_SERVICE_GET_URL + "&devId=" + str(udid) + "&id=" + str(id)
    # ip为空直接返回空
    s = requests.session()
    s.keep_alive = False
    res = requests.get(url)
    content = json.loads(res.text)
    res_code = content.get("errorCode")
    try:
        if res_code == 0:
            pass
        else :
            logger.info(udid)
    except:
        logger.info(udid)

'''
    设备激活在酷我音乐激活
'''
def activate():

    # 随机查询类型
    keyword = ""
    # 获取随机参数索引
    index = random.randint(0, 19)
    search_type = random.randint(0, 2)
    if search_type == 0:
        keyword = singer_arr[index]
    elif search_type == 1:
        keyword = song_arr[index]
    else:
        keyword = singer_song_arr[index]

    udid = get_task()
    # 调用酷我音乐接口
    id = request_data_service(udid, keyword)
    if not id == -1:
        get_url(udid, id)
    threadQueue.put(threading.Thread(target=activate))

'''
    将设备udid加载到内存中
'''
def init_udids():
    # 读取udid文件
    logger.info("开始加载udid")
    with open("activate_rule_value.txt", "r") as f:
        while True:
            line = f.readline()
            udids.append(line.replace("\n", ""))
            if(not line):
                break
    # 加载数据库中的udid
    dbUdids = get_udid_by_db()
    for udid in dbUdids:
        udids.append(udid[0])

    # 打乱数组中顺序
    random.shuffle(udids)
    logger.info("加载完成^_^！udid总数为：" + str(len(udids)))

'''
 获取要执行的任务 用栈的原理来实现，防止重复udid被处理
'''
def get_task():
    global hourCurrentNum, currentNum
    # 保证线程之间安全
    lock.acquire()
    try:
        currentNum += 1
        hourCurrentNum += 1
        return udids.pop()
    except IndexError:
        logger.error("任务已经全部被执行完了$__$")
    finally:
        lock.release()

# 设置定时任务
schedule.every().day.at("19:48").do(change_time_max_num)
if __name__=='__main__':
    logger = init_logger(LOGGER_PATH)
    udid_log = init_logger(UDID_LOGGER_PATH)
    udids = []
    # 将设备加载到内存中
    init_udids()
    print(len(udids))
    # 记录上一小时执行的时间点
    preHourTime = 0
    # 每个小时执行最大的数量
    hourTaskMaxNum = 0
    # 记录每个小时执行的当前数量
    hourCurrentNum = 0
    # 记录上一分钟执行的时间点
    preTime = 0
    # 每分钟执行最大的数量
    taskMaxNum = 0
    # 记录每分钟执行的当前数量
    currentNum = 0

    #############################################  多线程执行任务  ###############################################
    # 定义线程数量
    threadNum = 10
    # 定义队列模拟线程池
    threadQueue = Queue.Queue()
    for i in range(0, threadNum):
        threadQueue.put(threading.Thread(target=activate))
    # 多线程执行任务
    while True:
        schedule.run_pending()
        # 小时维度数据处理
        curr_hour = time.strftime('%H', time.localtime(time.time()))
        if not curr_hour == preHourTime:
            logger.info("剩余设备数：" + str(len(udids)))
            logger.info("当前时间点" + str(preHourTime) + "---执行任务：" + str(hourCurrentNum) + "---允许最大任务数：" + str(hourTaskMaxNum))
            hourCurrentNum = 0
            preHourTime = curr_hour
            # 取每小时的最大处理量
            hourTaskMaxNum = time_max_num.get(int(curr_hour))

        # 分钟维度数据处理
        curr_minute = time.strftime('%M', time.localtime(time.time()))
        if not curr_minute == preTime:
            preTime = curr_minute
            currentNum = 0
            # 将小时数据分解成分钟
            addRandom = random.randint(0, min((hourTaskMaxNum/10), 100))
            taskMaxNum = (hourTaskMaxNum / 60) + addRandom

        # 先判断是否有任务，防止创建完新的线程没有任务
        if len(udids) <= 0:
            logger.info("任务完成^_^")
            break

        # 队列有线程并且任务没有超过当前时间点的最大数据量就执行任务
        # lock.acquire()
        # try:
        if currentNum < taskMaxNum and threadQueue.not_empty:
            threadQueue.get().start()
        # finally:
        #     lock.release()