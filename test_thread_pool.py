#!/usr/bin/env python
# -*- coding:utf-8 -*-


import threadpool
import threading
import time

def say(list):
    print (threading.current_thread().name)
    insert = []
    for name in list:
        insert.append(name)
    print insert

name_list =['xiaozi','aa','bb','cc']
start_time = time.time()
pool = threadpool.ThreadPool(4)
requests = threadpool.makeRequests(say, name_list)
[pool.putRequest(req) for req in requests]
pool.wait()
print '%d second'% (time.time()-start_time)