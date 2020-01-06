#!/usr/bin/env python
# -*- coding:utf-8 -*-
import time
# def test():
#     for i in range(1, 10):
#         print(i)


if __name__ == '__main__':
    # a = ['1.1.1.1','2.2.2.2','3.3.3.3']
    # select_str = 'select * from activate_rule_value where ruleValue in (%s)' % ','.join(['%s'] * len(a))
    # print(select_str)
    # {u'63214c6190c54eed8e3b9e367f14bb7a': 1525673124169L, u'928465aee8c14302ba91de6a490d1bd8': 1536825449188L}
    # auth_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(1536825449188L / 1000))
    print(time.strftime('%H',time.localtime(time.time())))
    thread_num = 5
    curr_hour = time.strftime('%H',time.localtime(time.time()))
    if (8< int(curr_hour) <22):
        thread_num = 1
    print thread_num