#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests
import json

url = 'http://127.0.0.1:8010/rest/v2/device/activate'
body = {"appKey": "jfksajfkajsk", "udid": "fjhajhdfja", "timestamp": "12346546416", "signature":"1456463"}

param = "appKey=" + "jfksajfkajsk" + "&udid=" + "fjhajhdfja" + "&timestamp=" + "12346546416" + "&signature=" + "1456463"
# result = requests.post(url=url, data=param)
result = requests.post(url, body)


print result.text


