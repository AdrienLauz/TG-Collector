from datetime import datetime
import time
import os
import json
import requests
import logging

logFilename = './warning.log'

logging.basicConfig(
                encoding='utf-8',
                level    = logging.INFO,          # 定义输出到文件的log级别                                                
                format   = '%(asctime)s  %(filename)s : %(levelname)s  %(message)s',    # 定义输出log的格式
                datefmt  = '%Y-%m-%d %A %H:%M:%S',                                     # 时间
                filename = logFilename,                # log文件名
                filemode = 'w')                        # 写入模式“w”或“a”

def send_warning():
    url = 'http://www.pushplus.plus/send'
    data = {
        "token": '24872b7560f34946934b9411567712bf',
        "title": 'TG 采集情况',
        "content": "TG 采集出现问题，15分钟没有更新了。"
    }
    body = json.dumps(data).encode(encoding='utf-8')
    headers = {'Content-Type': 'application/json'}
    requests.post(url, data=body, headers=headers)

def is_modefied(logFilename,last_time):
    current_time = os.path.getmtime(logFilename)
    if (current_time != last_time):  return False
    else: return True

def main():
    logFilename = './tgout.log'
    now = os.path.getmtime(logFilename)
    time.sleep(10)
    while(1):
        t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        if (is_modefied(logFilename,now)):
            now = os.path.getmtime(logFilename)
            logging.error(f"Warning !!!!!!!!!!!time:{t}")
            send_warning()
            time.sleep(60*60*3)
        else:
            now = os.path.getmtime(logFilename)
            logging.info(f"OK,time:{t}")
            time.sleep(60*15)

main()


