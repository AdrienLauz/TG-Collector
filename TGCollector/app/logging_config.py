# logger_config.py
import logging

logFilename = './TgoutDefault.log'
logging.basicConfig(
    encoding='utf-8',
    level=logging.INFO,                 # 定义输出到文件的log级别                                 
    format='%(asctime)s  %(filename)s : %(levelname)s  %(message)s',   # 定义输出log的格式  
    datefmt='%Y-%m-%d %A %H:%M:%S',                                   # 时间  
    filename=logFilename,                    # log文件名
    filemode='w'                     # 写入模式“w”或“a”   
)