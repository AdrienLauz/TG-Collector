import asyncio
import os
import sys
import threading
import logging
import yaml

env = None

def loadconfig(configfile = "./config.yaml"):
    global env
    if env is not None:
        logging.info("Config already loaded, skipping reload.")
        return  # 如果 env 已经加载，则直接返回
    
    # 检查配置文件
    logging.info(f"Begin to load config")
    if not os.path.exists(configfile):
        logging.error(f"Unable to find the configuration file")
        sys.exit(0)

    # 加载配置文件
    with open(configfile,'r',encoding='utf-8') as f:
        try:
            env = yaml.load(f.read() ,Loader=yaml.FullLoader)
        except:
            logging.error(f"Unable to loading the configuration file")
            sys.exit(0)
    logging.info(f"Config loading completed")

loadconfig()