import logging_config
import logging
import os
import sys
import copy
import threading
import json
import yaml
import time
import asyncio
import queue
import hashlib
from config import loadconfig,env
import telegram_pb2
import random
import requests

from telethon import TelegramClient,events,functions
from telethon.tl.types import PeerUser, PeerChat, PeerChannel,Dialog,User,Channel,Chat,ChannelParticipantsAdmins,InputGeoPoint,InputPeerChannel,InputPeerChat
from telethon.errors.rpcerrorlist import ChannelPrivateError,ChannelPublicGroupNaError,ChannelInvalidError,PeerIdInvalidError,ChatIdInvalidError,UserIdInvalidError,ChatAdminRequiredError
from telethon.tl import types

from datetime import datetime
from geopy.distance import geodesic



from TgMinIO import TGMinIO
from TgMQ import TGMQ
from TgChannel import ChannelCollector
from TgMSG import MsgCollector
from TgUser import UserCollector

# 新整理后的版本，导入的库可能有所缺漏，出现问题可以自行参考以前版本或自行判断添加库
banner = """
-------------------------------------------------------------------------------------------------------------------------------------------
                _____________           _______     _________       
                \___________/          ||      ||  | |_______|    
                     /  /              ||______||  | |_______|              ___________                                __               
            ________/  /_________      ||______||  | |_______|              \__    ___/____                          _/  |_  ____         
            \______   __________/      ||      ||  | | \  \__                 |    |  / ___\    _______________|\    \   __\/ ___\      
                  /  / \  \            ||______||  | |  \  \/                 |    | / /_/  >  /_______________  /    |  | / /_/  >      
                 /  /   \  \           ||______||  | | / \  \                 |____| \___  /                   |/     |__| \___  /      
                /  /     \  \          ||      ||  | |/  /\  \                      /_____/                               /_____/                          
               /__/       \__\         ||______||  |____/  \__\                                   
-------------------------------------------------------------------------------------------------------------------------------------------
"""
version = 'SKTEYE-TG-SeperV: 1.0.0'
update_time = '2024.10.24'


class TGInformer:
    def __init__(self):

        logging.info(banner)
        logging.info('Version:'+version)

        # 配置文件配置
        self.DUMP_MODEL = env['INFO_DUMP_LOCAL']        # 是否存储到本地中
        self.SKIP_FIRST = env['SKIP_FIRST_UPDATA']      # 启动时是否跳过加载用户
        self.VIRTU_NEAR_USERS = env['VIRTU_NEAR_USER']  # 是否启动虚拟定位搜索附近的人
        self.GEONAME_AK = env['GEONAME_AK']             # Baidu 的 ak
        self.UPDATA_MODEL = env['INFO_UPDATA_TYPE']     # 上传数据模式

        # TG 账号信息
        self.ACCOUNT = {
            'account_id' : env['TELEGRAM_ACCOUNT_ID'],
            'account_api_id':env['TELEGRAM_API_APP_ID'],
            'account_api_hash':env['TELEGRAM_API_HASH'],
            'account_first_name':env['TELEGRAM_ACCOUNT_FIRST_NAME'],
            'account_last_name':env['TELEGRAM_ACCOUNT_LAST_NAME'],
            'account_user_name':env['TELEGRAM_ACCOUNT_USER_NAME'],
            'account_phone':env['TELEGRAM_ACCOUNT_PHONE_NUMBER'], 
        }

        # MQ 配置信息加载
        self.MQ_MSG_TOPIC = env['MQ_MSG_TOPIC']
        self.MQ_USERS_TOPIC = env['MQ_USERS_TOPIC']
        self.MQ_CHANNEL_TOPIC = env['MQ_CHANNEL_TOPIC']
        self.MQ_RELATION_TOPIC = env['MQ_RELATION_TOPIC']
        self.MQ_USERNAME = env['MQ_USERNAME']
        self.MQ_PASSWORD= env['MQ_USERNAME']
        self.MQ_IP= env['MQ_USERNAME']

        # MinIO 配置信息加载
        self.MinIO_IP= env['MinIO_IP']
        self.MINIO_PORT= env['MINIO_PORT']
        self.MINIO_BUCKET_NAME= env['MINIO_BUCKET_NAME']
        self.MINIO_ACCESS_KEY= env['MINIO_ACCESS_KEY'] 
        self.MINIO_SECRET_KEY= env['MINIO_SECRET_KEY'] 

        # tg 连接实例
        self.CLIENT = None 

        # 异步锁（后面具体看看这个异步的含义）
        self.LOCK_UPLOAD_MSG = threading.Lock()             # es 消息累积存储异步锁
        self.LOCK_FILTER_MSG = threading.Lock()             # 过滤消息异步锁
        self.LOCK_LOCAL_MSG = threading.Lock()              # 本地消息存储异步锁
        self.LOCK_LOCAL_USER = threading.Lock()              # 本地用户存储异步锁
        self.LOCK_LOCAL_CHANNEL = threading.Lock()                # 本地频道存储异步锁
        self.LOCK_CHANNEL_ADD = threading.Lock()            # 等待添加更新频道异步锁

        # 采集数据缓存
        self.UPLOAD_MESSAGE = []                            # 等待上传的 message
        self.CHANNEL_META = []                              # 已加入 channel 的信息
        self.CHANNEL_MESSAGE_BOT = {}                        # 用于过滤 BOT 消息
        self.CHANNEL_PARTICIPANT_ADMIN = []                 # 只允许 admin 查看 成员的群组
        self.tg_mq = TGMQ(self)
        self.tg_minio = TGMinIO()
        self.channel_collector = ChannelCollector(self) 
        self.user_collector = UserCollector(self)
        self.msg_collector = MsgCollector(self)
        self.bucket_name = self.tg_minio.bucket_name       

        # 线程池
        self.LOOP = asyncio.get_event_loop()

        # 时间参数
        self.CHANNEL_REFRESH_WAIT = 60 * 15                 # 日志频道数据显示刷新间隔
        self.MSG_TRANSFER_GAP = 10                          # msg 传输间隔 /s

        # 加载传输
        self.Load_Transfer()

        # 启动协程开始执行监听程序
        logging.info(f'当前采集模式为：{env["ENV"]}')
        logging.info(f"Begin informer")
        self.LOOP.run_until_complete(self.Bot_Interval())

    def Load_Transfer(self):
        """ 
        根据配置，加载采集数据的传输目标
        """ 
        if self.UPDATA_MODEL == '0':
            # 没有传输目标，所以无需建立连接
            pass
        elif self.UPDATA_MODEL == '1':
            # ES 为传输目标进行加载
            logging.info(f"Begin load ES")
            self.ES_CONNECT = self.Es_Connect(Times=5)
            if self.ES_CONNECT == None:
                logging.error(f"Fail to load ES")
                sys.exit(0)

        elif self.UPDATA_MODEL == '2':
            # 采用 rabbitmq 的方法传输数据
            logging.info(f"Begin load Rabbit MQ")
            self.tg_mq.MQ_CLIENT = self.tg_mq.Mq_Connect(env)
            if self.tg_mq.MQ_CLIENT == None:
                logging.error(f"Fail to load Rabbit MQ")
                sys.exit(0)

        elif self.UPDATA_MODEL == '3':
            # 采用用户自定义的方法传输数据
            logging.info(f"Begin load custom")
            self.CUSTOM_CONNECT = self.Custom_Connect(env)
            if self.CUSTOM_CONNECT == None:
                logging.error(f"Fail to load custom")
                sys.exit(0)

        else:
            # 其它情况，也可以认为是没有传输目标，所以无需建立连接
            pass

    # 初始启动部分 
    async def Bot_Interval(self):
        """ 
        根据当前的配置建立会话，并保存 session ，方便下一次登录，并调用监控函数开始监控
        """ 
        logging.info(f'Logging in with account # {self.ACCOUNT["account_phone"]} ... \n')

        # 用来存储会话文件的地址，方便下一次的会话连接
        session_file = self.ACCOUNT['account_phone']

        # 实例化一个 tg 端对象，初次登录会记录指定路径中，后续登录会直接使用以前的账户信息
        self.CLIENT = TelegramClient(session_file, self.ACCOUNT['account_api_id'], self.ACCOUNT['account_api_hash'])

        # 异步的启动这个实例化的 tg 客户端对象，其中手机号为配置文件中的手机号
        await self.CLIENT.start(phone=f'{self.ACCOUNT["account_phone"]}')

        # 检查当前用户是否已经授权使用 API
        if not await self.CLIENT.is_user_authorized():
            logging.info(f'Client is currently not logged in, please sign in! Sending request code to {self.ACCOUNT["account_phone"]}, please confirm on your mobile device')
            
            # 当发现没有授权时，向手机号发送验证码
            await self.CLIENT.send_code_request(self.ACCOUNT['account_phone'])
            self.TG_USER = await self.CLIENT.sign_in(self.ACCOUNT['account_phone'], input('Enter code: '))

        # 完成初始化监控频道
        await self.Star_Monitor_Channel()

        # 循环定时进行信息更新
        count = 0
        while True:
            count +=1
            logging.info(f'############## {count} Running bot interval ##############')
            await asyncio.sleep(self.CHANNEL_REFRESH_WAIT)
            # 检查并更新 channel
            await self.Channel_Flush()

    async def Channel_Flush(self):
        """ 
        检查是否存在新加入的 channel 信息，同时对于其中的信息进行更新
        """ 
        count = 0
        update = False
        async for dialog in self.CLIENT.iter_dialogs():
            channel_id = dialog.id 
            # 检查是否需要更新
            if dialog.is_user:
                continue
            for i in self.CHANNEL_META:
                if i['channel id'] == channel_id :
                    channel_id = None
                    break
            if channel_id == None:
                continue
            logging.info(f'Find a new channel:{dialog.name} : {dialog.id}')
            await self.channel_collector.Upload_Channel(dialog)
        
            # 过滤 bot 消息
            if not str(channel_id) in self.CHANNEL_MESSAGE_BOT:
                self.CHANNEL_MESSAGE_BOT[channel_id] = queue.Queue(10)

        logging.info(f'########## {sys._getframe().f_code.co_name}: Monitoring channel: {json.dumps(self.CHANNEL_META,ensure_ascii=False,indent=4)}')

    async def Star_Monitor_Channel(self):
        """ 
        开始执行监听
        """ 
        # 检测本地图片路径是否存在
        picture_path = r'./file/picture'
        user_picture_path = picture_path + r'/user'
        channel_picture_path = picture_path + r'/channel'
        if not os.path.exists(user_picture_path):
            os.makedirs(user_picture_path)
            logging.info(f'Create the picture dir:{user_picture_path}')
        if not os.path.exists(channel_picture_path):
            os.makedirs(channel_picture_path)
            logging.info(f'Create the picture dir:{channel_picture_path}')
        msg_picture_path = picture_path + r'/message_pic'
        if not os.path.exists(msg_picture_path):
            os.makedirs(msg_picture_path)
            logging.info(f'Create the picture dir:{msg_picture_path}')

        # 检测是否需要本地存储，并检查路径
        if self.DUMP_MODEL == '1':
            local_path = './file/local_store'
            if not os.path.exists(local_path):   
                message_path = local_path + r'/message'
                channel_path = local_path + r'/channel_info'
                user_path = local_path + r'/user_info'
                os.makedirs(message_path)
                logging.info(f'Create the message dir:{message_path}')
                os.makedirs(channel_path)
                logging.info(f'Create the channel info dir:{channel_path}')
                os.makedirs(user_path)
                logging.info(f'Create the user info dir:{user_path}')

        # 根据配置检查是否需要遍历所有群组的用户
        if self.SKIP_FIRST == '0':
            await self.Channel_Load()
        else:
            await self.Channel_Load_Skip()

        # 处理接收到的 msg
        @self.CLIENT.on(events.NewMessage)
        async def Message_Event_Handler(event):
            # 只处理来自频道和群组的 msg
            message = event.raw_text
            if isinstance(event.message.to_id, PeerChannel):
                logging.info(f'############################################################## \n Get the channel message is ({message}) \n ##############################################################')
            elif isinstance(event.message.to_id, PeerChat):
                logging.info(f'############################################################## \n Get the chat message is ({message}) \n ##############################################################')
            else:
                # 两者均不是，跳过
                return
            # 通过协程存储当前的新 msg
            await self.msg_collector.Msg_Handler(event)

        # 根据配置决定是否启动虚拟定位，获取附近的人用户信息
        if self.VIRTU_NEAR_USERS == '1':
            await self.Nearly_Geo_User()

        # 每隔固定时间，上传一次消息
        while True:
            await self.Transfer_Msg()
            await asyncio.sleep(self.MSG_TRANSFER_GAP)

    async def Channel_Load(self):
        """ 
        遍历当前账户中的 channel 的所有信息(包括用户)，并传输到存储服务器上
        """ 
        count = 0         # 群组总数
        async for dialog in self.CLIENT.iter_dialogs():
            if dialog.is_user:
                continue
            await self.channel_collector.Upload_Channel(dialog)
            channel_id = str(dialog.id)
            if not channel_id in self.CHANNEL_MESSAGE_BOT:
                self.CHANNEL_MESSAGE_BOT[channel_id] = queue.Queue(10)
            await self.user_collector.Upload_UserChannel(dialog)
            count += 1
        logging.info(f'{sys._getframe().f_code.co_name}:Channel Count:{count} ;Monitoring channel: {json.dumps(self.CHANNEL_META ,ensure_ascii=False,indent=4)}')

    async def Channel_Load_Skip(self):
        """ 
        遍历当前账户中的 channel 的所有信息(不包括用户)，并传输到存储服务器上
        """ 
        count = 0         # 群组总数
        async for dialog in self.CLIENT.iter_dialogs():
            if dialog.is_user:
                continue
            await self.channel_collector.Upload_Channel(dialog)
            channel_id = str(dialog.id)
            if not channel_id in self.CHANNEL_MESSAGE_BOT:
                self.CHANNEL_MESSAGE_BOT[channel_id] = queue.Queue(10)
            count += 1
        logging.info(f'{sys._getframe().f_code.co_name}:Channel Count:{count} ;Monitoring channel: {json.dumps(self.CHANNEL_META ,ensure_ascii=False,indent=4)}')
        
    def Store_Data_Json(self,File_Name:str,Lock:threading.Lock,Data:dict):
        """ 
        打开 json 文件，并将数据存入
        @param File_Name: 存储文件的完整路径
        @param Lock: 异步锁
        @param Data: 数据内容
        """ 
        with Lock:
            if not os.path.exists(File_Name):
                with open(File_Name,'w') as f:
                    init_json ={}
                    json_first = json.dumps(init_json)
                    f.write(json_first)
            with open(File_Name, 'r') as f:
                file_data = json.load(f)
            count = len(file_data) + 1
            try:
                file_data[str(count)]=(Data)
            except Exception as e:
                logging.error(f"Json file error:({e}).")
            json_data = json.dumps(file_data,ensure_ascii=False,indent=4)
            with open(File_Name,'w') as f:
                f.write(json_data)
 
 
    # 此为不传输部分
    def Msg_None_Transfer(self):
        """
        没有传输目标，所以需要清空
        """ 
        with self.LOCK_UPLOAD_MSG:
            self.UPLOAD_MESSAGE = []

    # 传输部分（将数据上传） 
    async def Transfer_Msg(self):
        """ 
        根据配置选项，定期对监听到的 msg 进行传输
        """ 
        logging.info(f"Start transfer msg.")
        if self.UPDATA_MODEL == '0':
            # 没有传输目标
            self.Msg_None_Transfer()
        elif self.UPDATA_MODEL == '1':
            # ES 为传输目标
            self.Es_Msg_Transfer()
        elif self.UPDATA_MODEL == '2':
            # rabbitmq 为传输目标
            self.tg_mq.Mq_Msg_Transfer()
        elif self.UPDATA_MODEL == '3':
            # 自定义的传输目标
            self.Custom_Msg_Transfer()
        else:
            # 其它情况，也可以认为是没有传输目标
            self.Msg_None_Transfer()

    def Transfer_Channel(self,Format_Channel:dict):
        """
        根据配置选项，对得到的 channel 信息进行传输
        """
        if self.UPDATA_MODEL == '0':
            # 没有传输目标
            pass
        elif self.UPDATA_MODEL == '1':
            # ES 为传输目标
            self.Es_Channel_Transfer(Format_Channel)
        elif self.UPDATA_MODEL == '2':
            # rabbitmq 为传输目标
            self.tg_mq.Mq_Channel_Transfer(Format_Channel)
        elif self.UPDATA_MODEL == '3':
            # 自定义的传输目标
            self.Custom_Channel_Transfer(Format_Channel)
        else:
            # 其它情况，也可以认为是没有传输目标
            pass

    # 自定义部分
    def Transfer_Users(self,Format_Users:list):
        """
        根据配置，进行相应的信息保存操作
        @param Format_Users: 规范的 users 信息列表
        """
        if self.UPDATA_MODEL == '0':
            # 没有传输目标
            pass
        elif self.UPDATA_MODEL == '1':
            # ES 为传输目标
            self.Es_Users_Transfer(Format_Users)
        elif self.UPDATA_MODEL == '2':
            # rabbitmq 为传输目标
            self.tg_mq.Mq_Users_Transfer(Format_Users)
        elif self.UPDATA_MODEL == '3':
            # 自定义的传输目标
            self.Custom_Users_Transfer(Format_Users)
        else:
            # 其它情况，也可以认为是没有传输目标
            pass

    def Custom_Msg_Transfer(self):
        """
        需要用户自定义传输方法，默认采用 pass
        """ 
        pass

    def Custom_Channel_Transfer(self,Format_Channel:dict):
        """ 
        需要用户自定义传输方法，默认采用 pass
        @param Format_Channel: 规范的channel 属性
        """ 
        pass

    def Custom_Users_Transfer(self,Format_Users:dict):
        """ 
        需要用户自定义传输方法，默认采用 pass
        @param Format_Users: 规范的 users 列表
        """ 
        pass

    # 自定义部分
    def Custom_Connect(self,Env:dict):
        """ 
        用户自定的传输目标，需要用户自己去实现
        """
        pass
    
if __name__ == '__main__':
    informer = TGInformer()
    informer.init()