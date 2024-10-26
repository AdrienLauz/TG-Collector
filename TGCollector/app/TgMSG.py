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
import telegram_pb2
import random
import requests

from telethon import TelegramClient,events,functions
from telethon.tl.types import PeerUser, PeerChat, PeerChannel,Dialog,User,Channel,Chat,ChannelParticipantsAdmins,InputGeoPoint,InputPeerChannel,InputPeerChat
from telethon.errors.rpcerrorlist import ChannelPrivateError,ChannelPublicGroupNaError,ChannelInvalidError,PeerIdInvalidError,ChatIdInvalidError,UserIdInvalidError,ChatAdminRequiredError
from telethon.tl import types

from datetime import datetime
from geopy.distance import geodesic
import logging_config
import logging

from config import env

class MsgCollector:
    def __init__(self,tg_informer):
        self.tg_informer = tg_informer
        

    # MSG 部分
    async def Msg_Handler(self,Event:events):
        # 处理消息中的多媒体部分
        media = await self.Msg_Media(Event)

        # 处理消息中的文本部分（大多数消息属性也在这部分）
        text_info = await self.Msg_Text(Event)

        # 合成消息字段
        msg_info = self.Get_Msg_Info(media,text_info)

        # 过滤消息（垃圾机器人消息过滤）
        if self.Filter_Msg(msg_info):
            logging.info(f'the msg has abandon{msg_info["tg_msg"]}')
            return 
        await self.Dump_Msg(msg_info)

    async def Msg_Media(self,Event:events)->dict:
        """ 
        检查是否是多媒体文件，并提取多媒体文件的信息
        非多媒体文件返回 {}
        @param Event: msg 事件
        @return: 多媒体文件属性
        """ 
        media = {}
        if Event.photo:
            logging.info(f'The message have photo')
            file_name,file_path,local_path =  await self.Download_File(Event)
            if file_path == '':
                return media
            file_store = local_path
            file_size = os.path.getsize(file_store)
            file_hash = self.File_Md5(file_store)
            media = {
                'file_type': '.jpg',
                'file_store': file_store,
                'file_name': file_name,
                'file_hash': file_hash,
                'file_size': file_size,
            }
            os.remove(file_store)    
            logging.info(f"Successfully deleted local file {file_name}")
        return media

    async def Download_File(self,Event:events,max_retries=3)->(str,str):
        """ 
        将图片存储到指定路径
        @param Event: 消息事件
        @return: 文件名、文件存储路径
        """ 
        retry_count = 0  # 当前重试次数
        
        file_name = self.GetImageName(Event)
        file_path = self.GetImagePath(Event)

        if not os.path.exists(file_path):
            os.makedirs(file_path)
        download_path = file_path+'/' + file_name

        while retry_count < max_retries:
            try:
                # 下载文件
                await Event.download_media(download_path)
                logging.info(f'Picture downloaded successfully: {download_path}')

                # 校验文件是否存在并且文件大小正常
                if os.path.exists(download_path):
                    file_size = os.path.getsize(download_path)
                    if file_size > 0:
                        logging.info(f"File {download_path} is ready for upload, size: {file_size} bytes")
                        break  # 下载成功，跳出重试循环
                    else:
                        logging.error(f"File {download_path} is empty. Retrying download...")
                else:
                    logging.error(f"File {download_path} does not exist. Retrying download...")

            except Exception as e:
                logging.error(f"Error downloading file: {e}. Retrying download...")

            retry_count += 1
            await asyncio.sleep(2)  # 重试前的等待时间

        if retry_count == max_retries:
            logging.error(f"Failed to download file after {max_retries} attempts.")
            return '', ''

        if not os.path.exists(download_path) or os.path.getsize(download_path) == 0:
            logging.error(f"File download failed or file does not exist or is empty: {download_path}")
            return '', ''

        logging.info(f"File {download_path} exists and is ready for upload.")

        # 延迟 2 秒后再上传到 MinIO
        await asyncio.sleep(5)  # 增加延迟，确保文件完全下载        

        # 生成以日期命名的路径，如 "picture/message_pic/24_10_22"
        now = datetime.now()
        date_folder = now.strftime("%d_%m_%y")
        minio_subfolder = f"picture/message_pic/{date_folder}"
    
        # 上传到 MinIO，并确保上传路径符合需求
        logging.info(f"Uploading file to MinIO: {download_path}")
        upload_success = await self.tg_informer.tg_minio.upload_to_minio(download_path, env.bucket_name, minio_subfolder)

        if upload_success :
            try:
                file_path = upload_success     # 使用 MinIO 路径
                # os.remove(download_path)    
                # logging.info(f"Successfully deleted local file {file_name}")
            except Exception as e:
                logging.error(f"Error deleting local file: {e}")
        else:
            logging.error(f"Failed to upload {file_name} to MinIO.")
        # 上传功能结束
        return file_name, file_path , download_path

    def GetImageName(self,Event:events)->str:
        """ 
        获得图片文件名，用于存储
        @param Event: msg 事件
        @return: 图片名称
        """ 
        if Event.message.grouped_id:
            image_name = str(Event.message.id)+'_'+str(Event.message.grouped_id)+'.jpg'
        else:
            image_name = str(Event.message.id)+'_'+str(Event.message.chat_id)+'.jpg'

        return image_name

    def GetImagePath(self,Event:events):
        """ 
        获得图片的存储路径
        @param Event: 新消息事件
        @return: 图片将要存储的文件夹路径
        """     
        file_path = './file/picture/message_pic' # 使用当前日期替换占位符
        if isinstance(Event.message.to_id, PeerChannel):
            channel_id = int(Event.message.to_id.channel_id)
        elif isinstance(Event.message.to_id, PeerChat):
            channel_id = int(Event.message.to_id.chat_id)
        else:
            channel_id = 'None'

        now = datetime.now()
        file_path = file_path+ '/'+str(channel_id)+'/'+now.strftime("%y_%m_%d")
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        return file_path

    def File_Md5(self,File_path:str)->str:
        """ 
        计算文件的 md5 值
        @param File_Path: 文件完整路径
        @return: 文件的md5值
        """ 
        if not os.path.isfile(File_path):
            logging.error(f"Invalid file path (not a file): {File_path}")
            return ""

        m = hashlib.md5()
        with open(File_path,'rb') as file:
            while True:
                data = file.read(4096)
                if not data:
                    break
                m.update(data)
        return m.hexdigest()

    def Path_fix(self,Original_Path:str)->str:
        """
        修复对图片的文件路径问题(主要是为了 Ngnix 的问题)
        param Original_Path: 修改前的路径
        return: 修改后的路径
        """
        if Original_Path == None:
            return None
        fix_path = Original_Path
        pre_str = "./file/"
        if pre_str in Original_Path:
            index = Original_Path.index(pre_str) + 7  # 找到 "./" 的位置并加上长度 2 来获取开始提取的索引
            extracted_string = Original_Path[index:]  # 提取从索引开始的字符串
            fix_path = 'tg/' + extracted_string
        return fix_path

    def Channel_id_fix (self,channel_id)->str:
        """
        将 channel id 的前缀修复完整
        """
        str_channel_id = str(channel_id)
        full_channel_id = str_channel_id
        if str_channel_id.startswith("-100"):
            full_channel_id = str_channel_id
        elif str_channel_id.startswith("-"):
            full_channel_id = "-100"+str_channel_id[1:]
        elif str_channel_id.startswith("100"):
            full_channel_id = '-' + str_channel_id
        logging.info(f"channel id from({str_channel_id}) to ({full_channel_id})")
        return full_channel_id

    async def Msg_Text(self,Event:events)->dict:
        
        msg_link = {}
        msg_text = Event.raw_text     # str
        grouped_id = Event.message.grouped_id   #int
        sender_id  =  str(Event.sender_id)      # str
        msg_id = Event.message.id
        is_reply = False
        reply_id = ""
        is_forward = False
        forward_user_id = ""
        forward_group_id = ""
        msg_time = Event.message.date
        group_id = ""
        last_edit_time = Event.message.edit_date 
        read_count = Event.message.views
        user_like = 0
        user_like_users = []

        # group id
        if isinstance(Event.message.to_id, PeerChannel):
            group_id = int(Event.message.to_id.channel_id)
        elif isinstance(Event.message.to_id, PeerChat):
            group_id = int(Event.message.to_id.chat_id)
            print(Event.message.to_id.chat_id)

        if group_id != "":
            group_id = self.Channel_id_fix(group_id)
        # 超链接部分
        text_url = []
        for ent,txt in Event.get_entities_text():
            cnt = {
                'text':str(txt),
                'url':str(ent)
            }
            text_url.append(cnt)
        if (len(text_url)!= 0):
            for i in range(len(text_url)):
                msg_link[text_url[i]['text']] = text_url[i]['url']

        # 回复部分
        if Event.message.reply_to is not None:
            is_reply = True
            reply_id = Event.message.reply_to.reply_to_msg_id

        # 转发部分
        if Event.message.fwd_from is not None:
            is_forward = True
            from_peer = Event.message.fwd_from.from_id
            if isinstance(from_peer,PeerChat):
                forward_group_id = from_peer.chat_id
            elif isinstance(from_peer,PeerUser):
                forward_user_id = from_peer.user_id
            elif isinstance(from_peer,PeerChannel):
                forward_group_id = from_peer.channel_id
            # 确保 forward_group_id 和 forward_user_id 都是整数
            try:
                if forward_group_id:
                    forward_group_id = int(forward_group_id)  # 转换为整数
                if forward_user_id:
                    forward_user_id = int(forward_user_id)    # 转换为整数
            except ValueError as e:
                logging.error(f"Invalid ID conversion error: {e}")
                # 设置无效 ID 为 None 或处理异常
                forward_group_id = None
                forward_user_id = None

        # 点赞统计
        reactions = {
            "thumbs_up":[],
            "thumbs_down":[],
            "heart":[],
            "fire":[],
            "smile_with_hearts":[],
            "clap":[],
            "smile":[],
            "thinking":[],
            "exploding_head":[],
            "scream":[],
            "angry":[],
            "single_tear":[],
            "party_popper":[],
            "starstruck":[],
            "vomiting":[],
            "poop":[],
            "praying":[],
            "unknowntype":[],
        }
        if Event.message.reactions :
            reactions_list = Event.message.reactions.recent_reactions
            if (reactions_list is not None):
                for i in reactions_list:
                    like_type  = self.User_Like_Type(i.reaction)
                    user_id = ""
                    user_peer = i.peer_id
                    if isinstance(user_peer,PeerChat):
                        user_id = str(user_peer.chat_id)
                    elif isinstance(user_peer,PeerUser):
                        user_id = str(user_peer.user_id)
                    elif isinstance(user_peer,PeerChannel):
                        user_id = str(user_peer.channel_id)
                    else :
                        user_id = "None"
                    if user_id == "None" :
                        user_id = ""

                    date = self.Time_To_Str(i.date)
                    like_info = {
                        "user_id" : user_id,
                        "date" : date
                    }

                    reactions[like_type].append(like_info)
                
        text_info = {
            "msg_text":msg_text,
            "sender_id":sender_id,
            "group_id":group_id,
            "msg_id":msg_id,
            "grouped_id":grouped_id,
            "msg_link":msg_link,
            "is_reply":is_reply,
            "reply_msg_id":reply_id,
            "is_forward":is_forward,
            "forward_user_id":forward_user_id,
            "forward_group_id":forward_group_id,
            "msg_time":msg_time,
            "last_edit_time":last_edit_time,
            "read_count":read_count,
            "users_reactions":reactions,
        }
        return text_info

    def Time_To_Str(self,date:datetime)->str:
        """ 
        将时间转换为时间戳 * 1000
        并以 str 类型返回
        """
        if date == None:
            return ""
        int_date = int(datetime.timestamp(date)*1000)
        str_date = str(int_date)
        return str_date

    def User_Like_Type(self,reaction:str)->str:
        """
        获得用户的 reaction 类型
        """ 
        if reaction == "👍" :
            return "thumbs_up"
        if reaction == "👎" :
            return "thumbs_down"
        if reaction == "❤️" :
            return "heart"
        if reaction == "🔥" :
            return "fire"
        if reaction == "🥰" :
            return "smile_with_hearts"
        if reaction == "👏" :
            return "clap"
        if reaction == "😁" :
            return "smile"
        if reaction == "🤔" :
            return "thinking"
        if reaction == "🤯" :
            return "exploding_head"
        if reaction == "😱" :
            return "scream"
        if reaction == "🤬" :
            return "angry"
        if reaction == "😢" :
            return "single_tear"
        if reaction == "🎉" :
            return "party_popper"
        if reaction == "🤩" :
            return "starstruck"
        if reaction == "🤮" :
            return "vomiting"
        if reaction == "💩" :
            return "poop"
        if reaction == "🙏" :
            return "praying"
        print(f"unknown reaction:{reaction}")
        return "unknowntype"

    def Get_Msg_Info(self,Media_Info:dict,Text_Info:dict):
        """ 
        将获得消息属性合并
        """ 
        msg_info = {
            "text_info":Text_Info,
            "media_info":Media_Info
        }
        return msg_info

    def Filter_Msg(self,Msg_Info:dict)->bool:
        """ 
        过滤无效的 msg 消息，如：bot 发送的广告（同一群组内相同内容）
        @return: 是否为无效 msg
        """ 
        channel_id = Msg_Info['text_info']['group_id']
        if not (channel_id in self.tg_informer.CHANNEL_MESSAGE_BOT):
            logging.error(f"Not found dialog({channel_id}) in message cache")
            return False
        with env.LOCK_FILTER_MSG:
            message_queue = self.tg_informer.CHANNEL_MESSAGE_BOT[channel_id]
            if Msg_Info['text_info']['msg_text'] != '':
                filter_text = Msg_Info['text_info']['msg_text'] 
            elif Msg_Info['media_info'] != {}:
                filter_text = Msg_Info['media']['md5']
            else:
                return False

            empty = message_queue.empty()
            flag = False
            if not empty:
                queue_len = message_queue.qsize()
                for i in range(queue_len):
                    item = message_queue.get()
                    if item == filter_text:
                        flag = True
                    else:
                        message_queue.put(item)
                if flag:
                    try:
                        message_queue.put(filter_text)
                    except Exception as e:
                        logging.error(f"Get an error({e})")
                else:
                    if message_queue.full():
                        item = message_queue.get()
                        message_queue.put(filter_text)
                    else:
                        message_queue.put(filter_text)
            else:
                message_queue.put(filter_text)
        return flag

    async def Dump_Msg(self,Msg_Info:dict):
        """ 
        将提取到的 msg 属性进行格式转换、存储准备和根据配置是否本地存储
        @param Msg_Info: 提取到的完整的 msg 的属性
        """ 
        format_msg = self.Format_Msg(Msg_Info)

        if self.tg_informer.DUMP_MODEL == '1':
            self.Store_Msg_In_Json_File(format_msg)

        self.Update_Msg_List(format_msg)

    def Format_Msg(self,Msg_Info:dict)->dict:
        """
        将获得的完整 msg 属性转换为所需要的格式
        @param Msg_Info: 完整的 msg 属性
        @return: 规范格式的 msg 属性 
        """
        file_info = {}
        if Msg_Info['media_info'] != {}:
            file_info["file_hash"] = Msg_Info['media_info']['file_hash']
            file_info["file_size"] = Msg_Info['media_info']['file_size']
            file_info["file_name"] = Msg_Info['media_info']['file_name']
            file_info["file_store"] = Msg_Info['media_info']['file_store']
            file_info["file_type"] = Msg_Info['media_info']['file_type']

        # file_info = {
            #"file_hash": Msg_Info['media_info'].get('file_hash', None),
            # 'file_hash': Msg_Info['media_info']['file_hash'],
            # 'file_size': Msg_Info['media_info']['file_size'],
            # 'file_name': Msg_Info['media_info']['file_name'],
            # 'file_store': Msg_Info['media_info']['file_store'],
            # 'file_type': Msg_Info['media_info']['file_type'],
        # } if Msg_Info['media_info'] else {}

        reply_info = {
            'is_reply':Msg_Info['text_info']['is_reply'],
            'reply_id':Msg_Info['text_info']['reply_msg_id'],

        }

        forward_info = {
            'forward_user_id':Msg_Info['text_info']['forward_user_id'],
            'forward_group_id':Msg_Info['text_info']['forward_group_id'],
        }

        format_msg = {
            'msg_id':Msg_Info['text_info']['msg_id'],
            'grouped_id':Msg_Info['text_info']['grouped_id'],
            'tg_msg':Msg_Info['text_info']['msg_text'],
            'msg_time': self.Time_To_Str(Msg_Info['text_info']['msg_time']),
            'sender_id':Msg_Info['text_info']['sender_id'],
            'group_id':Msg_Info['text_info']['group_id'],
            'read_count':Msg_Info['text_info']['read_count'],
            'reply_info':reply_info,
            'file_info':file_info,
            'forward_info':forward_info,
            'users_reactions':Msg_Info['text_info']['users_reactions'],
            'last_edit_time':self.Time_To_Str(Msg_Info['text_info']['last_edit_time']),

        }
        return format_msg

    def Update_Msg_List(self,Format_Msg:dict):
        """ 
        将获得的 msg 属性添加到待上传的 msg 列表中
        @param Format_Msg: msg 属性
        """ 
        with self.tg_informer.LOCK_UPLOAD_MSG:
            self.tg_informer.UPLOAD_MESSAGE.append(Format_Msg)

    def Store_Msg_In_Json_File(self,Format_Msg:dict):
        """ 
        将获得的 msg 属性保存到本地（以 json 格式）
        @param Format_Msg: msg 属性
        """ 
        now = datetime.now()
        file_date =  now.strftime("%y_%m_%d")
        json_file_name = './file/local_store/message/'+file_date+'_messages.json'
        self.tg_informer.Store_Data_Json(json_file_name, self.tg_informer.LOCK_LOCAL_MSG, Format_Msg)
