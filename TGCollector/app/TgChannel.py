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
import logging_config
import logging
from telethon import TelegramClient,events,functions
from telethon.tl.types import PeerUser, PeerChat, PeerChannel,Dialog,User,Channel,Chat,ChannelParticipantsAdmins,InputGeoPoint,InputPeerChannel,InputPeerChat
from telethon.errors.rpcerrorlist import ChannelPrivateError,ChannelPublicGroupNaError,ChannelInvalidError,PeerIdInvalidError,ChatIdInvalidError,UserIdInvalidError,ChatAdminRequiredError
from telethon.tl import types

from datetime import datetime
from geopy.distance import geodesic


from config import env
from TgMSG import MsgCollector

class ChannelCollector:
    def __init__(self,tg_informer):
        self.tg_informer = tg_informer

       

    # Channel 部分
    async def Upload_Channel(self,Dialog:Dialog):
        """ 
        获取会话对应群组信息上传，并记录下基本信息
        @param Dialog: 会话
        """ 
        group_info = await self.Group_Info(Dialog)
        if group_info == None:
            return
        self.Upload_Group(group_info)

    async def Group_Info(self,Dialog:Dialog)->dict:
        """ 
        获取会话对应群组信息
        """
        group_id = Dialog.id 
        group_name = Dialog.title
        if group_name == "" or group_name == None:
            logging.error(f"Channel name error. Group ID:{group_id}")
            return None
        gather_time = datetime.now()
        is_private = False
        group_member_count = 0
        try:
            group_member_count = Dialog.entity.participants_count
        except Exception as e:
            logging.error(f"Can't get group({group_name}) count.")

        input_dialog = Dialog.input_entity

        # dialog 类型判断
        group_url = []
        group_type = 'group'
        if isinstance(Dialog.entity, Channel):
            channel_usernames = Dialog.entity.usernames
            if channel_usernames is not None:
                for i in channel_usernames:
                    username = i.username
                    url = 'https://t.me/'+str(username)
                    group_url.append(url)
            group_type = 'channel'
            if Dialog.entity.username is not None:
                url = 'https://t.me/'+str(Dialog.entity.username)
                if url not in group_url:
                    group_url.append(url)
            if not(Dialog.entity.broadcast):
                group_type = 'megagroup'
        elif (isinstance(Dialog.entity, Chat)):
            if hasattr(Dialog.entity,'usernames'):
                for i in channel_usernames:
                    username = i.username
                    url = 'https://t.me/'+str(username)
                    group_url.append(url)
            if hasattr(Dialog.entity,'username'):
                url = 'https://t.me/'+str(Dialog.entity.username)
                if url not in group_url:
                    group_url.append(url)
            group_type = 'group'
            if group_member_count > 200:
                group_type = 'megagroup'

        # group 图片
        group_head_img = ""
        photo_path = f'./file/picture/channel/' + str(int(group_id)) + '.jpg'
        if os.path.exists(photo_path):
            index = 1
            oldfilename = f'./file/picture/channel/' + str(int(group_id))+ f'({str(index)})' + '.jpg'
            while (os.path.exists(oldfilename)):
                index += 1
                oldfilename = f'./file/picture/channel/' + str(int(group_id))+ f'({str(index)})' + '.jpg'
            os.rename(photo_path,oldfilename)

        try:
            real_photo_path = await self.tg_informer.CLIENT.download_profile_photo(Dialog.input_entity,file=photo_path)
            # group_head_img = real_photo_path
        except Exception as e:
            logging.error(f"Get an group pic download error: ({e})")

        # 延迟 2 秒后再上传到 MinIO
        await asyncio.sleep(5)  # 增加延迟，确保文件完全下载       

        minio_path = await self.tg_informer.tg_minio.upload_to_minio(photo_path, self.tg_informer.tg_minio.bucket_name, "channel")

        if minio_path:
            try:
                group_head_img = minio_path  # 使用 MinIO 路径
                os.remove(photo_path)
                logging.info(f"Successfully deleted local file {photo_path}")
            except Exception as e:
                logging.error(f"Error deleting local file: {e}")
        else:
            logging.error(f"Failed to upload {photo_path} to MinIO.")
            if not os.path.exists(photo_path):
                default_path_photo = "./file/picture/user/default.jpg"
                logging.info(f"File {photo_path} does not exists. Upload default picture {default_path_photo} to MinIO.")
                await self.tg_informer.tg_minio.upload_to_minio(default_path_photo, self.tg_informer.tg_minio.bucket_name,"channel")
            


        # Full Entity 获取信息
        group_intro = ""
        result = None
        # print(f"type group:{type(input_dialog)}")
        if isinstance(input_dialog, InputPeerChannel):
            try:
                result = await self.tg_informer.CLIENT(functions.channels.GetFullChannelRequest(input_dialog))
                logging.info(f'full channel:{group_name}')
            except TimeoutError as e:
                logging.error(f"A timeout occurred while fetching data from the worker.({e})")
            except ChannelPublicGroupNaError as e:
                logging.error(f"Channel/Supergroup not available.({e})")
            except ChannelPrivateError as e:
                logging.error(f"The channel specified is private and you lack permission to access it. Another reason may be that you were banned from it.{e}")
                is_private = True
            except ChannelInvalidError as e:
                logging.error(f"Invalid channel object. Make sure to pass the right types, for instance making sure that the request is designed for channels or otherwise look for a different one more suited.({e})")
            except Exception as e:
                logging.error (f"Channel full Other error occurred({e})")
            if result is not None:
                full_chat = result.full_chat
                if (full_chat is not None):
                    group_intro = full_chat.about
        elif isinstance(input_dialog, InputPeerChat):
            try:
                print(input_dialog)
                result = await self.tg_informer.CLIENT(functions.messages.GetFullChatRequest(input_dialog.chat_id))                
                logging.info(f'full chat:{group_name}')
            except PeerIdInvalidError as e:
                logging.error(f"An invalid Peer was used. Make sure to pass the right peer type and that the value is valid (for instance, bots cannot start conversations).({e})")
            except ChatIdInvalidError as e:
                logging.error(f"Invalid object ID for a chat. Make sure to pass the right types, for instance making sure that the request is designed for chats (not channels/megagroups) or otherwise look for a different one more suited\nAn example working with a megagroup and AddChatUserRequest, it will fail because megagroups are channels. Use InviteToChannelRequest instead.({e})")
            except Exception as e:
                logging.error(f"Get an error ({group_name}):{e}")
            if result is not None:
                full_chat = result.full_chat
                if (full_chat is not None):
                    group_intro = full_chat.about

        # 获取置顶消息
        group_announcement = []
        logging.info(f"Get group announcement.")
        try:
            async for msg in self.tg_informer.CLIENT.iter_messages(Dialog.entity, filter=types.InputMessagesFilterPinned()):
                group_announcement.append(msg.id)
        except Exception as e:
            logging.error(f"Group announcement error:({e}).")

        # 获取管理员用户
        group_admin = []
        logging.info(f"Get group admin.")
        try:
            async for user in self.tg_informer.CLIENT.iter_participants(Dialog.entity, filter=ChannelParticipantsAdmins):
                group_admin.append(str(user.id))
            print(f" group admin successful:group({group_name}):({group_admin})")
        except Exception as e:
            logging.error(f"Group admin error:({e}).")

        group_info = {
            'group_id':group_id,
            'group_name':group_name,
            'group_intro':group_intro,
            'group_type':group_type,
            'is_private':is_private,
            'group_url':group_url,
            'group_head_img':group_head_img,
            'group_member_count':group_member_count,
            'gather_time':gather_time,
            'group_announcement':group_announcement,
            'group_admin':group_admin
        }
        flag = True
        for i in self.tg_informer.CHANNEL_META:
            if group_id == i['channel id']:
                flag = False
        if flag:
            group_meta = {
                'channel id':group_id,
                'channel name':group_name,
                'channel about': group_intro,
            }
            with self.tg_informer.LOCK_CHANNEL_ADD:
                self.tg_informer.CHANNEL_META.append(group_meta)
        return group_info
        
    def Upload_Group(self,Group_Info:dict):
        """ 
        将 channel 信息存储下来
        @param Group_Info: 获得的完整 Channel 信息
        """ 
        format_channel = self.Format_Channel(Group_Info)

        if self.tg_informer.DUMP_MODEL == '1':
            self.Store_Channel_In_Json_File(format_channel)

        self.tg_informer.Transfer_Channel(format_channel)

    def Format_Channel(self,Group_Info:dict)->dict:
        """ 
        将获得的完整 channel 属性转换为所需要的格式
        @param Group_Info: 完整的 channel 属性
        @return: 规范格式的 channel 属性
        """ 
        gather_time = self.tg_informer.msg_collector.Time_To_Str(Group_Info['gather_time'])

        format_channel = {
            'group_id':Group_Info['group_id'],
            'group_name':Group_Info['group_name'],
            'group_intro':Group_Info['group_intro'],
            'group_url':Group_Info['group_url'],
            'group_type':Group_Info['group_type'],
            'is_private':Group_Info['is_private'],
            'group_head_img':Group_Info['group_head_img'],
            'member_count':Group_Info['group_member_count'],
            'gather_time':gather_time,
            'group_announcement':Group_Info['group_announcement'],
            'group_admin':Group_Info['group_admin']
        }
        return format_channel

    def Store_Channel_In_Json_File(self,Format_Channel:dict):
        """ 
        将获得的 channel 属性保存到本地（以 json 格式）
        @param Format_Channel: 规范的channel 属性
        """ 
        now = datetime.now()
        file_date = now.strftime("%y_%m_%d")
        json_file_name = './file/local_store/channel_info/'+file_date+'_channel_info.json'

        self.tg_informer.Store_Data_Json(json_file_name, self.tg_informer.LOCK_LOCAL_CHANNEL, Format_Channel)
