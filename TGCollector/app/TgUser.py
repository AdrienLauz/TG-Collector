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
from TgMSG import MsgCollector


class UserCollector:
    def __init__(self,tg_informer):
        self.tg_informer = tg_informer

    # User 部分
    async def Upload_UserChannel(self,Dialog:Dialog):
        """ 
        获取会话中用户信息，并存储
        @param Dialog: 会话
        """
        users_info = await self.Users_Info(Dialog)

        if (users_info == []):
            return 
        format_users = self.Format_Users(users_info)
        if self.tg_informer.DUMP_MODEL == '1':
            self.Store_Users_In_Json_File(format_users)

        self.tg_informer.Transfer_Users(format_users)

    async def Users_Info(self,Dialog:Dialog)->list:
        """  
        获得完整的用户信息列表
        """ 
        users_info = []
        all_dialogs = await self.tg_informer.CLIENT.get_dialogs()
        count = 0
        try:
            async for user in self.tg_informer.CLIENT.iter_participants(Dialog, aggressive=True):
                count += 1
                # test 下，只采集部分数据
                if env['ENV'] == 'test':
                    if (count > 1):
                        logging.info(f"Test model : Dialog({Dialog.title}) users count more than 1!!!")
                        break 
                user_info = await self.User_Info(user,Dialog = Dialog,All_dialog = all_dialogs)
                users_info.append(user_info)
        except ChatAdminRequiredError as e:
            logging.error(f"(ChatAdminRequiredError)Fail to get participant({Dialog.title})error:({e})")
        except Exception as e:
            logging.error(f"Fail to get participant({Dialog.title})error:({e})")
        return users_info

    async def User_Info(self,User:User,Dialog=None,Location = None,All_dialog = None)->dict:
        """ 
        获取单个用户的完整信息
        """ 
        # 基础信息
        user_id = User.id
        user_username = User.username
        user_firstname = User.first_name
        user_lastname = User.last_name
        user_tel = User.phone
        is_bot = False
        if User.bot is not None:
            is_bot = User.bot
        gather_time = datetime.now()


        # 头像信息
        user_head_img = ''
        pre_photo_path = './file/picture/user/'
        photo_path = pre_photo_path + str(user_id) + '.jpg'
        if os.path.exists(photo_path):
            index = 1
            oldfilename = pre_photo_path + f"{str(user_id)}({index}).jpg"
            while (os.path.exists(oldfilename)):
                index += 1
                oldfilename = pre_photo_path + f"{str(user_id)}({index}).jpg"
            os.rename(photo_path,oldfilename)
        try:
            user_head_img = await self.tg_informer.CLIENT.download_profile_photo(User,file=photo_path)
        except Exception as e:
            logging.error(f"User({user_firstname}) photo download occured an error({e}).")

        # 延迟 2 秒后再上传到 MinIO
        await asyncio.sleep(5)  # 增加延迟，确保文件完全下载 
        upload_success = await self.tg_informer.tg_minio.upload_to_minio(photo_path, self.tg_informer.tg_minio.bucket_name,"user")
        if upload_success:
            try:
                os.remove(photo_path)
                logging.info(f"Successfully deleted local file {photo_path}")
            except Exception as e:
                logging.error(f"Error deleting local file: {e}")
        else:
            logging.error(f"Failed to upload {photo_path} to MinIO.")
            if not os.path.exists(photo_path):
                default_path_photo = "./file/picture/user/default.jpg"
                logging.info(f"File {photo_path} does not exists. Upload default picture {default_path_photo} to MinIO.")
                await self.tg_informer.tg_minio.upload_to_minio(default_path_photo, self.tg_informer.tg_minio.bucket_name,"user")
                    
        
        
        # 群组相关
        user_status = {}
        user_group_id = []
        if Dialog is not None:
            user_group_id.append(str(Dialog.id))
            if User.restricted:
                user_status[str(Dialog.id)] = 'Banned'
            else:
                user_status[str(Dialog.id)] = 'Normal'
        """ if (All_dialog == None):
            dialogs = await self.CLIENT.get_dialogs()
        else:
            dialogs = All_dialog
        for dialog in dialogs:
            dialog_id  = str(dialog.id)
            if dialog_id in self.CHANNEL_PARTICIPANT_ADMIN:
                continue
            try:
                async for user in self.CLIENT.iter_participants(dialog.entity,search=user_firstname):
                    if (user_id != user.id):
                        continue
                    if dialog_id not in user_group_id:
                        user_group_id.append(dialog_id)
                        if user.restricted:
                            user_status[dialog_id] = 'Banned'
                        else:
                            user_status[dialog_id] = 'Normal'
            except ChatAdminRequiredError as e:
                logging.error(f"Fail to get participants({dialog.entity.title}) error:({e})")
                self.CHANNEL_PARTICIPANT_ADMIN.append(dialog_id)
            except Exception as e:
                logging.error(f"Fail to get participants({dialog.entity.title}) error type({type(e)}) error:({e})") """
       
        # 个人信息
        user_intro = ''
        result = None
        try:
            result = await self.tg_informer.CLIENT(functions.users.GetFullUserRequest(User))
        except TimeoutError as e:
            logging.error(f"A timeout occurred while fetching data from the worker.({e})")
        except UserIdInvalidError as e:
            logging.error(f"Invalid object ID for a user. Make sure to pass the right types, for instance making sure that the request is designed for users or otherwise look for a different one more suited.({e})")
        if result is None:
            logging.error(f"User full get error(User entity).")
            if user_username is not None:
                try:
                    result = await self.tg_informer.CLIENT(functions.users.GetFullUserRequest(user_username))
                except TimeoutError as e:
                    logging.error(f"A timeout occurred while fetching data from the worker.({e})")
                    logging.error(f"User full get error(username entity).")
                except UserIdInvalidError as e:
                    logging.error(f"Invalid object ID for a user. Make sure to pass the right types, for instance making sure that the request is designed for users or otherwise look for a different one more suited.({e})")
                    logging.error(f"User full get error(username entity).")
        if result is not None:
            """ print('full user: chats:')
            print(result.chats)
            print('*'*200)
            print('full user: users:')
            print(result.users)
            print('*'*200) """
            user_full = result.full_user
            if user_full is not None:
                user_intro = user_full.about

        # 定位信息()
        if Location is None:
            location = {}
        else:
            location = Location

        user_info = {
            'user_id':user_id,
            'user_username':user_username,
            'user_firstname':user_firstname,
            'user_lastname':user_lastname,
            'user_head_img':user_head_img,
            'user_tel':user_tel,
            'user_status':user_status,
            'gather_time':gather_time,
            'user_group_id':user_group_id,
            'is_bot':is_bot,
            'user_intro':user_intro,
            'location':location,
        }
        return user_info
        
    def Format_Users(self,Users_Info:list)->list:
        """ 
        将用户信息规范化
        """
        format_users = []
        for user in Users_Info:
            user_nickname = str(user['user_firstname'])
            if (user['user_lastname'] is not None):
                user_nickname +=  str(user['user_lastname'])
            gather_time = self.tg_informer.msg_collector.Time_To_Str(user['gather_time'])
            if (user['location'] != {}):
                location_date = self.tg_informer.msg_collector.Time_To_Str(user['location']['location_date'])
                location = {
                    'location_diatance':user['location']['location_diatance'],
                    'location_long':user['location']['location_long'],
                    'location_lat':user['location']['location_lat'],
                    'location_date':location_date,
                    'location_area':user['location']['location_area']
                }
                full_location = [location]
            else:
                full_location = []
            format_user = {
                'user_id':str(user['user_id']),
                'user_nickname':user_nickname,
                'user_username':user['user_username'],
                'user_about':user['user_intro'],
                'user_group_id':user['user_group_id'],
                'user_tel':user['user_tel'],
                'user_status':user['user_status'],
                'head_img':user['user_head_img'],
                'user_is_bot':user['is_bot'],
                'gather_time':gather_time,
                'location':full_location
            }
            format_users.append(format_user)
        return format_users

    def Store_Users_In_Json_File(self,Format_Users:list):
        """ 
        将规范的 users 信息进行本地保存（以 json 格式）
        @param Format_Users: 规范的 users 信息列表
        """ 
        now = datetime.now()
        file_date =  now.strftime("%y_%m_%d")
        json_file_name = './file/local_store/user_info/'+file_date+'_chat_user.json'
        for user in Format_Users:
            self.tg_informer.Store_Data_Json(json_file_name, self.tg_informer.LOCK_LOCAL_USER,user)

    async def Nearly_Geo_User(self):
        """
        获取附近的人，通过文件中指定的经纬度
        """ 

        # 根据配置搜索坐标
        if env['GEO_TYPE'] == '1':
            # 国内
            file_path ="./Domestic_Geo_Point.json"
        elif env['GEO_TYPE'] == '2':
            # 海外
            file_path ="./Overseas_Geo_Point.json"
        elif env['GEO_TYPE'] == '3':
            # 全部
            file_path = "./Geo_Point.json"
        else:
            return

        if not os.path.exists(file_path):
            logging.error("Not found Geo_Point.json file!!!!!")
            return
        with open(file_path,encoding='utf-8') as f:
            geo_data = json.load(f)

        logging.info("Begin to get nearly users")
        print(geo_data)

        while True:
            if env.VIRTU_NEAR_USERS == 0:
                
                break
            for key in geo_data.keys():
                location = geo_data[key]
                location['change_latitude'] = 0
                location['change_longitude'] = 0
                location['move_latitude'] = location['latitude']
                location['move_longitude'] = location['longitude']

                area = self.Detec_Geo(Geo=location)
                await self.Geo_Detect_Users(location,area)

                if env['GEO_FAST'] == '1':
                    randomnum = random.random()
                    fasttime = int(60*5*randomnum)
                    logging.info(f"Fast geo detect , will sleep ({5 * randomnum})mins = ({fasttime})s")
                    await asyncio.sleep(fasttime)
                    continue

                location = self.Rand_distance(location)
                while location is not None:
                    area = self.Detec_Geo(Geo=location)
                    await self.Geo_Detect_Users(location,area)
                    location = self.Rand_distance(location)
                
                if env['GEO_FAST'] != '1':
                    randomnum = random.random()
                    interval_time = int(60 * 60 * randomnum)
                    logging.info(f"Move to next city, will sleep ({60 * randomnum})mins = ({interval_time})s")
                    await asyncio.sleep(interval_time)

            randomnum = random.random()
            if env['GEO_FAST'] != '1':
                interval_time = int(60 * 60 * 2 * randomnum)
                logging.info (f"Geo detect area finish, will sleep ({randomnum*10})mins = ({interval_time})s")
            else:
                interval_time = int(60 * 60 * 5 * randomnum)
                logging.info (f"Geo detect area finish, will sleep ({randomnum*5})hours = ({interval_time})s")
            await asyncio.sleep(interval_time)

    def Detec_Geo(self,Geo:dict)->str:
        """ 
        识别给定经纬度的对应的实际城市
        @param Geo: 给定的经纬度信息
        @return: 识别的结果
        """
        latitude = Geo['move_latitude']
        longitude = Geo['move_longitude']
        area = ''

        if (env.GEONAME_AK == ""):
            logging.error("Geo ak is null,please fill it!!!!")
            logging.error("Geo will close!!!!!")
            env.VIRTU_NEAR_USERS = 0
            return area

        url = "https://api.map.baidu.com/reverse_geocoding/v3/?ak="+env.GEONAME_AK+"&language=zh-CN&language_auto=1&extensions_town=true&output=json&coordtype=wgs84ll&location="+str(latitude)+','+str(longitude)

        response = requests.get(url)
        answer = response.json()

        if (answer['status']!=0):
            logging.error(f"Baidu geo error,erro code is {answer['status']}!!!")
        else:
            area = answer['result']['formatted_address']
        return area

    async def Geo_Detect_Users(self,Location:dict,Area:str):
        """
        根据给予的经纬度数据搜索附近的人
        @param geo: 给予的经纬度信息
        @param tag: 对于给定的经纬度的现实城市位置信息
        """
        locationtime = datetime.now()

        latitude = Location['move_latitude']
        longitude = Location['move_longitude']

        location = {

            'location_long':Location['move_longitude'],
            'location_lat':Location['move_latitude'],
            'location_date':locationtime,
            'location_area':Area
        }

        input_geo_point = InputGeoPoint(lat=float(latitude),long=float(longitude),accuracy_radius=42)
        try:
            result = await self.tg_informer.CLIENT(functions.contacts.GetLocatedRequest(geo_point=input_geo_point))
        except UserpicUploadRequiredError as e:
            logging.error(f"You must have a profile picture before using this method.({e})")
            return 
        except Exception as e:
            logging.error(f"An error occured about GetLocatedRequest({e})")
            return 

        if len(result.updates) > 0:
            users_result = result.updates[0].peers
        else:
            logging.error(f"Geo no users in location!!!!")
            print("result")
            print(result)
            return
        users_info = []
        for user in users_result:
            location = {
                'location_distance':0,
                'location_long':Location['move_longitude'],
                'location_lat':Location['move_latitude'],
                'location_date':locationtime,
                'location_area':Area
            }
            if hasattr(user,"distance"):
                    location['distance'] = user.distance

            if isinstance(user.peer,PeerUser):
                try:
                    user_id = user.peer.user_id
                    user_entity = await self.tg_informer.CLIENT.get_entity(user_id)
                except Exception as e:
                    logging.error()
                    continue
                
                user_info = self.User_Info(user_entity,Location=location)
            users_info.append(user_info)

        if (users_info == []):
            logging.error(f"Geo no user in location!!!!")
            print('location result')
            print(result)
            return 

        format_users = self.Format_Users(users_info)
        if self.tg_informer.DUMP_MODEL == '1':
            self.Store_Users_In_Json_File(format_users)

        self.Transfer_Users(format_users)
        logging.info(f"Geo user successful count({len(users_info)})")

    def Rand_distance(self, Location:dict)->dict:
        """
        随机的移动经纬度坐标
        每次移动0~1公里
        环绕初始经纬度周围 50 公里
        """
        # 移动的距离
        num1 = (int(random.random()*10000))/1000
        lat_distance = num1 * 0.01 
        num2 = (int(random.random()*10000))/1000
        long_distance = num2 * 0.009
        Geo['change_latitude'] += lat_distance
        Geo['change_longitude'] += long_distance

        # 移动的方向
        signed1 = random.randint(-1,1)
        signed2 = random.randint(-1,1)

        # 确定移动后的经纬度
        latitude = float(Geo['latitude']) + signed1 * Geo['change_latitude']
        longitude = float(Geo['longitude']) + signed2 * Geo['change_longitude']

        # 修正纬度（-90~+90）
        if latitude >= 90 or latitude <= -90:
            latitude = float(Geo['latitude']) + (-1)* signed1 * Geo['change_latitude']

        # 修正经度（-180~+180）
        if longitude > 180:
            longitude = longitude - 360

        # 返回值
        Geo['move_latitude'] = '%.7f'%latitude
        Geo['move_longitude'] = '%.6f'%longitude

        # 指定在方圆 50 km 以内
        coord_1 = (float(Geo['latitude']),float(Geo['longitude']))
        coord_2 = (float(Geo['move_latitude']),float(Geo['move_longitude']))
        distance = geodesic(coord_1, coord_2).km
        if (distance > 50):
            return None
        logging.info(f"Geo after move ({Geo})")
        return Geo

