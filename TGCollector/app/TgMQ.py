
import pika
from pika.exceptions import ConnectionClosed,ChannelClosed,ChannelWrongStateError
import logging
import sys
import copy
import telegram_pb2
import time
from config import env
import logging_config
import logging
class TGMQ:
    def __init__(self,tg_informer):
        self.tg_informer = tg_informer
        self.MQ_MSG_TOPIC = env.get("MQ_MSG_TOPIC")
        self.MQ_USERS_TOPIC = env.get("MQ_USERS_TOPIC")
        self.MQ_CHANNEL_TOPIC = env.get("MQ_CHANNEL_TOPIC")
        self.MQ_RELATION_TOPIC = env.get("MQ_RELATION_TOPIC")
        self.username = env.get("MQ_USERNAME")
        self.password = env.get("MQ_PASSWORD")
        self.mq_ip = env.get("MQ_IP")
        try:
            self.mq_port = env.get("MQ_PORT")
        except ValueError as e:
            logging.error('MQ port is illegal, please check it!!!!')
            sys.exit(0)
        except Exception as e:
            logging.error('Error in type conversion of MQ port!!!!')
            sys.exit(0)


        # self.LOCK_UPLOAD_MSG = self.tg_informer.LOCK_UPLOAD_MSG
        # self.UPLOAD_MESSAGE = self.tg_informer.UPLOAD_MESSAGE

    def Mq_Connect(self,Env:dict):
        """
        采用 RabbitMQ 来进行传输
        和 mq 建立连接，向指定 topic 发送消息
        @return: 和 rabbitmq 的连接
        """
        user_info = pika.PlainCredentials(self.username,self.password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host = self.mq_ip,port = self.mq_port,credentials = user_info, heartbeat=0,connection_attempts=5))
        self.MQ_CONNECT = connection
        mq_client = connection.channel()

        mq_client.exchange_declare(exchange=self.MQ_MSG_TOPIC,exchange_type="topic", durable=True)
        mq_client.exchange_declare(exchange=self.MQ_USERS_TOPIC,exchange_type="topic", durable=True)
        mq_client.exchange_declare(exchange=self.MQ_CHANNEL_TOPIC,exchange_type="topic", durable=True)
        mq_client.exchange_declare(exchange=self.MQ_RELATION_TOPIC,exchange_type="topic", durable=True)

        # 声明并绑定队列到 Exchange
        queue_msg = mq_client.queue_declare(queue='queue_msg', durable=True)
        queue_users = mq_client.queue_declare(queue='queue_users', durable=True)
        queue_channel = mq_client.queue_declare(queue='queue_channel', durable=True)
        queue_relation = mq_client.queue_declare(queue='queue_relation', durable=True)

        # 绑定队列到 Exchange
        mq_client.queue_bind(exchange=self.MQ_MSG_TOPIC, queue='queue_msg', routing_key='#')  # 使用 # 作为通配符
        mq_client.queue_bind(exchange=self.MQ_USERS_TOPIC, queue='queue_users', routing_key='#')
        mq_client.queue_bind(exchange=self.MQ_CHANNEL_TOPIC, queue='queue_channel', routing_key='#')
        mq_client.queue_bind(exchange=self.MQ_RELATION_TOPIC, queue='queue_relation', routing_key='#')

        return mq_client

    def Rabbitmq_Single_Publish(self,Topic:str,Msg:bytes):
        """ 
        向 rabbitmq 指定的 topic 发送消息
        @param Topic: mq 的 topic
        @param Msg: 需要发送的消息
        """
        times = 100
        while times > 0:
            try:
                self.MQ_CLIENT.basic_publish(
                    exchange=Topic,
                    routing_key="",
                    body=Msg,
                    mandatory=True,
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                if times != 100:
                    logging.info ("Fixing rabbitmq connection Successfully")
                return
            except (ConnectionClosed, ChannelClosed, ChannelWrongStateError) as e:
                logging.error("Rabbitmq connection error!!!")
                logging.error('We try to reconnect')
                self.Rabbitmq_Reconnect()
                times -= 1
        logging.error(f"Error about Rabbitmq has happened")
        #print('end updata')

    def Rabbitmq_Multi_Publish(self,Topic:str,Msg:list):
        """ 
        向 rabbitmq 指定的 topic 发送消息
        @param Topic: mq 的 topic
        @param Msg: 需要发送的消息
        """
        for i in Msg:
            self.Rabbitmq_Single_Publish(Topic,i)

    def Rabbitmq_Reconnect(self):
        """
        重新连接 rabbitmq
        """
        logging.info("Fixing the rabbitmq connection.")
        try:
            self.MQ_CONNECT.close()
        except:
            logging.error(f"Fail close mq connect")
            sys.exit(0)

        self.MQ_CLIENT =self.Mq_Connect(self.ENV)
        time.sleep(5)
    # 需要修改了
    def Mq_Msg_Transfer(self):
        """
        将 msg 的属性传给 mq
        """ 
        with self.tg_informer.LOCK_UPLOAD_MSG:
            messages_list = copy.deepcopy(self.tg_informer.UPLOAD_MESSAGE)
            self.tg_informer.UPLOAD_MESSAGE = []

        if messages_list == []:
            return

        mq_msg_list = []
        for i in messages_list:
            messagepb = telegram_pb2.MessagePb()

            # 安全地获取各个字段并检查 None
            messagepb.msg_id = int(i.get('msg_id', 0) if i.get('msg_id') is not None else 0)
            messagepb.grouped_id = str(i.get('grouped_id', ''))
            messagepb.tg_msg = str(i.get('tg_msg', '') if i.get('tg_msg') is not None else '')
            messagepb.msg_time = str(i.get('msg_time', '') if i.get('msg_time') is not None else '')
            messagepb.sender_id = str(i.get('sender_id', '') if i.get('sender_id') is not None else '')
            messagepb.group_id = int(i.get('group_id', 0) if i.get('group_id') is not None else 0)
            messagepb.read_count = int(i.get('read_count', 0) if i.get('read_count') is not None else 0)

            # 处理 reply_info，确保安全访问
            if i.get('reply_info'):
                mq_reply_info = messagepb.reply_info
                mq_reply_info.is_reply = bool(i['reply_info'].get('is_reply', False))
                mq_reply_info.reply_id = str(i['reply_info'].get('reply_id', '') if i['reply_info'].get('reply_id') is not None else '')

            # 处理 file_info，使用 get() 访问字段
            if i.get('file_info'):
                mq_file_info = messagepb.file_info
                mq_file_info.file_type = str(i['file_info'].get('file_type', '') if i['file_info'].get('file_type') is not None else '')
                mq_file_info.file_store = str(i['file_info'].get('file_store', '') if i['file_info'].get('file_store') is not None else '')
                mq_file_info.file_name = str(i['file_info'].get('file_name', '') if i['file_info'].get('file_name') is not None else '')
                mq_file_info.file_hash = str(i['file_info'].get('file_hash', '') if i['file_info'].get('file_hash') is not None else '')
                mq_file_info.file_size = int(i['file_info'].get('file_size', 0) if i['file_info'].get('file_size') is not None else 0)

            # 处理 forward_info，确保字段安全
            if i.get('forward_info'):
                mq_forward_info = messagepb.forward_info
                mq_forward_info.forward_user_id = str(i['forward_info'].get('forward_user_id', '') if i['forward_info'].get('forward_user_id') is not None else '')
                mq_forward_info.forward_group_id = str(i['forward_info'].get('forward_group_id', '') if i['forward_info'].get('forward_group_id') is not None else '')

            # 处理 reactions，确保安全访问
            if i.get('users_reactions'):
                for reaction_key, reaction_values in i['users_reactions'].items():
                    user_reaction = telegram_pb2.UserReaction()  # 创建新的反应对象
                    if isinstance(reaction_values, list):
                        for reaction_value in reaction_values:
                            user_reaction.value.append(str(reaction_value))
                    messagepb.users_reactions[reaction_key].CopyFrom(user_reaction)

            # 处理最后编辑时间
            if i.get('last_edit_time'):
                messagepb.last_edit_time = str(i.get('last_edit_time', '') if i.get('last_edit_time') is not None else '')

            bin_mq_msg = messagepb.SerializeToString()
            mq_msg_list.append(bin_mq_msg)
        
        self.Rabbitmq_Multi_Publish(self.MQ_MSG_TOPIC,mq_msg_list)
        logging.info(f'Upload msg count{len(mq_msg_list)}')

    def Mq_Channel_Transfer(self,Format_Channel:dict):
        """
        channel 的属性需要传输给 mq 服务器上
        @param Format_Channel: 规范的channel 属性
        """ 
        mq_channel = telegram_pb2.ChannelPb()

        # 安全获取字段并检查 None
        mq_channel.group_id = int(Format_Channel.get('group_id', 0) if Format_Channel.get('group_id') is not None else 0)
        mq_channel.group_name = str(Format_Channel.get('group_name', '') if Format_Channel.get('group_name') is not None else '')
        mq_channel.group_intro = str(Format_Channel.get('group_intro', '') if Format_Channel.get('group_intro') is not None else '')
        mq_channel.group_url = str(Format_Channel.get('group_url', '') if Format_Channel.get('group_url') is not None else '')
        mq_channel.group_type = str(Format_Channel.get('group_type', '') if Format_Channel.get('group_type') is not None else '')
        mq_channel.is_private = bool(Format_Channel.get('is_private', False) if Format_Channel.get('is_private') is not None else False)
        mq_channel.group_head_img = str(Format_Channel.get('group_head_img', '') if Format_Channel.get('group_head_img') is not None else '')
        mq_channel.member_count = int(Format_Channel.get('member_count', 0) if Format_Channel.get('member_count') is not None else 0)
        mq_channel.gather_time = str(Format_Channel.get('gather_time', '') if Format_Channel.get('gather_time') is not None else '')
    
        # 处理群公告
        group_announcement = Format_Channel.get('group_announcement')
        if group_announcement is not None:
            if isinstance(group_announcement, list):
                mq_channel.group_announcement[:] = [str(a) for a in group_announcement]
            else:
                logging.error(f"Invalid group_announcement format: expected list, got {type(group_announcement).__name__}")
    
        # 处理管理员
        group_admin = Format_Channel.get('group_admin')
        if group_admin is not None:
            if isinstance(group_admin, list):
                mq_channel.group_admin[:] = [str(a) for a in group_admin]
            else:
                logging.error(f"Invalid group_admin format: expected list, got {type(group_admin).__name__}")

 #       # 处理地理位置（如果存在）
#        location = Format_Channel.get('location')
 #       if location is not None:
  #          if isinstance(location, dict):
  #              mq_location = mq_channel.location
  #              mq_location.address = str(location.get('address', '') if location.get('address') is not None else '')
  #          else:
   #             logging.error(f"Invalid location format: expected dict, got {type(location).__name__}")

        bin_mq_channel = mq_channel.SerializeToString()
        self.Rabbitmq_Single_Publish(self.MQ_CHANNEL_TOPIC,bin_mq_channel)

    def Mq_Users_Transfer(self,Format_Users:list):
        """ 
        users 的属性需要传输给 mq 服务器上
        @param Format_Users: 规范的 users 列表
        """ 
        bin_mq_users_list = []

        for i in Format_Users:
            mq_user = telegram_pb2.UserPb()

            # 安全获取字段并检查 None
            mq_user.user_id = str(i.get('user_id', '') if i.get('user_id') is not None else '')
            mq_user.user_nickname = str(i.get('user_nickname', '') if i.get('user_nickname') is not None else '')
            mq_user.user_username = str(i.get('user_username', '') if i.get('user_username') is not None else '')
            mq_user.user_about = str(i.get('user_about', '') if i.get('user_about') is not None else '')


            # 处理群组 ID，确保 user_group_id 为列表
            user_group_id = i.get('user_group_id')
            if user_group_id is not None and isinstance(user_group_id, list):
                mq_user.user_group_id[:] = [str(gid) for gid in user_group_id]
            else:
                logging.info(f"user_group_id is None or not a list for user {i.get('user_id', '')}")

            mq_user.user_tel = str(i.get('user_tel', '') if i.get('user_tel') is not None else '')

            # 处理用户状态，确保 user_status 是字典
            user_status = i.get('user_status')
            if user_status is not None and isinstance(user_status, dict):
                for status_key, status_values in user_status.items():
                    user_status_pb = telegram_pb2.UserStatus()
                    user_status_pb.key = str(status_key)
                    if isinstance(status_values, list):
                        user_status_pb.value.extend([str(value) for value in status_values])
                    mq_user.user_status.append(user_status_pb)
            else:
                logging.info(f"user_status is None or not a dict for user {i.get('user_id', '')}")

            mq_user.head_img = str(i.get('head_img', '') if i.get('head_img') is not None else '')
            mq_user.user_is_bot = bool(i.get('user_is_bot', False) if i.get('user_is_bot') is not None else False)
            mq_user.gather_time = str(i.get('gather_time', '') if i.get('gather_time') is not None else '')

            # 处理定位信息（如有需要）
            location = i.get('location')
 #           if location is not None and isinstance(location, dict):
  #              mq_user_location = mq_user.location
  #              mq_user_location.distance = int(location.get('distance', 0) if location.get('distance') is not None else 0)
  #              mq_user_location.long = float(location.get('longitude', 0.0) if location.get('longitude') is not None else 0.0)
  #              mq_user_location.lat = float(location.get('latitude', 0.0) if location.get('latitude') is not None else 0.0)
  #              mq_user_location.area = str(location.get('area', '') if location.get('area') is not None else '')
   #         else:
   #             logging.info(f"location is None or not a dict for user {i.get('user_id', '')}")


            bin_mq_user = mq_user.SerializeToString()
            bin_mq_users_list.append(bin_mq_user)

        self.Rabbitmq_Multi_Publish(self.MQ_USERS_TOPIC,bin_mq_users_list)
    # 修改结束
