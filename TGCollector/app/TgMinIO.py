import logging
import minio
from minio import Minio
from minio.error import S3Error
import os
from config import env
import sys
import logging_config
import logging
class TGMinIO:
    # MinIO 部分
    def __init__(self):
        self.ip = env.get("MinIO_IP")
        self.access_key = env.get("MINIO_ACCESS_KEY")
        self.secret_key = env.get("MINIO_SECRET_KEY")
        self.bucket_name = env.get("MINIO_BUCKET_NAME") 
        # 初始化 MinIO 客户端
        self.minio_client = Minio(
            self.ip,  # MinIO 服务器地址
            self.access_key,  # 访问密钥
            self.secret_key,  # 秘钥
            secure=False  # 根据你的服务器配置，决定是否启用 HTTPS
        )
        if self.connection_test():
           print("MinIO client initialized")  # 添加调试输出
        else:
           print("MinIO client initialized error!")  # 添加调试输出


    def connection_test(self):
        try:
            # 尝试列出存储桶，验证连接是否成功
            buckets = self.minio_client.list_buckets()
            print("MinIO connection successful. Available buckets:")
            for bucket in buckets:
                print(f"- {bucket.name}")
            return True

        except Exception as err:
            print(f"MinIO connection failed: {err}")
            return False

    # 异步处理：将图片上传到 MinIO
    async def upload_to_minio(self, local_file_path: str, bucket_name: str, subfolder:str):
        try:
            # 获取文件名
            file_name = os.path.basename(local_file_path)
            minio_object_name = f"{subfolder}/{file_name}".replace("\\", "/")

            # 上传前确认文件是否存在
            if os.path.exists(local_file_path):
                logging.info(f"File {local_file_path} exists. Starting upload to MinIO.")
            else:
                logging.error(f"File {local_file_path} does not exist. Skipping upload.")
                return False            
    
            #上传文件到 MinIO
            self.minio_client.fput_object(
                bucket_name,  # 存储桶名称
                minio_object_name,  # 保持原始文件名
                local_file_path,  # 本地文件路径
            )
            logging.info(f"Successfully uploaded {file_name} to {bucket_name} at {minio_object_name}")         
            minio_path = f"{bucket_name}/{minio_object_name}"
            return minio_path  # 返回 MinIO 路径  
        
        except S3Error as exc:
            logging.error(f"MinIO error occurred: {exc}")
            return False  # 上传失败
        except FileNotFoundError as fnf_error:
            logging.error(f"File not found: {fnf_error}")
            return False  # 文件未找到，上传失败
        except Exception as e:
            logging.error(f"An unexpected error occurred during MinIO upload: {e}")
            return False  # 其他异常，上传失败      

    