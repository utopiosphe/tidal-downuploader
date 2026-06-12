"""
S3 上传模块
"""
import boto3
import logging
import os
from botocore.config import Config as BotoConfig

logger = logging.getLogger("worker.uploader")


class S3Uploader:
    def __init__(self, s3_config: dict):
        self.config = s3_config
        self.enabled = bool(s3_config.get("endpoint") and s3_config.get("bucket"))
        self.client = None

        if self.enabled:
            self.client = boto3.client(
                "s3",
                endpoint_url=s3_config["endpoint"] or None,
                aws_access_key_id=s3_config["access_key"],
                aws_secret_access_key=s3_config["secret_key"],
                region_name=s3_config.get("region", "us-east-1"),
                config=BotoConfig(max_pool_connections=200),
            )
            logger.info(f"S3 已配置: {s3_config['endpoint']} / {s3_config['bucket']}")
        else:
            logger.warning("S3 未配置，下载的文件将保留在本地")

    def upload(self, local_path: str, s3_key: str) -> bool:
        """上传文件到 S3"""
        if not self.enabled:
            logger.debug(f"S3 未启用，跳过上传: {s3_key}")
            return False

        try:
            self.client.upload_file(
                local_path,
                self.config["bucket"],
                s3_key,
            )
            logger.info(f"S3 上传成功: {s3_key}")
            return True
        except Exception as e:
            logger.error(f"S3 上传失败: {s3_key} - {e}")
            raise

    def build_s3_key(self, task: dict, ext: str = "flac") -> str:
        """构建 S3 路径"""
        prefix = self.config.get("prefix", "flac/").rstrip("/")
        track_id = task.get("track_id", "0")
        return f"{prefix}/{track_id}.{ext}"


def _safe_name(name: str) -> str:
    """安全文件名"""
    unsafe = '<>:"/\\|?*\r\n'
    for c in unsafe:
        name = name.replace(c, "_")
    return name.strip(". ")[:200]
