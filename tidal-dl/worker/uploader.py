"""
S3 上传模块 - 支持多存储（AWS / GCS）
"""
import boto3
import logging
import os
import random
from botocore.config import Config as BotoConfig

logger = logging.getLogger("worker.uploader")


class S3Uploader:
    def __init__(self, s3_config: dict):
        self.config = s3_config
        self.storage_id = s3_config.get("id", "default")
        self.provider = s3_config.get("provider", "aws")
        self.enabled = bool(s3_config.get("endpoint") and s3_config.get("bucket"))
        self.client = None

        if self.enabled:
            boto_kwargs = {
                "max_pool_connections": 200,
            }
            # GCS 需要禁用 aws-chunked checksum（boto3 1.43+ 默认开启）
            if self.provider == "gcs":
                boto_kwargs["signature_version"] = "s3v4"
                boto_kwargs["request_checksum_calculation"] = "when_required"
                boto_kwargs["response_checksum_validation"] = "when_required"

            self.client = boto3.client(
                "s3",
                endpoint_url=s3_config["endpoint"] or None,
                aws_access_key_id=s3_config["access_key"],
                aws_secret_access_key=s3_config["secret_key"],
                region_name=s3_config.get("region", "us-east-1"),
                config=BotoConfig(**boto_kwargs),
            )
            logger.info(f"S3 已配置 [{self.storage_id}] ({self.provider}): "
                        f"{s3_config['endpoint']} / {s3_config['bucket']}")
        else:
            logger.warning(f"S3 [{self.storage_id}] 未配置，跳过")

    def upload(self, local_path: str, s3_key: str) -> bool:
        """上传文件到 S3"""
        if not self.enabled:
            logger.debug(f"S3 [{self.storage_id}] 未启用，跳过上传: {s3_key}")
            return False

        try:
            self.client.upload_file(
                local_path,
                self.config["bucket"],
                s3_key,
            )
            logger.info(f"S3 上传成功 [{self.storage_id}]: {s3_key}")
            return True
        except Exception as e:
            logger.error(f"S3 上传失败 [{self.storage_id}]: {s3_key} - {e}")
            raise

    def delete_objects(self, keys: list) -> int:
        """批量删除 S3 对象（每次最多 1000 个）"""
        if not self.enabled or not keys:
            return 0

        objects = [{"Key": k} for k in keys]
        resp = self.client.delete_objects(
            Bucket=self.config["bucket"],
            Delete={"Objects": objects, "Quiet": True}
        )
        errors = resp.get("Errors", [])
        return len(keys) - len(errors)

    def build_s3_key(self, task: dict, ext: str = "flac") -> str:
        """构建 S3 路径"""
        prefix = self.config.get("prefix", "flac/").rstrip("/")
        track_id = task.get("track_id", "0")
        return f"{prefix}/{track_id}.{ext}"


class MultiUploader:
    """管理多个 S3Uploader 实例，随机选取上传"""

    def __init__(self, s3_configs: list):
        self.uploaders = {}
        for cfg in s3_configs:
            if cfg.get("enabled", True):
                uploader = S3Uploader(cfg)
                if uploader.enabled:
                    self.uploaders[uploader.storage_id] = uploader

        self.enabled = len(self.uploaders) > 0
        if self.enabled:
            logger.info(f"MultiUploader 初始化: {len(self.uploaders)} 个存储 "
                        f"({', '.join(self.uploaders.keys())})")
        else:
            logger.warning("MultiUploader: 无可用存储")

    def pick(self) -> S3Uploader | None:
        """随机选取一个可用的 Uploader"""
        if not self.uploaders:
            return None
        return random.choice(list(self.uploaders.values()))

    def get(self, storage_id: str) -> S3Uploader | None:
        """根据 storage_id 获取指定 Uploader"""
        return self.uploaders.get(storage_id)

    def upload(self, local_path: str, task: dict, ext: str = "flac") -> tuple:
        """随机选取存储上传，返回 (storage_id, s3_key)"""
        uploader = self.pick()
        if not uploader:
            return ("", "")

        s3_key = uploader.build_s3_key(task, ext)
        uploader.upload(local_path, s3_key)
        return (uploader.storage_id, s3_key)

    def update(self, s3_configs: list):
        """热更新存储配置"""
        new_ids = set()
        for cfg in s3_configs:
            sid = cfg.get("id", "default")
            new_ids.add(sid)
            if not cfg.get("enabled", True):
                # 禁用的移除
                self.uploaders.pop(sid, None)
                continue
            # 新增或变更的重建
            old = self.uploaders.get(sid)
            if not old or old.config != cfg:
                uploader = S3Uploader(cfg)
                if uploader.enabled:
                    self.uploaders[sid] = uploader

        # 移除已删除的存储
        for sid in list(self.uploaders.keys()):
            if sid not in new_ids:
                del self.uploaders[sid]

        self.enabled = len(self.uploaders) > 0


def _safe_name(name: str) -> str:
    """安全文件名"""
    unsafe = '<>:"/\\|?*\r\n'
    for c in unsafe:
        name = name.replace(c, "_")
    return name.strip(". ")[:200]
