"""全局配置管理"""
import json
from database import get_db

DEFAULT_CONFIG = {
    "proxy": {
        "host": "0609bdtest.pop.adqczcf.xyz",
        "socks5_port": 41003,
        "http_port": 41002,
        "username": "0609bdtest",
        "password": "0609bdtest",
        "protocol": "socks5"
    },
    "s3": {
        "endpoint": "",
        "access_key": "",
        "secret_key": "",
        "bucket": "",
        "region": "",
        "prefix": "flac/"
    },
    "download": {
        "quality": "LOSSLESS",
        "concurrency": 10,
        "max_retries": 3,
        "retry_delay": 5,
        "rate_limit_delay": 30,
        "task_timeout": 300,
        "country_code": "NG"
    }
}


def get_config(db) -> dict:
    """从数据库读取全部配置"""
    cursor = db.cursor()
    cursor.execute("SELECT `key`, `value` FROM config")
    rows = cursor.fetchall()
    result = {}
    for row in rows:
        key = row["key"]
        value = row["value"]
        try:
            result[key] = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            result[key] = value
    # 合并默认值
    for k, v in DEFAULT_CONFIG.items():
        if k not in result:
            result[k] = v
    return result


def get_config_section(db, section: str) -> dict:
    """获取指定配置段"""
    cursor = db.cursor()
    cursor.execute("SELECT `value` FROM config WHERE `key` = %s", (section,))
    row = cursor.fetchone()
    if row:
        try:
            return json.loads(row["value"])
        except (json.JSONDecodeError, TypeError):
            return {}
    return DEFAULT_CONFIG.get(section, {})


def update_config(db, data: dict):
    """更新配置（部分更新）"""
    cursor = db.cursor()
    for key, value in data.items():
        json_value = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
        cursor.execute(
            "INSERT INTO config (`key`, `value`) VALUES (%s, %s) "
            "ON DUPLICATE KEY UPDATE `value` = %s",
            (key, json_value, json_value)
        )
    db.commit()
