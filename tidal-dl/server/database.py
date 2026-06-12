"""MySQL 数据库连接管理"""
import pymysql
from contextlib import contextmanager
from dbutils.pooled_db import PooledDB

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "tidal",
    "password": "tidal_dl_2026",
    "database": "tidal_dl",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
}

# 初始化连接池
POOL = PooledDB(
    creator=pymysql,
    maxconnections=100,  # 最大连接数
    mincached=10,        # 初始空闲连接数
    maxcached=50,        # 最大空闲连接数
    maxshared=0,         # 共享连接数
    blocking=True,       # 超过最大连接数时阻塞等待
    maxusage=None,       # 单个连接最大复用次数
    setsession=[],       # 开始会话前执行的命令列表
    ping=1,              # ping MySQL 服务器的频率 (1=每次获取前)
    **DB_CONFIG
)


def get_connection():
    """从连接池获取数据库连接"""
    return POOL.connection()


@contextmanager
def get_db():
    """数据库连接上下文管理器"""
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_db_dependency():
    """FastAPI 依赖注入"""
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()
