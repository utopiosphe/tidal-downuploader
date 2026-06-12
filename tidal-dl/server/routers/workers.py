"""Worker 注册 + 心跳 API"""
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from typing import Optional
from database import get_db_dependency
from config import get_config

router = APIRouter(prefix="/api/workers", tags=["Workers"])


class WorkerRegister(BaseModel):
    name: str = ""
    hostname: str = ""
    ip: str = ""
    max_concurrency: int = 10


class WorkerHeartbeat(BaseModel):
    status: str = "online"
    active_tasks: int = 0
    total_downloaded: int = 0
    total_failed: int = 0
    total_bytes: int = 0


@router.get("")
def list_workers(db=Depends(get_db_dependency)):
    """获取所有 Worker"""
    cursor = db.cursor()
    cursor.execute("SELECT * FROM workers ORDER BY last_heartbeat DESC")
    return cursor.fetchall()


class WorkerUpdate(BaseModel):
    max_concurrency: Optional[int] = None
    name: Optional[str] = None


@router.put("/{worker_id}")
def update_worker(worker_id: str, data: WorkerUpdate, db=Depends(get_db_dependency)):
    """更新 Worker 配置"""
    cursor = db.cursor()
    updates = []
    params = []
    if data.max_concurrency is not None:
        updates.append("max_concurrency = %s")
        params.append(data.max_concurrency)
    if data.name is not None:
        updates.append("name = %s")
        params.append(data.name)
    if not updates:
        return {"message": "无修改"}
    params.append(worker_id)
    cursor.execute(f"UPDATE workers SET {', '.join(updates)} WHERE id = %s", params)
    db.commit()
    return {"message": "Worker 配置已更新"}


@router.delete("/{worker_id}")
def delete_worker(worker_id: str, db=Depends(get_db_dependency)):
    """删除 Worker"""
    cursor = db.cursor()
    cursor.execute("DELETE FROM workers WHERE id = %s", (worker_id,))
    db.commit()
    return {"message": "Worker 已删除"}


@router.post("/register")
def register_worker(data: WorkerRegister, db=Depends(get_db_dependency)):
    """Worker 注册（同 hostname+name 复用已有记录）"""
    import uuid
    cursor = db.cursor()

    # 查找已有同名 Worker
    cursor.execute(
        "SELECT id FROM workers WHERE hostname = %s AND name = %s LIMIT 1",
        (data.hostname, data.name or data.hostname)
    )
    existing = cursor.fetchone()

    if existing:
        worker_id = existing["id"]
        cursor.execute(
            "UPDATE workers SET ip = %s, status = 'online', "
            "last_heartbeat = NOW() WHERE id = %s",
            (data.ip, worker_id)
        )
    else:
        worker_id = f"w-{uuid.uuid4().hex[:12]}"
        cursor.execute(
            "INSERT INTO workers (id, name, hostname, ip, max_concurrency, status, last_heartbeat, registered_at) "
            "VALUES (%s, %s, %s, %s, %s, 'online', NOW(), NOW())",
            (worker_id, data.name or data.hostname, data.hostname, data.ip, data.max_concurrency)
        )

    # 分配一个可用账号（如果还没有）
    cursor.execute("SELECT assigned_account_id FROM workers WHERE id = %s", (worker_id,))
    w = cursor.fetchone()
    if not w or not w.get("assigned_account_id"):
        cursor.execute(
            "SELECT id FROM tidal_accounts WHERE status = 'active' ORDER BY total_downloads ASC LIMIT 1"
        )
        account = cursor.fetchone()
        if account:
            cursor.execute(
                "UPDATE workers SET assigned_account_id = %s WHERE id = %s",
                (account["id"], worker_id)
            )

    db.commit()

    # 返回完整配置
    config = get_config(db)
    cursor.execute("SELECT assigned_account_id FROM workers WHERE id = %s", (worker_id,))
    w = cursor.fetchone()
    return {
        "worker_id": worker_id,
        "config": config,
        "account_id": w["assigned_account_id"] if w else None
    }


@router.post("/{worker_id}/heartbeat")
def heartbeat(worker_id: str, data: WorkerHeartbeat, db=Depends(get_db_dependency)):
    """Worker 心跳"""
    cursor = db.cursor()
    cursor.execute(
        "UPDATE workers SET status = %s, active_tasks = %s, "
        "total_downloaded = %s, total_failed = %s, total_bytes = %s, "
        "last_heartbeat = NOW() WHERE id = %s",
        (data.status, data.active_tasks,
         data.total_downloaded, data.total_failed, data.total_bytes,
         worker_id)
    )
    db.commit()
    return {"message": "ok"}


@router.get("/{worker_id}/config")
def get_worker_config(worker_id: str, db=Depends(get_db_dependency)):
    """获取 Worker 配置"""
    cursor = db.cursor()
    cursor.execute("SELECT * FROM workers WHERE id = %s", (worker_id,))
    worker = cursor.fetchone()
    if not worker:
        return {"error": "Worker 不存在"}

    config = get_config(db)

    # 并发优先级: Worker 节点设置 > 全局配置
    worker_concurrency = worker.get("max_concurrency")
    effective_concurrency = worker_concurrency or config["download"]["concurrency"]

    return {
        "worker_id": worker_id,
        "concurrency": effective_concurrency,
        "quality": config["download"]["quality"],
        "proxy": config["proxy"],
        "s3": config["s3"],
        "download": config["download"],
    }
