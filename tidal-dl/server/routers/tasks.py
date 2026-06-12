"""任务分配 + 状态上报 API"""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional
from database import get_db_dependency
from config import get_config_section

router = APIRouter(prefix="/api/tasks", tags=["Tasks"])


class TaskFetch(BaseModel):
    worker_id: str
    batch_size: int = 10
    account_id: Optional[int] = None


class TaskStatusUpdate(BaseModel):
    status: str                        # downloading / uploading / completed / failed
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    file_size: Optional[int] = None
    actual_quality: Optional[str] = None
    codec: Optional[str] = None
    s3_key: Optional[str] = None

class TaskBatchUpdateItem(BaseModel):
    task_id: int
    status: str
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    file_size: Optional[int] = None
    actual_quality: Optional[str] = None
    codec: Optional[str] = None
    s3_key: Optional[str] = None

class TaskBatchUpdate(BaseModel):
    updates: list[TaskBatchUpdateItem]


@router.get("")
def list_tasks(
    status: str = "",
    job_id: int = 0,
    page: int = 1,
    page_size: int = 50,
    db=Depends(get_db_dependency)
):
    """查看任务列表（管理端）"""
    cursor = db.cursor()
    conditions = []
    params = []

    if status:
        conditions.append("status = %s")
        params.append(status)
    if job_id:
        conditions.append("job_id = %s")
        params.append(job_id)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    offset = (page - 1) * page_size

    cursor.execute(f"SELECT * FROM tasks {where} ORDER BY id LIMIT %s OFFSET %s",
                   params + [page_size, offset])
    tasks = cursor.fetchall()

    cursor.execute(f"SELECT COUNT(*) as total FROM tasks {where}", params)
    total = cursor.fetchone()["total"]

    return {"tasks": tasks, "total": total, "page": page, "page_size": page_size}


@router.post("/fetch")
def fetch_tasks(data: TaskFetch, db=Depends(get_db_dependency)):
    """Worker 拉取待下载任务"""
    cursor = db.cursor()
    download_config = get_config_section(db, "download")

    # 先回收超时任务（状态为 assigned/downloading 但超过 task_timeout 秒未更新）
    timeout = download_config.get("task_timeout", 300)
    cursor.execute(
        "UPDATE tasks SET status = 'pending', assigned_worker_id = NULL, "
        "assigned_account_id = NULL "
        "WHERE status IN ('assigned', 'downloading', 'uploading') "
        "AND updated_at < DATE_SUB(NOW(), INTERVAL %s SECOND)",
        (timeout,)
    )

    # 查找 pending 任务（先查 running jobs）
    cursor.execute("SELECT id FROM jobs WHERE status = 'running'")
    jobs = cursor.fetchall()
    if not jobs:
        db.commit()
        return {"tasks": [], "account": None}
    
    job_ids = [str(j["id"]) for j in jobs]
    jobs_in = ",".join(job_ids)

    cursor.execute(
        f"SELECT * FROM tasks WHERE status = 'pending' AND job_id IN ({jobs_in}) "
        f"LIMIT %s FOR UPDATE SKIP LOCKED",
        (data.batch_size,)
    )
    tasks = cursor.fetchall()

    if not tasks:
        db.commit()
        return {"tasks": [], "account": None}

    # 获取 Worker 指定的账号（由 Worker 本地 AccountPool 选择）
    account = None
    account_id = data.account_id

    if account_id:
        cursor.execute(
            "SELECT id, access_token, refresh_token, token_expires_at, "
            "country_code, user_id, client_id "
            "FROM tidal_accounts WHERE id = %s AND status = 'active'",
            (account_id,)
        )
        account = cursor.fetchone()

    # 标记任务为 assigned
    task_ids = [t["id"] for t in tasks]
    placeholders = ",".join(["%s"] * len(task_ids))
    cursor.execute(
        f"UPDATE tasks SET status = 'assigned', assigned_worker_id = %s, "
        f"assigned_account_id = %s, updated_at = NOW() "
        f"WHERE id IN ({placeholders})",
        [data.worker_id, account_id] + task_ids
    )
    db.commit()

    # 清理敏感信息，只返回必要字段
    clean_tasks = []
    for t in tasks:
        clean_tasks.append({
            "id": t["id"],
            "job_id": t["job_id"],
            "track_id": t["track_id"],
            "title": t["title"],
            "artist": t["artist"],
            "album": t["album"],
            "album_id": t["album_id"],
            "track_number": t["track_number"],
            "duration": t["duration"],
            "audio_quality": t["audio_quality"],
            "isrc": t["isrc"],
            "max_retries": t["max_retries"],
        })

    return {"tasks": clean_tasks, "account": account}


@router.post("/{task_id}/status")
def update_task_status(task_id: int, data: TaskStatusUpdate, db=Depends(get_db_dependency)):
    """Worker 上报任务状态"""
    cursor = db.cursor()

    if data.status == "completed":
        cursor.execute(
            "UPDATE tasks SET status = 'completed', file_size = %s, "
            "actual_quality = %s, codec = %s, s3_key = %s, "
            "completed_at = NOW(), updated_at = NOW() WHERE id = %s",
            (data.file_size, data.actual_quality, data.codec, data.s3_key, task_id)
        )
        # 更新 job 计数
        cursor.execute(
            "UPDATE jobs j SET completed = "
            "(SELECT COUNT(*) FROM tasks t WHERE t.job_id = j.id AND t.status = 'completed') "
            "WHERE j.id = (SELECT job_id FROM tasks WHERE id = %s)",
            (task_id,)
        )
        # 更新账号下载计数 + 重置 rate_limit_count
        cursor.execute(
            "UPDATE tidal_accounts SET total_downloads = total_downloads + 1, "
            "rate_limit_count = 0, last_used_at = NOW() "
            "WHERE id = (SELECT assigned_account_id FROM tasks WHERE id = %s)",
            (task_id,)
        )

    elif data.status == "failed":
        # 检查是否还能重试
        cursor.execute("SELECT retry_count, max_retries FROM tasks WHERE id = %s", (task_id,))
        task = cursor.fetchone()
        if task and task["retry_count"] >= task["max_retries"]:
            new_status = "dead"
        else:
            new_status = "failed"

        cursor.execute(
            "UPDATE tasks SET status = %s, error_code = %s, error_message = %s, "
            "retry_count = retry_count + 1, "
            "assigned_worker_id = NULL, assigned_account_id = NULL, "
            "updated_at = NOW() WHERE id = %s",
            (new_status, data.error_code, data.error_message, task_id)
        )

        # 如果是 failed（可重试），自动重置为 pending
        if new_status == "failed":
            cursor.execute(
                "UPDATE tasks SET status = 'pending' WHERE id = %s",
                (task_id,)
            )

        # 更新 job 失败计数
        cursor.execute(
            "UPDATE jobs j SET failed = "
            "(SELECT COUNT(*) FROM tasks t WHERE t.job_id = j.id AND t.status IN ('failed', 'dead')) "
            "WHERE j.id = (SELECT job_id FROM tasks WHERE id = %s)",
            (task_id,)
        )

    else:
        # downloading / uploading
        cursor.execute(
            "UPDATE tasks SET status = %s, updated_at = NOW() WHERE id = %s",
            (data.status, task_id)
        )

    db.commit()

    # 检查 job 是否全部完成
    cursor.execute(
        "SELECT j.id, j.total_tracks, "
        "(SELECT COUNT(*) FROM tasks t WHERE t.job_id = j.id AND t.status IN ('completed', 'dead')) as done_count "
        "FROM jobs j WHERE j.id = (SELECT job_id FROM tasks WHERE id = %s)",
        (task_id,)
    )
    job = cursor.fetchone()
    if job and job["done_count"] >= job["total_tracks"]:
        cursor.execute("UPDATE jobs SET status = 'completed' WHERE id = %s", (job["id"],))
        db.commit()

    return {"message": "ok"}


@router.post("/batch-status")
def update_task_status_batch(data: TaskBatchUpdate, db=Depends(get_db_dependency)):
    """Worker 批量上报任务状态"""
    cursor = db.cursor()
    updated_jobs = set()

    for item in data.updates:
        task_id = item.task_id
        if item.status == "completed":
            cursor.execute(
                "UPDATE tasks SET status = 'completed', file_size = %s, "
                "actual_quality = %s, codec = %s, s3_key = %s, error_message = %s, "
                "completed_at = NOW(), updated_at = NOW() WHERE id = %s",
                (item.file_size, item.actual_quality, item.codec, item.s3_key, item.error_message, task_id)
            )
            cursor.execute(
                "UPDATE tidal_accounts SET total_downloads = total_downloads + 1, "
                "rate_limit_count = 0, last_used_at = NOW() "
                "WHERE id = (SELECT assigned_account_id FROM tasks WHERE id = %s)",
                (task_id,)
            )
        elif item.status == "failed":
            cursor.execute("SELECT retry_count, max_retries FROM tasks WHERE id = %s", (task_id,))
            task = cursor.fetchone()
            if task and task["retry_count"] >= task["max_retries"]:
                new_status = "dead"
            else:
                new_status = "failed"
            cursor.execute(
                "UPDATE tasks SET status = %s, error_code = %s, error_message = %s, "
                "retry_count = retry_count + 1, "
                "assigned_worker_id = NULL, assigned_account_id = NULL, "
                "updated_at = NOW() WHERE id = %s",
                (new_status, item.error_code, item.error_message, task_id)
            )
            if new_status == "failed":
                cursor.execute("UPDATE tasks SET status = 'pending' WHERE id = %s", (task_id,))
        else:
            cursor.execute(
                "UPDATE tasks SET status = %s, updated_at = NOW() WHERE id = %s",
                (item.status, task_id)
            )
        
        cursor.execute("SELECT job_id FROM tasks WHERE id = %s", (task_id,))
        res = cursor.fetchone()
        if res:
            updated_jobs.add(res["job_id"])

    # 批量更新 Job 统计
    for job_id in updated_jobs:
        cursor.execute(
            "UPDATE jobs j SET "
            "completed = (SELECT COUNT(*) FROM tasks t WHERE t.job_id = j.id AND t.status = 'completed'), "
            "failed = (SELECT COUNT(*) FROM tasks t WHERE t.job_id = j.id AND t.status IN ('failed', 'dead')) "
            "WHERE j.id = %s",
            (job_id,)
        )
        cursor.execute(
            "SELECT id, total_tracks, completed, failed FROM jobs WHERE id = %s",
            (job_id,)
        )
        job = cursor.fetchone()
        if job and (job["completed"] + job["failed"]) >= job["total_tracks"]:
            cursor.execute("UPDATE jobs SET status = 'completed' WHERE id = %s", (job_id,))
            
    db.commit()
    return {"message": "ok", "updated_count": len(data.updates)}

