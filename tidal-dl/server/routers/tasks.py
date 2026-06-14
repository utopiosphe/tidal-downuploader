"""任务分配 + 状态上报 API"""
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
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
    account_id: Optional[int] = None
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
        # 更新 job 计数 (增量更新，避免扫千万级全表造成死锁)
        cursor.execute(
            "UPDATE jobs SET completed = completed + 1 "
            "WHERE id = (SELECT job_id FROM tasks WHERE id = %s)",
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

        # 更新 job 失败计数 (增量更新)
        if new_status == "dead":
            cursor.execute(
                "UPDATE jobs SET failed = failed + 1 "
                "WHERE id = (SELECT job_id FROM tasks WHERE id = %s)",
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
                "assigned_account_id = COALESCE(%s, assigned_account_id), "
                "completed_at = NOW(), updated_at = NOW() WHERE id = %s",
                (item.file_size, item.actual_quality, item.codec, item.s3_key, item.error_message, item.account_id, task_id)
            )
            if item.account_id:
                cursor.execute(
                    "UPDATE tidal_accounts SET total_downloads = total_downloads + 1, "
                    "rate_limit_count = 0, last_used_at = NOW() "
                    "WHERE id = %s",
                    (item.account_id,)
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

    # 批量更新 Job 统计 (改为增量，避免严重死锁)
    for job_id in updated_jobs:
        # 这里为了极致性能，不再实时聚合，仅检查是否满足完成条件
        # 我们用一个轻量的查询，如果有必要可以在外围跑一个定时对账任务
        cursor.execute(
            "SELECT id, total_tracks, completed, failed FROM jobs WHERE id = %s",
            (job_id,)
        )
        job = cursor.fetchone()
        if job and (job["completed"] + job["failed"]) >= job["total_tracks"]:
            cursor.execute("UPDATE jobs SET status = 'completed' WHERE id = %s", (job_id,))
            
    db.commit()
    return {"message": "ok", "updated_count": len(data.updates)}


# ========== 任务导出 ==========

GROUP_SIZE = 50000

@router.get("/export/groups")
def get_export_groups(job_id: int, db=Depends(get_db_dependency)):
    """获取指定批次已完成任务的分组信息（每5万条一组，按完成时间正序）"""
    cursor = db.cursor()
    cursor.execute(
        "SELECT COUNT(*) as total FROM tasks WHERE status='completed' AND job_id = %s",
        (job_id,)
    )
    total = cursor.fetchone()["total"]

    if total == 0:
        return {"total": 0, "groups": []}

    groups = []
    num_groups = (total + GROUP_SIZE - 1) // GROUP_SIZE

    for i in range(num_groups):
        offset = i * GROUP_SIZE
        limit = min(GROUP_SIZE, total - offset)
        cursor.execute(
            "SELECT completed_at FROM ("
            "  SELECT completed_at FROM tasks WHERE status='completed' AND job_id = %s "
            "  ORDER BY completed_at ASC LIMIT %s OFFSET %s"
            ") t ORDER BY completed_at DESC LIMIT 1",
            (job_id, limit, offset)
        )
        oldest = cursor.fetchone()
        cursor.execute(
            "SELECT completed_at FROM tasks WHERE status='completed' AND job_id = %s "
            "ORDER BY completed_at ASC LIMIT 1 OFFSET %s",
            (job_id, offset)
        )
        newest = cursor.fetchone()

        groups.append({
            "group_index": i,
            "offset": offset,
            "count": limit,
            "label": f"第 {i+1} 组 ({offset+1}-{offset+limit})",
            "time_range_start": str(oldest["completed_at"]) if oldest else "",
            "time_range_end": str(newest["completed_at"]) if newest else "",
        })

    return {"total": total, "group_size": GROUP_SIZE, "groups": groups}


@router.get("/export/download")
def export_download(job_id: int, group: int = 0, db=Depends(get_db_dependency)):
    """导出指定批次、指定分组的已完成任务为 CSV"""
    import csv
    import io

    cursor = db.cursor()
    offset = group * GROUP_SIZE

    cursor.execute(
        "SELECT id, track_id, title, artist, album, isrc, actual_quality, codec, "
        "file_size, s3_key, completed_at "
        "FROM tasks WHERE status='completed' AND job_id = %s "
        "ORDER BY completed_at ASC "
        "LIMIT %s OFFSET %s",
        (job_id, GROUP_SIZE, offset)
    )
    rows = cursor.fetchall()

    def generate():
        output = io.StringIO()
        output.write('\ufeff')  # BOM for Excel
        writer = csv.writer(output)
        writer.writerow(["ID", "Track ID", "标题", "艺术家", "专辑", "ISRC",
                         "音质", "编码", "文件大小(MB)", "S3路径", "下载链接", "完成时间"])
        yield output.getvalue()
        output.seek(0)
        output.truncate(0)

        for row in rows:
            s3_key = row.get("s3_key") or ""
            download_url = f"https://xiyaa.aybksd136.com/{s3_key}" if s3_key else ""
            file_size_mb = round((row.get("file_size") or 0) / 1024 / 1024, 2)
            writer.writerow([
                row.get("id", ""),
                row.get("track_id", ""),
                row.get("title", ""),
                row.get("artist", ""),
                row.get("album", ""),
                row.get("isrc") or "",
                row.get("actual_quality") or "",
                row.get("codec") or "",
                file_size_mb,
                s3_key,
                download_url,
                str(row.get("completed_at") or ""),
            ])
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)

    cursor.execute("SELECT name FROM jobs WHERE id = %s", (job_id,))
    job = cursor.fetchone()
    job_name = job["name"] if job else f"job_{job_id}"
    filename = f"{job_name}_group_{group+1}.csv"
    return StreamingResponse(
        generate(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

