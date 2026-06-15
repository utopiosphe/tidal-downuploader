"""任务分配 + 状态上报 API"""
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
import time as _time
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
    storage_id: Optional[str] = None

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
    storage_id: Optional[str] = None

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


# 超时重置定时器（60 秒执行一次，避免每次 fetch 都做重量级 UPDATE）
_last_timeout_check = 0
_TIMEOUT_CHECK_INTERVAL = 60


@router.post("/fetch")
def fetch_tasks(data: TaskFetch, db=Depends(get_db_dependency)):
    """Worker 拉取待下载任务"""
    global _last_timeout_check
    cursor = db.cursor()
    download_config = get_config_section(db, "download")

    # 超时回收：60 秒检查一次（避免 15 个 Worker 并发锁竞争）
    now = _time.time()
    timeout = download_config.get("task_timeout", 300)
    if now - _last_timeout_check > _TIMEOUT_CHECK_INTERVAL:
        _last_timeout_check = now
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

    # 原子抢占：UPDATE LIMIT 直接标记任务为 assigned（避免 FOR UPDATE SKIP LOCKED 锁竞争）
    cursor.execute(
        f"UPDATE tasks FORCE INDEX(idx_job_status_completed_size) "
        f"SET status = 'assigned', assigned_worker_id = %s, "
        f"assigned_account_id = %s, updated_at = NOW() "
        f"WHERE job_id IN ({jobs_in}) AND status = 'pending' "
        f"LIMIT %s",
        [data.worker_id, account_id, data.batch_size]
    )
    affected = cursor.rowcount
    db.commit()

    if affected == 0:
        return {"tasks": [], "account": None}

    # 取回刚分配的任务（用 idx_assigned_worker 索引，只扫几十行）
    cursor.execute(
        "SELECT * FROM tasks WHERE assigned_worker_id = %s AND status = 'assigned' "
        "ORDER BY updated_at DESC LIMIT %s",
        (data.worker_id, data.batch_size)
    )
    tasks = cursor.fetchall()

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

    # 检查 job 是否全部完成（用 jobs 表字段推算，避免 COUNT 子查询扫百万行）
    cursor.execute(
        "SELECT id, total_tracks, completed, failed FROM jobs "
        "WHERE id = (SELECT job_id FROM tasks WHERE id = %s)",
        (task_id,)
    )
    job = cursor.fetchone()
    if job and (job["completed"] + job["failed"]) >= job["total_tracks"]:
        cursor.execute("UPDATE jobs SET status = 'completed' WHERE id = %s", (job["id"],))
        db.commit()

    return {"message": "ok"}


@router.post("/batch-status")
def update_task_status_batch(data: TaskBatchUpdate, db=Depends(get_db_dependency)):
    """Worker 批量上报任务状态（含 Deadlock 自动重试）"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            cursor = db.cursor()
            # 按 job_id 汇总 completed/dead 增量
            job_completed = {}  # job_id -> completed_count
            job_failed = {}    # job_id -> dead_count

            for item in data.updates:
                task_id = item.task_id
                if item.status == "completed":
                    cursor.execute(
                        "UPDATE tasks SET status = 'completed', file_size = %s, "
                        "actual_quality = %s, codec = %s, s3_key = %s, storage_id = %s, error_message = %s, "
                        "assigned_account_id = COALESCE(%s, assigned_account_id), "
                        "completed_at = NOW(), updated_at = NOW() WHERE id = %s",
                        (item.file_size, item.actual_quality, item.codec, item.s3_key, item.storage_id, item.error_message, item.account_id, task_id)
                    )
                    if item.account_id:
                        cursor.execute(
                            "UPDATE tidal_accounts SET total_downloads = total_downloads + 1, "
                            "rate_limit_count = 0, last_used_at = NOW() "
                            "WHERE id = %s",
                            (item.account_id,)
                        )
                    # 记录 completed 增量
                    cursor.execute("SELECT job_id FROM tasks WHERE id = %s", (task_id,))
                    res = cursor.fetchone()
                    if res:
                        job_completed[res["job_id"]] = job_completed.get(res["job_id"], 0) + 1

                elif item.status == "failed":
                    cursor.execute("SELECT retry_count, max_retries, job_id FROM tasks WHERE id = %s", (task_id,))
                    task = cursor.fetchone()
                    if not task:
                        continue
                    if task["retry_count"] >= task["max_retries"]:
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
                    elif new_status == "dead":
                        # 记录 dead 增量
                        job_failed[task["job_id"]] = job_failed.get(task["job_id"], 0) + 1
                else:
                    cursor.execute(
                        "UPDATE tasks SET status = %s, updated_at = NOW() WHERE id = %s",
                        (item.status, task_id)
                    )

            # 批量更新 Job 计数（增量）
            all_job_ids = set(list(job_completed.keys()) + list(job_failed.keys()))
            for job_id in all_job_ids:
                c = job_completed.get(job_id, 0)
                f = job_failed.get(job_id, 0)
                if c > 0 and f > 0:
                    cursor.execute(
                        "UPDATE jobs SET completed = completed + %s, failed = failed + %s WHERE id = %s",
                        (c, f, job_id)
                    )
                elif c > 0:
                    cursor.execute("UPDATE jobs SET completed = completed + %s WHERE id = %s", (c, job_id))
                elif f > 0:
                    cursor.execute("UPDATE jobs SET failed = failed + %s WHERE id = %s", (f, job_id))

                # 检查 Job 是否全部完成
                cursor.execute(
                    "SELECT id, total_tracks, completed, failed FROM jobs WHERE id = %s",
                    (job_id,)
                )
                job = cursor.fetchone()
                if job and (job["completed"] + job["failed"]) >= job["total_tracks"]:
                    cursor.execute("UPDATE jobs SET status = 'completed' WHERE id = %s", (job_id,))

            db.commit()
            return {"message": "ok", "updated_count": len(data.updates)}

        except Exception as e:
            db.rollback()
            if "Deadlock" in str(e) and attempt < max_retries - 1:
                _time.sleep(0.1 * (attempt + 1))  # 退避重试
                continue
            raise


# ========== 任务导出 ==========

GROUP_SIZE = 50000

_export_cache = {}  # key: job_id -> {"ts": float, "data": dict}
_EXPORT_TTL = 120  # 缓存 2 分钟


@router.get("/export/groups")
def get_export_groups(job_id: int, db=Depends(get_db_dependency)):
    """获取指定批次已完成任务的分组信息（每5万条一组，按完成时间正序）

    优化策略：
    1. 单次顺序查询（利用 idx_job_status_completed 索引排序，无 filesort）
    2. Python 侧逐行分桶（无窗口函数开销）
    3. 120 秒进程内缓存
    """
    # 缓存检查
    cached = _export_cache.get(job_id)
    if cached and _time.time() - cached["ts"] < _EXPORT_TTL:
        return cached["data"]

    cursor = db.cursor()

    # 单次查询：利用索引排序，避免 ROW_NUMBER 窗口函数
    cursor.execute(
        "SELECT file_size, completed_at FROM tasks "
        "WHERE job_id = %s AND status = 'completed' "
        "ORDER BY completed_at ASC",
        (job_id,)
    )

    groups = []
    total = 0
    total_size_all = 0
    grp_count = 0
    grp_size = 0
    grp_start = None
    grp_end = None
    grp_idx = 0

    for row in cursor:
        total += 1
        fs = row["file_size"] or 0
        total_size_all += fs
        grp_count += 1
        grp_size += fs
        if grp_start is None:
            grp_start = row["completed_at"]
        grp_end = row["completed_at"]

        if grp_count >= GROUP_SIZE:
            offset = grp_idx * GROUP_SIZE
            groups.append({
                "group_index": grp_idx,
                "offset": offset,
                "count": grp_count,
                "total_size": grp_size,
                "label": f"第 {grp_idx+1} 组 ({offset+1}-{offset+grp_count})",
                "time_range_start": str(grp_start) if grp_start else "",
                "time_range_end": str(grp_end) if grp_end else "",
            })
            grp_idx += 1
            grp_count = 0
            grp_size = 0
            grp_start = None

    # 最后一组不满 GROUP_SIZE
    if grp_count > 0:
        offset = grp_idx * GROUP_SIZE
        groups.append({
            "group_index": grp_idx,
            "offset": offset,
            "count": grp_count,
            "total_size": grp_size,
            "label": f"第 {grp_idx+1} 组 ({offset+1}-{offset+grp_count})",
            "time_range_start": str(grp_start) if grp_start else "",
            "time_range_end": str(grp_end) if grp_end else "",
        })

    result = {"total": total, "total_size": total_size_all, "group_size": GROUP_SIZE, "groups": groups}

    # 写入缓存
    _export_cache[job_id] = {"ts": _time.time(), "data": result}

    return result


@router.get("/export/download")
def export_download(job_id: int, group: int = 0, db=Depends(get_db_dependency)):
    """导出指定批次、指定分组的已完成任务为 CSV"""
    import csv
    import io

    cursor = db.cursor()
    offset = group * GROUP_SIZE

    cursor.execute(
        "SELECT id, track_id, title, artist, album, isrc, actual_quality, codec, "
        "file_size, s3_key, storage_id, completed_at "
        "FROM tasks WHERE status='completed' AND job_id = %s "
        "ORDER BY completed_at ASC "
        "LIMIT %s OFFSET %s",
        (job_id, GROUP_SIZE, offset)
    )
    rows = cursor.fetchall()

    # 构建 storage_id → download_domain 映射
    from config import get_config
    cfg = get_config(db)
    s3_list = cfg.get("s3", [])
    domain_map = {}
    default_domain = ""
    for s in (s3_list if isinstance(s3_list, list) else [s3_list]):
        sid = s.get("id", "default")
        domain_map[sid] = s.get("download_domain", "").rstrip("/")
        if not default_domain:
            default_domain = domain_map[sid]

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
            sid = row.get("storage_id") or ""
            domain = domain_map.get(sid, default_domain)
            download_url = f"{domain}/{s3_key}" if s3_key and domain else ""
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


# ========== 趋势图 ==========

_trend_cache = {}  # key: (job_id, hours) -> {"ts": float, "data": list}
_TREND_TTL = 60  # 缓存 60 秒


@router.get("/trend")
def get_trend(job_id: int, hours: int = 24, db=Depends(get_db_dependency)):
    """获取指定批次的完成/失败趋势数据（按时间分桶聚合）

    自动选择分桶粒度：
    - ≤6h  → 5分钟
    - ≤24h → 15分钟
    - ≤72h → 1小时
    - >72h → 6小时
    """
    # 限制范围
    hours = max(1, min(hours, 720))  # 1h ~ 30天

    # 缓存检查
    cache_key = (job_id, hours)
    cached = _trend_cache.get(cache_key)
    if cached and _time.time() - cached["ts"] < _TREND_TTL:
        return {"buckets": cached["data"], "cached": True}

    # 自动分桶粒度
    if hours <= 6:
        bucket_minutes = 5
    elif hours <= 24:
        bucket_minutes = 15
    elif hours <= 72:
        bucket_minutes = 60
    else:
        bucket_minutes = 360

    # 对齐到桶边界的 SQL 表达式 (MariaDB, %% 转义给 PyMySQL)
    if bucket_minutes < 60:
        bucket_expr = (
            f"CONCAT(DATE_FORMAT(completed_at, '%%Y-%%m-%%d %%H:'), "
            f"LPAD(FLOOR(MINUTE(completed_at) / {bucket_minutes}) * {bucket_minutes}, 2, '0'))"
        )
    else:
        hours_per_bucket = bucket_minutes // 60
        bucket_expr = (
            f"CONCAT(DATE_FORMAT(completed_at, '%%Y-%%m-%%d '), "
            f"LPAD(FLOOR(HOUR(completed_at) / {hours_per_bucket}) * {hours_per_bucket}, 2, '0'), ':00')"
        )

    cursor = db.cursor()

    # 查询 completed（利用 idx_job_status_completed 索引）
    cursor.execute(
        f"SELECT {bucket_expr} as bucket, "
        f"COUNT(*) as cnt, COALESCE(SUM(file_size), 0) as size "
        f"FROM tasks WHERE job_id = %s AND status = 'completed' "
        f"AND completed_at >= NOW() - INTERVAL %s HOUR "
        f"GROUP BY bucket ORDER BY bucket",
        (job_id, hours)
    )
    completed_rows = {r["bucket"]: r for r in cursor.fetchall()}

    # 查询 failed + dead
    cursor.execute(
        f"SELECT {bucket_expr} as bucket, "
        f"COUNT(*) as cnt "
        f"FROM tasks WHERE job_id = %s AND status IN ('failed', 'dead') "
        f"AND completed_at >= NOW() - INTERVAL %s HOUR "
        f"GROUP BY bucket ORDER BY bucket",
        (job_id, hours)
    )
    failed_rows = {r["bucket"]: r for r in cursor.fetchall()}

    # 查询累计基数（时间范围之前的数据）
    cursor.execute(
        "SELECT COUNT(*) as cnt, COALESCE(SUM(file_size), 0) as size "
        "FROM tasks WHERE job_id = %s AND status = 'completed' "
        "AND completed_at < NOW() - INTERVAL %s HOUR",
        (job_id, hours)
    )
    base = cursor.fetchone()
    cum_completed = base["cnt"] if base else 0
    cum_size = base["size"] if base else 0

    cursor.execute(
        "SELECT COUNT(*) as cnt "
        "FROM tasks WHERE job_id = %s AND status IN ('failed', 'dead') "
        "AND completed_at < NOW() - INTERVAL %s HOUR",
        (job_id, hours)
    )
    base_f = cursor.fetchone()
    cum_failed = base_f["cnt"] if base_f else 0

    # 合并所有桶
    all_buckets = sorted(set(list(completed_rows.keys()) + list(failed_rows.keys())))

    result = []
    for b in all_buckets:
        c = completed_rows.get(b, {})
        f = failed_rows.get(b, {})
        c_cnt = c.get("cnt", 0)
        c_size = c.get("size", 0)
        f_cnt = f.get("cnt", 0)

        cum_completed += c_cnt
        cum_size += c_size
        cum_failed += f_cnt

        result.append({
            "time": b,
            "completed": c_cnt,
            "failed": f_cnt,
            "size": c_size,
            "cum_completed": cum_completed,
            "cum_size": cum_size,
            "cum_failed": cum_failed,
        })

    # 写入缓存
    _trend_cache[cache_key] = {"ts": _time.time(), "data": result}

    return {"buckets": result, "cached": False}
