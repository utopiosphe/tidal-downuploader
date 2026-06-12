"""任务批次管理 + JSON 导入 API"""
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from database import get_db_dependency
import json
import csv
import io

router = APIRouter(prefix="/api/jobs", tags=["Jobs"])


@router.get("")
def list_jobs(db=Depends(get_db_dependency)):
    """获取所有任务批次"""
    cursor = db.cursor()
    cursor.execute(
        "SELECT j.*, "
        "(SELECT COUNT(*) FROM tasks t WHERE t.job_id = j.id AND t.status = 'pending') as pending_count, "
        "(SELECT COUNT(*) FROM tasks t WHERE t.job_id = j.id AND t.status IN ('assigned','downloading','uploading')) as active_count "
        "FROM jobs j ORDER BY j.id DESC"
    )
    return cursor.fetchall()


@router.post("/import")
async def import_json(
    file: UploadFile = File(...),
    name: str = Form(""),
    quality: str = Form("LOSSLESS"),
    db=Depends(get_db_dependency)
):
    """导入 JSON 曲目列表创建任务批次"""
    content = await file.read()
    try:
        tracks = json.loads(content.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise HTTPException(status_code=400, detail=f"JSON 解析失败: {str(e)}")

    if not isinstance(tracks, list):
        raise HTTPException(status_code=400, detail="JSON 必须是数组格式")

    if not name:
        name = file.filename or "unnamed"

    cursor = db.cursor()

    # 创建 Job
    cursor.execute(
        "INSERT INTO jobs (name, source_file, total_tracks, target_quality, status) "
        "VALUES (%s, %s, %s, %s, 'running')",
        (name, file.filename, len(tracks), quality)
    )
    job_id = cursor.lastrowid

    # 批量创建 Tasks
    task_values = []
    for track in tracks:
        artist = track.get("artist", {})
        if isinstance(artist, dict):
            artist_name = artist.get("name", "Unknown")
        else:
            artist_name = str(artist)

        album = track.get("album", {})
        if isinstance(album, dict):
            album_title = album.get("title", "Unknown")
            album_id = album.get("id", 0)
        else:
            album_title = str(album)
            album_id = 0

        task_values.append((
            job_id,
            track.get("id", 0),
            track.get("title", "Unknown"),
            artist_name,
            album_title,
            album_id,
            track.get("trackNumber", 0),
            track.get("duration", 0),
            track.get("audioQuality", quality),
            track.get("isrc", ""),
        ))

    if task_values:
        cursor.executemany(
            "INSERT INTO tasks "
            "(job_id, track_id, title, artist, album, album_id, "
            "track_number, duration, audio_quality, isrc) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            task_values
        )

    db.commit()

    return {
        "message": f"导入成功: {len(tracks)} 首曲目",
        "job_id": job_id,
        "total_tracks": len(tracks)
    }


@router.post("/import-csv")
async def import_csv(
    file: UploadFile = File(...),
    name: str = Form(""),
    quality: str = Form("LOSSLESS"),
    db=Depends(get_db_dependency)
):
    """流式导入超大 CSV 列表创建任务批次"""
    if not name:
        name = file.filename or "unnamed_csv"

    cursor = db.cursor()
    # 先创建 Job 占位，total_tracks 稍后更新
    cursor.execute(
        "INSERT INTO jobs (name, source_file, total_tracks, target_quality, status) "
        "VALUES (%s, %s, %s, %s, 'running')",
        (name, file.filename, 0, quality)
    )
    job_id = cursor.lastrowid
    db.commit()

    total_count = 0
    batch_size = 5000
    task_values = []
    
    try:
        # 使用 TextIOWrapper 包装 spooled temporary file 进行流式读取
        text_file = io.TextIOWrapper(file.file, encoding='utf-8', errors='ignore')
        reader = csv.DictReader(text_file)
        
        for row in reader:
            track_id = row.get("id", "0").strip()
            if not track_id or not track_id.isdigit():
                continue

            track_id = int(track_id)
            isrc = row.get("isrc", "").strip()

            task_values.append((
                job_id,
                track_id,
                "Unknown",  # title
                "Unknown",  # artist
                "Unknown",  # album
                0,          # album_id
                0,          # track_number
                0,          # duration
                quality,
                isrc
            ))
            
            total_count += 1
            
            if len(task_values) >= batch_size:
                cursor.executemany(
                    "INSERT INTO tasks "
                    "(job_id, track_id, title, artist, album, album_id, "
                    "track_number, duration, audio_quality, isrc) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    task_values
                )
                db.commit()
                task_values = []
                
        # 插入剩余的
        if task_values:
            cursor.executemany(
                "INSERT INTO tasks "
                "(job_id, track_id, title, artist, album, album_id, "
                "track_number, duration, audio_quality, isrc) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                task_values
            )
            db.commit()
            
        # 更新 Job 的 total_tracks
        cursor.execute("UPDATE jobs SET total_tracks = %s WHERE id = %s", (total_count, job_id))
        db.commit()
        
    except Exception as e:
        cursor.execute("UPDATE jobs SET status = 'paused' WHERE id = %s", (job_id,))
        db.commit()
        raise HTTPException(status_code=400, detail=f"CSV 解析失败: {str(e)}")

    return {
        "message": f"流式导入成功: 写入 {total_count} 首曲目",
        "job_id": job_id,
        "total_tracks": total_count
    }


@router.get("/{job_id}")
def get_job(job_id: int, db=Depends(get_db_dependency)):
    """获取批次详情"""
    cursor = db.cursor()
    cursor.execute("SELECT * FROM jobs WHERE id = %s", (job_id,))
    job = cursor.fetchone()
    if not job:
        raise HTTPException(status_code=404, detail="批次不存在")

    # 获取状态统计
    cursor.execute(
        "SELECT status, COUNT(*) as count FROM tasks WHERE job_id = %s GROUP BY status",
        (job_id,)
    )
    status_counts = {row["status"]: row["count"] for row in cursor.fetchall()}
    job["status_counts"] = status_counts
    return job


@router.get("/{job_id}/tasks")
def get_job_tasks(
    job_id: int,
    status: str = "",
    page: int = 1,
    page_size: int = 50,
    db=Depends(get_db_dependency)
):
    """获取批次下的任务列表"""
    cursor = db.cursor()
    offset = (page - 1) * page_size

    if status:
        cursor.execute(
            "SELECT * FROM tasks WHERE job_id = %s AND status = %s "
            "ORDER BY id LIMIT %s OFFSET %s",
            (job_id, status, page_size, offset)
        )
    else:
        cursor.execute(
            "SELECT * FROM tasks WHERE job_id = %s ORDER BY id LIMIT %s OFFSET %s",
            (job_id, page_size, offset)
        )
    tasks = cursor.fetchall()

    # 总数
    if status:
        cursor.execute(
            "SELECT COUNT(*) as total FROM tasks WHERE job_id = %s AND status = %s",
            (job_id, status)
        )
    else:
        cursor.execute("SELECT COUNT(*) as total FROM tasks WHERE job_id = %s", (job_id,))
    total = cursor.fetchone()["total"]

    return {"tasks": tasks, "total": total, "page": page, "page_size": page_size}


@router.post("/{job_id}/retry-failed")
def retry_failed_tasks(job_id: int, db=Depends(get_db_dependency)):
    """重试批次中所有失败的任务"""
    cursor = db.cursor()
    cursor.execute(
        "UPDATE tasks SET status = 'pending', retry_count = 0, "
        "error_code = NULL, error_message = NULL, "
        "assigned_worker_id = NULL, assigned_account_id = NULL "
        "WHERE job_id = %s AND status IN ('failed', 'dead')",
        (job_id,)
    )
    affected = cursor.rowcount
    db.commit()
    return {"message": f"已重置 {affected} 个失败任务"}


@router.post("/{job_id}/pause")
def pause_job(job_id: int, db=Depends(get_db_dependency)):
    """暂停批次"""
    cursor = db.cursor()
    cursor.execute("UPDATE jobs SET status = 'paused' WHERE id = %s", (job_id,))
    db.commit()
    return {"message": "批次已暂停"}


@router.post("/{job_id}/resume")
def resume_job(job_id: int, db=Depends(get_db_dependency)):
    """恢复批次"""
    cursor = db.cursor()
    cursor.execute("UPDATE jobs SET status = 'running' WHERE id = %s", (job_id,))
    db.commit()
    return {"message": "批次已恢复"}
