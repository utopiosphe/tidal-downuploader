"""Dashboard 统计 API"""
from fastapi import APIRouter, Depends
from database import get_db_dependency

router = APIRouter(prefix="/api/dashboard", tags=["Dashboard"])


@router.get("")
def get_dashboard(db=Depends(get_db_dependency)):
    """总览统计"""
    cursor = db.cursor()

    # 从 jobs 表一次性获取所有汇总数据（毫秒级，无需扫 tasks 表）
    cursor.execute(
        "SELECT COUNT(*) as job_count, "
        "COALESCE(SUM(total_tracks), 0) as total, "
        "COALESCE(SUM(completed), 0) as completed, "
        "COALESCE(SUM(failed), 0) as failed "
        "FROM jobs"
    )
    jobs_summary = cursor.fetchone()
    total_tasks = int(jobs_summary["total"])
    completed = int(jobs_summary["completed"])
    failed = int(jobs_summary["failed"])
    total_jobs = int(jobs_summary["job_count"])

    # 活跃任务数（assigned/downloading/uploading 量很小，走 idx_assigned_worker 索引）
    cursor.execute(
        "SELECT COUNT(*) as cnt FROM tasks "
        "WHERE status IN ('assigned', 'downloading', 'uploading')"
    )
    active = cursor.fetchone()["cnt"]

    pending = max(0, total_tasks - completed - active - failed)

    # 总下载大小（从 export_groups 表汇总，只有几十行，毫秒级）
    cursor.execute("SELECT COALESCE(SUM(total_size), 0) as total_bytes FROM export_groups")
    total_bytes = cursor.fetchone()["total_bytes"]

    # Worker 统计
    cursor.execute(
        "SELECT COUNT(*) as total, "
        "SUM(CASE WHEN last_heartbeat > DATE_SUB(NOW(), INTERVAL 60 SECOND) THEN 1 ELSE 0 END) as online "
        "FROM workers"
    )
    worker_stats = cursor.fetchone()

    # 账号统计
    cursor.execute(
        "SELECT status, COUNT(*) as count FROM tidal_accounts GROUP BY status"
    )
    account_stats = {row["status"]: row["count"] for row in cursor.fetchall()}

    # 最近完成的任务（用 id DESC 走主键索引，避免 completed_at filesort）
    cursor.execute(
        "SELECT id, title, artist, album, file_size, actual_quality, completed_at "
        "FROM tasks WHERE status = 'completed' ORDER BY id DESC LIMIT 10"
    )
    recent_completed = cursor.fetchall()

    # 最近失败的任务（用 id DESC 走主键索引）
    cursor.execute(
        "SELECT id, title, artist, error_code, error_message, updated_at "
        "FROM tasks WHERE status IN ('failed', 'dead') ORDER BY id DESC LIMIT 10"
    )
    recent_failed = cursor.fetchall()

    return {
        "tasks": {
            "total": total_tasks,
            "pending": pending,
            "active": active,
            "completed": completed,
            "failed": failed,
            "by_status": {"completed": completed, "failed": failed, "active": active, "pending": pending},
        },
        "total_bytes": total_bytes,
        "total_bytes_mb": round(total_bytes / (1024 * 1024), 1) if total_bytes else 0,
        "total_jobs": total_jobs,
        "workers": {
            "total": worker_stats["total"] or 0,
            "online": worker_stats["online"] or 0,
        },
        "accounts": account_stats,
        "recent_completed": recent_completed,
        "recent_failed": recent_failed,
    }
