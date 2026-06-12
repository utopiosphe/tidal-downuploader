"""Dashboard 统计 API"""
from fastapi import APIRouter, Depends
from database import get_db_dependency

router = APIRouter(prefix="/api/dashboard", tags=["Dashboard"])


@router.get("")
def get_dashboard(db=Depends(get_db_dependency)):
    """总览统计"""
    cursor = db.cursor()

    # 从 jobs 表快速获取总任务数
    cursor.execute("SELECT COALESCE(SUM(total_tracks), 0) as total FROM jobs")
    total_tasks = int(cursor.fetchone()["total"])

    # 仅统计非 pending 的状态（因为 pending 数据量极大，会导致千万级全表扫描）
    cursor.execute(
        "SELECT status, COUNT(*) as count FROM tasks "
        "WHERE status IN ('completed', 'failed', 'dead', 'assigned', 'downloading', 'uploading') "
        "GROUP BY status"
    )
    task_stats = {row["status"]: row["count"] for row in cursor.fetchall()}

    completed = task_stats.get("completed", 0)
    active = task_stats.get("assigned", 0) + task_stats.get("downloading", 0) + task_stats.get("uploading", 0)
    failed = task_stats.get("failed", 0) + task_stats.get("dead", 0)
    
    # 用减法算出 pending 数量，避免扫描千万级数据
    pending = max(0, total_tasks - completed - active - failed)

    # 总下载大小
    cursor.execute("SELECT COALESCE(SUM(file_size), 0) as total_bytes FROM tasks WHERE status = 'completed'")
    total_bytes = cursor.fetchone()["total_bytes"]

    # Job 统计
    cursor.execute("SELECT COUNT(*) as count FROM jobs")
    total_jobs = cursor.fetchone()["count"]

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

    # 最近完成的任务
    cursor.execute(
        "SELECT id, title, artist, album, file_size, actual_quality, completed_at "
        "FROM tasks WHERE status = 'completed' ORDER BY completed_at DESC LIMIT 10"
    )
    recent_completed = cursor.fetchall()

    # 最近失败的任务
    cursor.execute(
        "SELECT id, title, artist, error_code, error_message, updated_at "
        "FROM tasks WHERE status IN ('failed', 'dead') ORDER BY updated_at DESC LIMIT 10"
    )
    recent_failed = cursor.fetchall()

    return {
        "tasks": {
            "total": total_tasks,
            "pending": pending,
            "active": active,
            "completed": completed,
            "failed": failed,
            "by_status": task_stats,
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
