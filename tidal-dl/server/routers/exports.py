"""导出分组管理 + S3 清理 API"""
import threading
import time as _time
import logging
import csv
import io
import os
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from database import get_db_dependency, get_connection

logger = logging.getLogger("server.exports")

router = APIRouter(prefix="/api/exports", tags=["Exports"])

GROUP_SIZE = 50000
EXPORT_DIR = "/opt/tidal-dl/exports"


# ========== 分组查询 ==========

@router.get("/groups")
def get_export_groups(job_id: int, db=Depends(get_db_dependency)):
    """获取指定批次的导出分组列表（从 export_groups 表读取）"""
    cursor = db.cursor()
    cursor.execute(
        "SELECT * FROM export_groups WHERE job_id = %s ORDER BY group_index",
        (job_id,)
    )
    groups = cursor.fetchall()

    total = sum(g["task_count"] for g in groups)
    total_size = sum(g["total_size"] for g in groups)

    # 序列化 datetime
    for g in groups:
        for k in ("time_range_start", "time_range_end", "s3_cleaned_at", "created_at", "updated_at"):
            if g.get(k):
                g[k] = str(g[k])

    return {
        "total": total,
        "total_size": total_size,
        "group_size": GROUP_SIZE,
        "groups": groups
    }


# ========== 手动刷新分组 ==========

@router.post("/groups/refresh")
def refresh_export_groups(job_id: int, db=Depends(get_db_dependency)):
    """手动触发分组刷新：扫描 completed 任务，维护 export_groups 表"""
    count = _build_groups_for_job(job_id, db)
    return {"message": f"分组刷新完成", "groups_count": count}


def _build_groups_for_job(job_id: int, db) -> int:
    """为指定 job 构建/更新 export_groups（核心逻辑）"""
    cursor = db.cursor()

    # 查询已完成任务总数
    cursor.execute(
        "SELECT COUNT(*) as cnt FROM tasks WHERE job_id = %s AND status = 'completed'",
        (job_id,)
    )
    completed_count = cursor.fetchone()["cnt"]
    if completed_count == 0:
        return 0

    # 计算应有的分组数
    total_groups = (completed_count + GROUP_SIZE - 1) // GROUP_SIZE

    # 查询已有分组
    cursor.execute(
        "SELECT group_index, task_count FROM export_groups WHERE job_id = %s ORDER BY group_index",
        (job_id,)
    )
    existing = {row["group_index"]: row["task_count"] for row in cursor.fetchall()}

    updated = 0
    for grp_idx in range(total_groups):
        offset = grp_idx * GROUP_SIZE
        is_last = (grp_idx == total_groups - 1)
        expected_count = completed_count - offset if is_last else GROUP_SIZE

        # 如果已存在且是满组，跳过（不需要更新）
        if grp_idx in existing and existing[grp_idx] == GROUP_SIZE:
            continue

        # 查询该分组的统计信息
        cursor.execute(
            "SELECT COUNT(*) as cnt, COALESCE(SUM(file_size), 0) as total_size, "
            "MIN(completed_at) as time_start, MAX(completed_at) as time_end "
            "FROM tasks WHERE job_id = %s AND status = 'completed' "
            "ORDER BY completed_at ASC LIMIT %s OFFSET %s",
            (job_id, GROUP_SIZE, offset)
        )
        # 上面的 SQL 有 GROUP BY 问题，改用子查询
        cursor.execute(
            "SELECT COUNT(*) as cnt, COALESCE(SUM(file_size), 0) as total_size, "
            "MIN(completed_at) as time_start, MAX(completed_at) as time_end "
            "FROM (SELECT file_size, completed_at FROM tasks "
            "WHERE job_id = %s AND status = 'completed' "
            "ORDER BY completed_at ASC LIMIT %s OFFSET %s) sub",
            (job_id, GROUP_SIZE, offset)
        )
        stats = cursor.fetchone()

        # UPSERT
        cursor.execute(
            "INSERT INTO export_groups (job_id, group_index, task_count, total_size, "
            "time_range_start, time_range_end) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE task_count = VALUES(task_count), "
            "total_size = VALUES(total_size), "
            "time_range_start = VALUES(time_range_start), "
            "time_range_end = VALUES(time_range_end)",
            (job_id, grp_idx, stats["cnt"], stats["total_size"],
             stats["time_start"], stats["time_end"])
        )
        updated += 1

    db.commit()
    logger.info(f"Job {job_id}: 分组刷新完成, 共 {total_groups} 组, 更新 {updated} 组")
    return total_groups


# ========== CSV 下载 ==========

@router.get("/groups/download")
def download_group_csv(job_id: int, group: int = 0, db=Depends(get_db_dependency)):
    """导出指定分组的已完成任务为 CSV"""
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
        headers={"Content-Disposition": f"attachment; filename*=UTF-8''{filename}"}
    )


# ========== S3 清理 ==========

@router.post("/groups/{group_id}/cleanup")
def cleanup_group_s3(group_id: int, db=Depends(get_db_dependency)):
    """启动指定分组的 S3 资源清理（后台异步执行）"""
    cursor = db.cursor()
    cursor.execute("SELECT * FROM export_groups WHERE id = %s", (group_id,))
    group = cursor.fetchone()
    if not group:
        raise HTTPException(status_code=404, detail="分组不存在")

    if group["s3_cleanup_status"] == "running":
        raise HTTPException(status_code=400, detail="该分组正在清理中")

    # 标记为 pending
    cursor.execute(
        "UPDATE export_groups SET s3_cleanup_status = 'pending', s3_cleaned_count = 0 WHERE id = %s",
        (group_id,)
    )
    db.commit()

    # 获取 S3 配置列表
    import json
    cursor.execute("SELECT value FROM config WHERE `key` = 's3'")
    row = cursor.fetchone()
    if not row:
        raise HTTPException(status_code=500, detail="S3 配置未找到")
    s3_raw = json.loads(row["value"])
    s3_list = s3_raw if isinstance(s3_raw, list) else [s3_raw]

    # 后台线程执行清理
    t = threading.Thread(
        target=_do_cleanup,
        args=(group_id, group["job_id"], group["group_index"], s3_list),
        daemon=True
    )
    t.start()

    return {"message": "清理任务已启动", "group_id": group_id}


def _do_cleanup(group_id: int, job_id: int, group_index: int, s3_list: list):
    """后台线程：批量删除指定分组的 S3 资源（支持多存储）"""
    import boto3
    from botocore.config import Config as BotoConfig

    db = get_connection()
    try:
        cursor = db.cursor()

        # 更新状态为 running
        cursor.execute(
            "UPDATE export_groups SET s3_cleanup_status = 'running' WHERE id = %s",
            (group_id,)
        )
        db.commit()

        # 构建 storage_id → (client, bucket) 映射（跳过已禁用的存储，如已关闭的 GCS）
        clients = {}
        default_sid = None
        for cfg in s3_list:
            sid = cfg.get("id", "default")
            if not default_sid:
                default_sid = sid
            provider = cfg.get("provider", "aws")
            # 硬编码跳过 GCS：谷歌云已彻底关闭，清理时一律不再尝试删除其对象；
            # 同时跳过任何被禁用（enabled=False）的存储。GCS 的 key 会在下方 “sid not in clients” 分支被忽略
            if provider == "gcs" or not cfg.get("enabled", True):
                logger.info(f"Group {group_id}: 跳过存储 {sid}（provider={provider}, enabled={cfg.get('enabled', True)}）")
                continue
            boto_kwargs = {"max_pool_connections": 50}
            if provider == "gcs":
                boto_kwargs["signature_version"] = "s3v4"
                boto_kwargs["request_checksum_calculation"] = "when_required"
                boto_kwargs["response_checksum_validation"] = "when_required"

            client = boto3.client(
                "s3",
                endpoint_url=cfg.get("endpoint"),
                aws_access_key_id=cfg.get("access_key"),
                aws_secret_access_key=cfg.get("secret_key"),
                region_name=cfg.get("region", "us-east-1"),
                config=BotoConfig(**boto_kwargs),
            )
            clients[sid] = (client, cfg["bucket"])

        offset = group_index * GROUP_SIZE

        # 查询该组所有 s3_key + storage_id
        cursor.execute(
            "SELECT s3_key, storage_id FROM tasks "
            "WHERE job_id = %s AND status = 'completed' AND s3_key IS NOT NULL AND s3_key != '' "
            "ORDER BY completed_at ASC "
            "LIMIT %s OFFSET %s",
            (job_id, GROUP_SIZE, offset)
        )

        # 按 storage_id 分组
        batches = {}  # sid -> [keys]
        for row in cursor.fetchall():
            key = row["s3_key"]
            sid = row.get("storage_id") or default_sid
            if key:
                batches.setdefault(sid, []).append(key)

        cleaned = 0
        had_error = False
        for sid, keys in batches.items():
            if sid not in clients:
                # 存储未配置或已禁用（如已关闭的 GCS），跳过这部分对象，不计入失败
                logger.info(f"Group {group_id}: 跳过存储 {sid} 的 {len(keys)} 个对象（未启用/未配置）")
                continue
            client, bucket = clients[sid]
            try:
                # 分批删除（每次最多 1000）
                for i in range(0, len(keys), 1000):
                    batch = [{"Key": k} for k in keys[i:i+1000]]
                    client.delete_objects(
                        Bucket=bucket,
                        Delete={"Objects": batch, "Quiet": True}
                    )
                    cleaned += len(batch)
                    cursor.execute(
                        "UPDATE export_groups SET s3_cleaned_count = %s WHERE id = %s",
                        (cleaned, group_id)
                    )
                    db.commit()
                    logger.info(f"Group {group_id}: 已清理 {cleaned} 个文件 (存储: {sid})")
            except Exception as e:
                # 单个存储删除失败不再中断其它存储的清理
                had_error = True
                logger.error(f"Group {group_id}: 存储 {sid} 删除失败 - {e}")

        # 完成：所有启用的存储都成功才算 completed，否则 failed（已删数量保留）
        if had_error:
            cursor.execute(
                "UPDATE export_groups SET s3_cleanup_status = 'failed', "
                "s3_cleaned_count = %s WHERE id = %s",
                (cleaned, group_id)
            )
            db.commit()
            logger.warning(f"Group {group_id}: 部分存储删除失败，已删 {cleaned} 个文件")
        else:
            cursor.execute(
                "UPDATE export_groups SET s3_cleanup_status = 'completed', "
                "s3_cleaned_count = %s, s3_cleaned_at = NOW() WHERE id = %s",
                (cleaned, group_id)
            )
            db.commit()
            logger.info(f"Group {group_id}: S3 清理完成, 共删除 {cleaned} 个文件")

    except Exception as e:
        logger.error(f"Group {group_id}: S3 清理失败 - {e}")
        try:
            cursor.execute(
                "UPDATE export_groups SET s3_cleanup_status = 'failed' WHERE id = %s",
                (group_id,)
            )
            db.commit()
        except Exception:
            pass
    finally:
        db.close()


@router.get("/groups/{group_id}/cleanup-status")
def get_cleanup_status(group_id: int, db=Depends(get_db_dependency)):
    """查询清理进度"""
    cursor = db.cursor()
    cursor.execute(
        "SELECT id, s3_cleanup_status, s3_cleaned_count, task_count, s3_cleaned_at "
        "FROM export_groups WHERE id = %s",
        (group_id,)
    )
    group = cursor.fetchone()
    if not group:
        raise HTTPException(status_code=404, detail="分组不存在")

    if group.get("s3_cleaned_at"):
        group["s3_cleaned_at"] = str(group["s3_cleaned_at"])

    return group

# ========== 存储统计 ==========

@router.get("/storage-stats")
def get_storage_stats(db=Depends(get_db_dependency)):
    """统计每个存储的数据量（排除已清理的分组）"""
    cursor = db.cursor()
    cursor.execute(
        "SELECT COALESCE(t.storage_id, 'aws-eu') as sid, "
        "COUNT(*) as file_count, COALESCE(SUM(t.file_size), 0) as total_size "
        "FROM tasks t "
        "WHERE t.status = 'completed' AND t.s3_key IS NOT NULL AND t.s3_key != '' "
        "GROUP BY sid"
    )
    stats = {}
    for row in cursor.fetchall():
        stats[row["sid"]] = {
            "file_count": row["file_count"],
            "total_size": row["total_size"]
        }
    return stats


# ========== 后台定时分组构建 ==========

def start_group_builder():
    """启动后台定时分组构建线程"""
    def _loop():
        logger.info("📦 导出分组构建线程已启动（每 5 分钟）")
        while True:
            try:
                db = get_connection()
                cursor = db.cursor()
                cursor.execute("SELECT id FROM jobs WHERE status = 'running'")
                jobs = cursor.fetchall()
                for job in jobs:
                    try:
                        _build_groups_for_job(job["id"], db)
                    except Exception as e:
                        logger.error(f"分组构建失败 job_id={job['id']}: {e}")
                db.close()
            except Exception as e:
                logger.error(f"分组构建线程异常: {e}")
            _time.sleep(300)  # 5 分钟

    t = threading.Thread(target=_loop, daemon=True)
    t.start()
    return t
