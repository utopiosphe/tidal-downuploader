-- 冷热分离迁移:completed/dead 从 tasks 移到 tasks_archive,主表恒定小。
-- 在停机迁移窗口执行(见 docs/ARCHITECTURE.md 第八节)。

-- 1. 归档表:结构与 tasks 一致,但不需要抢占相关索引,增加导出/统计索引。
CREATE TABLE IF NOT EXISTS tasks_archive (
  id                bigint(20) unsigned NOT NULL,
  job_id            bigint(20) unsigned NOT NULL,
  track_id          bigint(20) NOT NULL,
  title             varchar(500) DEFAULT NULL,
  artist            varchar(500) DEFAULT NULL,
  album             varchar(500) DEFAULT NULL,
  album_id          bigint(20) DEFAULT NULL,
  track_number      int(11) DEFAULT 0,
  duration          int(11) DEFAULT 0,
  audio_quality     varchar(50) DEFAULT 'LOSSLESS',
  isrc              varchar(50) DEFAULT NULL,
  status            varchar(50) DEFAULT NULL,        -- completed / dead
  assigned_account_id bigint(20) unsigned DEFAULT NULL,
  retry_count       int(11) DEFAULT 0,
  error_code        varchar(100) DEFAULT NULL,
  error_message     text DEFAULT NULL,
  file_size         bigint(20) DEFAULT 0,
  actual_quality    varchar(50) DEFAULT NULL,
  codec             varchar(100) DEFAULT NULL,
  s3_key            varchar(1000) DEFAULT NULL,
  storage_id        varchar(50) DEFAULT NULL,
  completed_at      datetime DEFAULT NULL,
  PRIMARY KEY (id),
  KEY idx_job_completed (job_id, completed_at),      -- 导出/趋势:按 job + 时间
  KEY idx_job_status (job_id, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 2. 把现有 completed/dead 数据搬到归档表(分批,避免大事务;按 id 区间循环执行)。
--    示例(实际迁移时用脚本按 id 分批):
-- INSERT INTO tasks_archive (id,job_id,track_id,title,artist,album,album_id,track_number,
--   duration,audio_quality,isrc,status,assigned_account_id,retry_count,error_code,
--   error_message,file_size,actual_quality,codec,s3_key,storage_id,completed_at)
-- SELECT id,job_id,track_id,title,artist,album,album_id,track_number,duration,audio_quality,
--   isrc,status,assigned_account_id,retry_count,error_code,error_message,file_size,
--   actual_quality,codec,s3_key,storage_id,completed_at
-- FROM tasks WHERE status IN ('completed','dead') AND id BETWEEN ? AND ?;
-- DELETE FROM tasks WHERE status IN ('completed','dead') AND id BETWEEN ? AND ?;

-- 3. 瘦身后的 tasks 表只需抢占相关索引。
--    抢占查询: WHERE job_id IN (...) AND status='pending' LIMIT N
--    取回查询: WHERE assigned_worker_id=? AND status='assigned'
--    这两个索引已在原表存在(idx_job_id/idx_status_id/idx_assigned_worker),迁移后仍有效。

-- 4. (可选)迁移后 OPTIMIZE TABLE tasks; 回收空间。
