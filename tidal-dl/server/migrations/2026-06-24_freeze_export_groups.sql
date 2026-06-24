-- ============================================================
-- 固化导出分组：给每个 task 分配【永久组号】export_group_idx
-- 解决：旧逻辑用实时 offset 定位组成员，导致 CSV/清理结果随时间漂移，
--       清理因 s3_key 过滤口径不一致而删错对象（删到更新的批次）。
--
-- 原则：成员一次性钉死，永不重算 → Excel/计费固化、清理精确到组。
-- 已验证：按 (completed_at, id) 升序的组边界 == 现有 export_groups.time_range，
--          因此回填【不改变已有批次】，只是固定它们。
--
-- 注意：第 2、3 步在 ~888 万行上执行，请在低峰期运行；
--       第 3 步是单条大 UPDATE，耗时数分钟，建议执行前确认机器负载。
-- ============================================================

-- 1) 加列（DEFAULT NULL，INSTANT/inplace，瞬时完成）
ALTER TABLE tasks ADD COLUMN export_group_idx INT NULL DEFAULT NULL;

-- 2) 加索引（CSV / 清理 / 统计都按 (job_id, export_group_idx) 走）
ALTER TABLE tasks ADD INDEX idx_job_groupidx (job_id, export_group_idx);

-- 3) 回填 job 3：按 (completed_at, id) 升序固化组号，组号 = 序号 // 50000
--    只处理 status='completed' 的任务（与现有建组口径一致：含无 s3_key 的）。
UPDATE tasks t
JOIN (
    SELECT id,
           FLOOR((ROW_NUMBER() OVER (ORDER BY completed_at ASC, id ASC) - 1) / 50000) AS gidx
    FROM tasks
    WHERE job_id = 3 AND status = 'completed'
) r ON t.id = r.id
SET t.export_group_idx = r.gidx;

-- 4) 校验：固化后的组边界应与现有 export_groups.time_range 完全一致
--    （抽查几组，人工核对 min/max completed_at）
-- SELECT export_group_idx, COUNT(*) cnt, MIN(completed_at) t0, MAX(completed_at) t1
-- FROM tasks WHERE job_id=3 AND export_group_idx IN (50,120,170) GROUP BY export_group_idx;
