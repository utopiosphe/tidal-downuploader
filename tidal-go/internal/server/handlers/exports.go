package handlers

import (
	"context"
	"encoding/csv"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"tidal-go/internal/config"
	"tidal-go/internal/storage"
)

// GroupSize 每个导出分组的固化大小。
const GroupSize = 50000

// GetExportGroups 获取批次导出分组列表。
func (h *Handler) GetExportGroups(c *gin.Context) {
	jobID := c.Query("job_id")
	type grp struct {
		ID              int64      `db:"id" json:"id"`
		JobID           int64      `db:"job_id" json:"job_id"`
		GroupIndex      int        `db:"group_index" json:"group_index"`
		TaskCount       int        `db:"task_count" json:"task_count"`
		TotalSize       int64      `db:"total_size" json:"total_size"`
		S3CleanupStatus string     `db:"s3_cleanup_status" json:"s3_cleanup_status"`
		S3CleanedCount  int        `db:"s3_cleaned_count" json:"s3_cleaned_count"`
		TimeRangeStart  *time.Time `db:"time_range_start" json:"time_range_start"`
		TimeRangeEnd    *time.Time `db:"time_range_end" json:"time_range_end"`
		Sealed          bool       `db:"-" json:"sealed"`
	}
	var groups []grp
	_ = h.DB.Select(&groups, "SELECT id,job_id,group_index,task_count,total_size,s3_cleanup_status,s3_cleaned_count,time_range_start,time_range_end FROM export_groups WHERE job_id=? ORDER BY group_index", jobID)

	var jobStatus string
	_ = h.DB.Get(&jobStatus, "SELECT status FROM jobs WHERE id=?", jobID)
	jobDone := jobStatus == "completed"

	total, totalSize := 0, int64(0)
	for i := range groups {
		total += groups[i].TaskCount
		totalSize += groups[i].TotalSize
		groups[i].Sealed = groups[i].TaskCount >= GroupSize || jobDone
	}
	c.JSON(http.StatusOK, gin.H{"total": total, "total_size": totalSize, "group_size": GroupSize, "groups": groups})
}

// RefreshExportGroups 手动触发分组构建。
func (h *Handler) RefreshExportGroups(c *gin.Context) {
	jobID, _ := strconv.ParseInt(c.Query("job_id"), 10, 64)
	n := h.buildGroupsForJob(jobID)
	c.JSON(http.StatusOK, gin.H{"message": "分组刷新完成", "groups_count": n})
}

// buildGroupsForJob 为 job 分配并固化导出分组(append-only,成员永不重算)。
// 基于 tasks_archive(completed 已归档),给未分组的 completed 任务追加永久组号。
func (h *Handler) buildGroupsForJob(jobID int64) int {
	// 已分配数 = 追加起始序号
	var base int
	_ = h.DB.Get(&base, "SELECT COUNT(*) FROM tasks_archive WHERE job_id=? AND status='completed' AND export_group_idx IS NOT NULL", jobID)

	// 给未分组的 completed 追加组号 = FLOOR((base + rownum - 1) / GroupSize)
	_, _ = h.DB.Exec(
		"UPDATE tasks_archive t JOIN ("+
			"  SELECT id, (? + ROW_NUMBER() OVER (ORDER BY completed_at ASC, id ASC) - 1) AS seq "+
			"  FROM tasks_archive WHERE job_id=? AND status='completed' AND export_group_idx IS NULL"+
			") r ON t.id=r.id SET t.export_group_idx = FLOOR(r.seq / ?)",
		base, jobID, GroupSize)

	// 只重算受影响的组(>= 起始组)
	startGroup := base / GroupSize
	type row struct {
		Gidx      int       `db:"gidx"`
		Cnt       int       `db:"cnt"`
		TotalSize int64     `db:"total_size"`
		TStart    *time.Time `db:"t_start"`
		TEnd      *time.Time `db:"t_end"`
	}
	var rows []row
	_ = h.DB.Select(&rows,
		"SELECT export_group_idx AS gidx, COUNT(*) AS cnt, COALESCE(SUM(file_size),0) AS total_size, "+
			"MIN(completed_at) AS t_start, MAX(completed_at) AS t_end FROM tasks_archive "+
			"WHERE job_id=? AND export_group_idx IS NOT NULL AND export_group_idx >= ? GROUP BY export_group_idx",
		jobID, startGroup)
	for _, r := range rows {
		_, _ = h.DB.Exec(
			"INSERT INTO export_groups (job_id,group_index,task_count,total_size,time_range_start,time_range_end) "+
				"VALUES (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE task_count=VALUES(task_count), total_size=VALUES(total_size), "+
				"time_range_start=VALUES(time_range_start), time_range_end=VALUES(time_range_end)",
			jobID, r.Gidx, r.Cnt, r.TotalSize, r.TStart, r.TEnd)
	}

	var totalGroups int
	_ = h.DB.Get(&totalGroups, "SELECT COUNT(DISTINCT export_group_idx) FROM tasks_archive WHERE job_id=? AND export_group_idx IS NOT NULL", jobID)
	return totalGroups
}

// DownloadGroupCSV 导出指定分组为 CSV(按固化组号,内容永久可复现,流式)。
func (h *Handler) DownloadGroupCSV(c *gin.Context) {
	jobID, _ := strconv.ParseInt(c.Query("job_id"), 10, 64)
	group, _ := strconv.Atoi(c.DefaultQuery("group", "0"))

	// 封存校验
	var taskCount int
	_ = h.DB.Get(&taskCount, "SELECT task_count FROM export_groups WHERE job_id=? AND group_index=?", jobID, group)
	var jobStatus string
	_ = h.DB.Get(&jobStatus, "SELECT status FROM jobs WHERE id=?", jobID)
	if taskCount > 0 && taskCount < GroupSize && jobStatus != "completed" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "该分组尚未封存(生成中),暂不可下载"})
		return
	}

	// storage_id -> download_domain
	domainMap := map[string]string{}
	defaultDomain := ""
	for _, s := range h.Config().S3 {
		d := trimSlash(s.DownloadDomain)
		domainMap[s.ID] = d
		if defaultDomain == "" {
			defaultDomain = d
		}
	}

	var jobName string
	_ = h.DB.Get(&jobName, "SELECT name FROM jobs WHERE id=?", jobID)
	if jobName == "" {
		jobName = fmt.Sprintf("job_%d", jobID)
	}
	filename := fmt.Sprintf("%s_group_%d.csv", jobName, group+1)

	c.Header("Content-Type", "text/csv; charset=utf-8")
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename*=UTF-8''%s", filename))

	c.Writer.Write([]byte{0xEF,0xBB,0xBF}) // UTF-8 BOM for Excel
	w := csv.NewWriter(c.Writer)
	_ = w.Write([]string{"ID", "Track ID", "标题", "艺术家", "专辑", "ISRC", "音质", "编码", "文件大小(MB)", "S3路径", "下载链接", "完成时间"})

	rows, err := h.DB.Queryx(
		"SELECT id,track_id,title,artist,album,isrc,actual_quality,codec,file_size,s3_key,storage_id,completed_at "+
			"FROM tasks_archive WHERE job_id=? AND export_group_idx=? ORDER BY completed_at ASC, id ASC",
		jobID, group)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id, trackID, fileSize                                     int64
			title, artist, album, isrc, aq, codec, s3Key, storageID   *string
			completedAt                                               *time.Time
		)
		if err := rows.Scan(&id, &trackID, &title, &artist, &album, &isrc, &aq, &codec, &fileSize, &s3Key, &storageID, &completedAt); err != nil {
			continue
		}
		key := deref(s3Key)
		sid := deref(storageID)
		domain := domainMap[sid]
		if domain == "" {
			domain = defaultDomain
		}
		url := ""
		if key != "" && domain != "" {
			url = domain + "/" + key
		}
		ts := ""
		if completedAt != nil {
			ts = completedAt.Format("2006-01-02 15:04:05")
		}
		_ = w.Write([]string{
			strconv.FormatInt(id, 10), strconv.FormatInt(trackID, 10),
			deref(title), deref(artist), deref(album), deref(isrc), deref(aq), deref(codec),
			fmt.Sprintf("%.2f", float64(fileSize)/1024/1024), key, url, ts,
		})
		w.Flush()
	}
	w.Flush()
}

// CleanupGroupS3 启动分组 S3 清理(后台 goroutine)。
func (h *Handler) CleanupGroupS3(c *gin.Context) {
	groupID, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	var g struct {
		JobID           int64  `db:"job_id"`
		GroupIndex      int    `db:"group_index"`
		TaskCount       int    `db:"task_count"`
		S3CleanupStatus string `db:"s3_cleanup_status"`
	}
	if err := h.DB.Get(&g, "SELECT job_id,group_index,task_count,s3_cleanup_status FROM export_groups WHERE id=?", groupID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "分组不存在"})
		return
	}
	if g.S3CleanupStatus == "running" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "该分组正在清理中"})
		return
	}
	var jobStatus string
	_ = h.DB.Get(&jobStatus, "SELECT status FROM jobs WHERE id=?", g.JobID)
	if g.TaskCount < GroupSize && jobStatus != "completed" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "该分组尚未封存(未满 5 万),作业完成前不可清理"})
		return
	}

	_, _ = h.DB.Exec("UPDATE export_groups SET s3_cleanup_status='pending', s3_cleaned_count=0 WHERE id=?", groupID)
	cfg := h.Config()
	go h.doCleanup(groupID, g.JobID, g.GroupIndex, cfg.S3)
	c.JSON(http.StatusOK, gin.H{"message": "清理任务已启动", "group_id": groupID})
}

// doCleanup 后台批量删除分组的 S3 对象(跳过已禁用/GCS,单存储失败不中断其他)。
func (h *Handler) doCleanup(groupID, jobID int64, groupIndex int, s3cfgs []config.S3Config) {
	_, _ = h.DB.Exec("UPDATE export_groups SET s3_cleanup_status='running' WHERE id=?", groupID)

	cleaner, defaultSID := storage.NewCleaner(s3cfgs)

	// 查该组所有 s3_key + storage_id(按固化组号)
	rows, err := h.DB.Queryx(
		"SELECT s3_key, storage_id FROM tasks_archive WHERE job_id=? AND export_group_idx=? AND s3_key IS NOT NULL AND s3_key<>''",
		jobID, groupIndex)
	if err != nil {
		_, _ = h.DB.Exec("UPDATE export_groups SET s3_cleanup_status='failed' WHERE id=?", groupID)
		return
	}
	batches := map[string][]string{}
	for rows.Next() {
		var key string
		var sid *string
		if rows.Scan(&key, &sid) != nil {
			continue
		}
		s := defaultSID
		if sid != nil && *sid != "" {
			s = *sid
		}
		batches[s] = append(batches[s], key)
	}
	rows.Close()

	cleaned, hadErr := cleaner.DeleteBatches(context.Background(), batches, func(n int) {
		_, _ = h.DB.Exec("UPDATE export_groups SET s3_cleaned_count=? WHERE id=?", n, groupID)
	})

	if hadErr {
		_, _ = h.DB.Exec("UPDATE export_groups SET s3_cleanup_status='failed', s3_cleaned_count=? WHERE id=?", cleaned, groupID)
	} else {
		_, _ = h.DB.Exec("UPDATE export_groups SET s3_cleanup_status='completed', s3_cleaned_count=?, s3_cleaned_at=NOW() WHERE id=?", cleaned, groupID)
	}
}

// GetCleanupStatus 查询清理进度。
func (h *Handler) GetCleanupStatus(c *gin.Context) {
	var g struct {
		ID              int64      `db:"id" json:"id"`
		S3CleanupStatus string     `db:"s3_cleanup_status" json:"s3_cleanup_status"`
		S3CleanedCount  int        `db:"s3_cleaned_count" json:"s3_cleaned_count"`
		TaskCount       int        `db:"task_count" json:"task_count"`
		S3CleanedAt     *time.Time `db:"s3_cleaned_at" json:"s3_cleaned_at"`
	}
	if err := h.DB.Get(&g, "SELECT id,s3_cleanup_status,s3_cleaned_count,task_count,s3_cleaned_at FROM export_groups WHERE id=?", c.Param("id")); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "分组不存在"})
		return
	}
	c.JSON(http.StatusOK, g)
}

// StorageStats 各存储数据量统计(基于归档表)。
func (h *Handler) StorageStats(c *gin.Context) {
	rows, err := h.DB.Query(
		"SELECT COALESCE(storage_id,'aws-eu') sid, COUNT(*) file_count, COALESCE(SUM(file_size),0) total_size " +
			"FROM tasks_archive WHERE status='completed' AND s3_key IS NOT NULL AND s3_key<>'' GROUP BY sid")
	stats := map[string]gin.H{}
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var sid string
			var fc int
			var ts int64
			_ = rows.Scan(&sid, &fc, &ts)
			stats[sid] = gin.H{"file_count": fc, "total_size": ts}
		}
	}
	c.JSON(http.StatusOK, stats)
}

// RunGroupBuilder 后台定时分组构建(每 5 分钟,对 running jobs)。
func (h *Handler) RunGroupBuilder(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var ids []int64
			_ = h.DB.Select(&ids, "SELECT id FROM jobs WHERE status='running'")
			for _, id := range ids {
				func() {
					defer func() { recover() }()
					h.buildGroupsForJob(id)
				}()
			}
		}
	}
}

func trimSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
