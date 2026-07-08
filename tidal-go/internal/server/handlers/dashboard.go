package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// Health 健康检查。
func (h *Handler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "tidal-go-server"})
}

// Dashboard 总览统计(全部走 jobs 汇总字段 + 小表,毫秒级)。
func (h *Handler) Dashboard(c *gin.Context) {
	var jobsSummary struct {
		JobCount  int   `db:"job_count"`
		Total     int64 `db:"total"`
		Completed int64 `db:"completed"`
		Failed    int64 `db:"failed"`
	}
	_ = h.DB.Get(&jobsSummary,
		"SELECT COUNT(*) job_count, COALESCE(SUM(total_tracks),0) total, "+
			"COALESCE(SUM(completed),0) completed, COALESCE(SUM(failed),0) failed FROM jobs")

	// 活跃任务(主表小,直接 count)
	var active int
	_ = h.DB.Get(&active, "SELECT COUNT(*) FROM tasks WHERE status IN ('assigned','downloading','uploading')")

	pending := jobsSummary.Total - jobsSummary.Completed - jobsSummary.Failed - int64(active)
	if pending < 0 {
		pending = 0
	}

	// worker 统计
	var workerStats struct {
		Total  int `db:"total"`
		Online int `db:"online"`
	}
	_ = h.DB.Get(&workerStats,
		"SELECT COUNT(*) total, SUM(CASE WHEN last_heartbeat > DATE_SUB(NOW(), INTERVAL 60 SECOND) THEN 1 ELSE 0 END) online FROM workers")

	// 账号统计
	rows, _ := h.DB.Query("SELECT status, COUNT(*) FROM tidal_accounts GROUP BY status")
	accountStats := map[string]int{}
	if rows != nil {
		defer rows.Close()
		for rows.Next() {
			var s string
			var n int
			_ = rows.Scan(&s, &n)
			accountStats[s] = n
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"tasks": gin.H{
			"total": jobsSummary.Total, "pending": pending, "active": active,
			"completed": jobsSummary.Completed, "failed": jobsSummary.Failed,
		},
		"total_jobs": jobsSummary.JobCount,
		"workers":    gin.H{"total": workerStats.Total, "online": workerStats.Online},
		"accounts":   accountStats,
	})
}
