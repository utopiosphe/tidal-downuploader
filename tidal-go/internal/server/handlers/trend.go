package handlers

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// GetTrend 批次完成/失败趋势(按时间分桶聚合)。
// 数据源:tasks_archive(completed/dead 已归档到这里)。
// 自动分桶:≤6h→5min,≤24h→15min,≤72h→1h,>72h→6h。
func (h *Handler) GetTrend(c *gin.Context) {
	jobID, _ := strconv.ParseInt(c.Query("job_id"), 10, 64)
	hours, _ := strconv.Atoi(c.DefaultQuery("hours", "24"))
	if hours < 1 {
		hours = 1
	}
	if hours > 720 {
		hours = 720
	}

	// 分桶粒度(分钟)
	var bucketMin int
	switch {
	case hours <= 6:
		bucketMin = 5
	case hours <= 24:
		bucketMin = 15
	case hours <= 72:
		bucketMin = 60
	default:
		bucketMin = 360
	}

	// 对齐到桶边界的 SQL 表达式
	var bucketExpr string
	if bucketMin < 60 {
		bucketExpr = fmt.Sprintf(
			"CONCAT(DATE_FORMAT(completed_at,'%%Y-%%m-%%d %%H:'),LPAD(FLOOR(MINUTE(completed_at)/%d)*%d,2,'0'))",
			bucketMin, bucketMin)
	} else {
		hpb := bucketMin / 60
		bucketExpr = fmt.Sprintf(
			"CONCAT(DATE_FORMAT(completed_at,'%%Y-%%m-%%d '),LPAD(FLOOR(HOUR(completed_at)/%d)*%d,2,'0'),':00')",
			hpb, hpb)
	}

	type bucket struct {
		Bucket string `db:"bucket"`
		Cnt    int    `db:"cnt"`
		Size   int64  `db:"size"`
	}

	// completed 分桶
	completed := map[string]bucket{}
	rows, _ := h.DB.Queryx(fmt.Sprintf(
		"SELECT %s AS bucket, COUNT(*) AS cnt, COALESCE(SUM(file_size),0) AS size "+
			"FROM tasks_archive WHERE job_id=? AND status='completed' "+
			"AND completed_at >= NOW() - INTERVAL ? HOUR GROUP BY bucket ORDER BY bucket", bucketExpr),
		jobID, hours)
	if rows != nil {
		for rows.Next() {
			var b bucket
			if rows.StructScan(&b) == nil {
				completed[b.Bucket] = b
			}
		}
		rows.Close()
	}

	// failed/dead 分桶
	failed := map[string]bucket{}
	rows2, _ := h.DB.Queryx(fmt.Sprintf(
		"SELECT %s AS bucket, COUNT(*) AS cnt, 0 AS size "+
			"FROM tasks_archive WHERE job_id=? AND status IN ('failed','dead') "+
			"AND completed_at >= NOW() - INTERVAL ? HOUR GROUP BY bucket ORDER BY bucket", bucketExpr),
		jobID, hours)
	if rows2 != nil {
		for rows2.Next() {
			var b bucket
			if rows2.StructScan(&b) == nil {
				failed[b.Bucket] = b
			}
		}
		rows2.Close()
	}

	// 累计基数(时间窗口之前的量)
	var cumCompleted, cumFailed int
	var cumSize int64
	_ = h.DB.QueryRow(
		"SELECT COUNT(*), COALESCE(SUM(file_size),0) FROM tasks_archive "+
			"WHERE job_id=? AND status='completed' AND completed_at < NOW() - INTERVAL ? HOUR",
		jobID, hours).Scan(&cumCompleted, &cumSize)
	_ = h.DB.QueryRow(
		"SELECT COUNT(*) FROM tasks_archive WHERE job_id=? AND status IN ('failed','dead') "+
			"AND completed_at < NOW() - INTERVAL ? HOUR", jobID, hours).Scan(&cumFailed)

	// 合并所有桶,按时间排序
	keys := map[string]struct{}{}
	for k := range completed {
		keys[k] = struct{}{}
	}
	for k := range failed {
		keys[k] = struct{}{}
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)

	result := make([]gin.H, 0, len(sorted))
	for _, b := range sorted {
		cCnt, cSize := completed[b].Cnt, completed[b].Size
		fCnt := failed[b].Cnt
		cumCompleted += cCnt
		cumSize += cSize
		cumFailed += fCnt
		result = append(result, gin.H{
			"time": b, "completed": cCnt, "failed": fCnt, "size": cSize,
			"cum_completed": cumCompleted, "cum_size": cumSize, "cum_failed": cumFailed,
		})
	}

	c.JSON(http.StatusOK, gin.H{"buckets": result, "cached": false})
	_ = time.Now
}
