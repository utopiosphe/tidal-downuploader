package handlers

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"tidal-go/internal/models"
)

// ---- fetch:原子抢占任务 ----

type fetchReq struct {
	WorkerID  string `json:"worker_id"`
	BatchSize int    `json:"batch_size"`
}

var lastTimeoutReset time.Time

// Fetch 拉取待下载任务(原子抢占,主表已冷热分离,恒定快)。
func (h *Handler) Fetch(c *gin.Context) {
	var req fetchReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.BatchSize <= 0 {
		req.BatchSize = 10
	}

	// 超时回收:60 秒执行一次(避免每次 fetch 都做 UPDATE)
	if time.Since(lastTimeoutReset) > 60*time.Second {
		lastTimeoutReset = time.Now()
		timeout := h.Config().Download.TaskTimeout
		if timeout <= 0 {
			timeout = 300
		}
		_, _ = h.DB.Exec(
			"UPDATE tasks SET status='pending', assigned_worker_id=NULL, assigned_account_id=NULL "+
				"WHERE status IN ('assigned','downloading','uploading') "+
				"AND updated_at < DATE_SUB(NOW(), INTERVAL ? SECOND)",
			timeout,
		)
	}

	// 找 running jobs
	var jobIDs []int64
	if err := h.DB.Select(&jobIDs, "SELECT id FROM jobs WHERE status='running'"); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if len(jobIDs) == 0 {
		c.JSON(http.StatusOK, gin.H{"tasks": []any{}})
		return
	}

	// 原子抢占:UPDATE ... LIMIT
	query, args, _ := sqlxIn(
		"UPDATE tasks SET status='assigned', assigned_worker_id=?, updated_at=NOW() "+
			"WHERE job_id IN (?) AND status='pending' LIMIT ?",
		req.WorkerID, jobIDs, req.BatchSize,
	)
	res, err := h.DB.Exec(query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		c.JSON(http.StatusOK, gin.H{"tasks": []any{}})
		return
	}

	// 取回刚分配的任务
	var tasks []models.Task
	err = h.DB.Select(&tasks,
		"SELECT * FROM tasks WHERE assigned_worker_id=? AND status='assigned' "+
			"ORDER BY updated_at DESC LIMIT ?",
		req.WorkerID, req.BatchSize,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"tasks": tasks})
}

// ---- report:统一批量上报(合并 status + account report) ----

type reportItem struct {
	TaskID        int64  `json:"task_id"`
	Status        string `json:"status"`
	AccountID     *int64 `json:"account_id"`
	ErrorCode     string `json:"error_code"`
	ErrorMessage  string `json:"error_message"`
	FileSize      int64  `json:"file_size"`
	ActualQuality string `json:"actual_quality"`
	Codec         string `json:"codec"`
	S3Key         string `json:"s3_key"`
	StorageID     string `json:"storage_id"`
}

type reportReq struct {
	Updates []reportItem `json:"updates"`
}

// Report 批量上报任务状态。completed/dead 走事务归档(冷热分离核心)。
func (h *Handler) Report(c *gin.Context) {
	var req reportReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	for _, it := range req.Updates {
		switch it.Status {
		case "completed":
			h.archiveTask(it, "completed")
		case "failed":
			h.handleFailed(it)
		default:
			// downloading / uploading:轻量更新
			_, _ = h.DB.Exec("UPDATE tasks SET status=?, updated_at=NOW() WHERE id=?", it.Status, it.TaskID)
		}
	}
	c.JSON(http.StatusOK, gin.H{"message": "ok", "updated_count": len(req.Updates)})
}

// archiveTask 事务内:写归档表 + 从 tasks 删除 + 账号计数 + job 计数增量。
func (h *Handler) archiveTask(it reportItem, finalStatus string) {
	tx, err := h.DB.Beginx()
	if err != nil {
		return
	}
	defer tx.Rollback()

	// 读出该任务的 job_id(用于计数)
	var jobID int64
	if err := tx.Get(&jobID, "SELECT job_id FROM tasks WHERE id=?", it.TaskID); err != nil {
		return // 任务不存在(可能已归档),跳过
	}

	// 写归档表(从 tasks 复制不变字段 + 上报的结果字段)
	_, err = tx.Exec(
		"INSERT INTO tasks_archive "+
			"(id,job_id,track_id,title,artist,album,album_id,track_number,duration,audio_quality,isrc,"+
			" status,assigned_account_id,retry_count,error_code,error_message,file_size,actual_quality,"+
			" codec,s3_key,storage_id,completed_at) "+
			"SELECT id,job_id,track_id,title,artist,album,album_id,track_number,duration,audio_quality,isrc,"+
			" ?,?,retry_count,?,?,?,?,?,?,?,NOW() "+
			"FROM tasks WHERE id=? "+
			"ON DUPLICATE KEY UPDATE status=VALUES(status), file_size=VALUES(file_size), "+
			" s3_key=VALUES(s3_key), storage_id=VALUES(storage_id), completed_at=VALUES(completed_at)",
		finalStatus, it.AccountID, nullStr(it.ErrorCode), nullStr(it.ErrorMessage),
		it.FileSize, nullStr(it.ActualQuality), nullStr(it.Codec), nullStr(it.S3Key), nullStr(it.StorageID),
		it.TaskID,
	)
	if err != nil {
		return
	}

	// 从活跃表删除
	if _, err := tx.Exec("DELETE FROM tasks WHERE id=?", it.TaskID); err != nil {
		return
	}

	// 账号下载计数
	if it.AccountID != nil {
		_, _ = tx.Exec(
			"UPDATE tidal_accounts SET total_downloads=total_downloads+1, rate_limit_count=0, last_used_at=NOW() WHERE id=?",
			*it.AccountID,
		)
	}

	if err := tx.Commit(); err != nil {
		return
	}
	// job 计数走攒批
	if finalStatus == "completed" {
		h.addJobCounter(jobID, 1, 0)
	} else {
		h.addJobCounter(jobID, 0, 1)
	}
}

// handleFailed 失败:可重试→回 pending;超限→dead(归档)。
func (h *Handler) handleFailed(it reportItem) {
	var retryCount, maxRetries int
	var jobID int64
	err := h.DB.QueryRow("SELECT retry_count, max_retries, job_id FROM tasks WHERE id=?", it.TaskID).
		Scan(&retryCount, &maxRetries, &jobID)
	if err != nil {
		return
	}

	if retryCount >= maxRetries {
		// dead:归档
		h.archiveTask(reportItem{
			TaskID: it.TaskID, AccountID: it.AccountID,
			ErrorCode: it.ErrorCode, ErrorMessage: it.ErrorMessage,
		}, "dead")
		return
	}
	// 可重试:回 pending,retry_count++
	_, _ = h.DB.Exec(
		"UPDATE tasks SET status='pending', error_code=?, error_message=?, "+
			"retry_count=retry_count+1, assigned_worker_id=NULL, assigned_account_id=NULL, updated_at=NOW() WHERE id=?",
		nullStr(it.ErrorCode), nullStr(it.ErrorMessage), it.TaskID,
	)
}

// nullStr 空串转 NULL(避免写入空字符串)。
func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// sqlxIn 展开 IN 子句(简化版,仅处理本文件用到的形态)。
func sqlxIn(query string, workerID string, ids []int64, limit int) (string, []any, error) {
	placeholders := strings.Repeat("?,", len(ids))
	placeholders = placeholders[:len(placeholders)-1]
	q := strings.Replace(query, "IN (?)", "IN ("+placeholders+")", 1)
	args := make([]any, 0, len(ids)+2)
	args = append(args, workerID)
	for _, id := range ids {
		args = append(args, id)
	}
	args = append(args, limit)
	return q, args, nil
}
