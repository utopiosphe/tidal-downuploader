package handlers

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"tidal-go/internal/models"
)

type registerReq struct {
	Name           string `json:"name"`
	Hostname       string `json:"hostname"`
	IP             string `json:"ip"`
	MaxConcurrency int    `json:"max_concurrency"`
}

// RegisterWorker 注册 worker(同 hostname+name 复用记录)。
func (h *Handler) RegisterWorker(c *gin.Context) {
	var req registerReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	name := req.Name
	if name == "" {
		name = req.Hostname
	}

	var existingID string
	err := h.DB.QueryRow("SELECT id FROM workers WHERE hostname=? AND name=? LIMIT 1", req.Hostname, name).Scan(&existingID)
	var workerID string
	if err == nil && existingID != "" {
		workerID = existingID
		_, _ = h.DB.Exec("UPDATE workers SET ip=?, status='online', last_heartbeat=NOW() WHERE id=?", req.IP, workerID)
	} else {
		workerID = "w-" + randHex(12)
		conc := req.MaxConcurrency
		if conc <= 0 {
			conc = 10
		}
		_, _ = h.DB.Exec(
			"INSERT INTO workers (id,name,hostname,ip,max_concurrency,status,last_heartbeat,registered_at) "+
				"VALUES (?,?,?,?,?,'online',NOW(),NOW())",
			workerID, name, req.Hostname, req.IP, conc,
		)
	}
	c.JSON(http.StatusOK, gin.H{"worker_id": workerID})
}

type heartbeatReq struct {
	Status          string `json:"status"`
	ActiveTasks     int    `json:"active_tasks"`
	TotalDownloaded int    `json:"total_downloaded"`
	TotalFailed     int    `json:"total_failed"`
	TotalBytes      int64  `json:"total_bytes"`
}

// Heartbeat worker 心跳。
func (h *Handler) Heartbeat(c *gin.Context) {
	workerID := c.Param("id")
	var req heartbeatReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	_, _ = h.DB.Exec(
		"UPDATE workers SET status=?, active_tasks=?, total_downloaded=?, total_failed=?, total_bytes=?, last_heartbeat=NOW() WHERE id=?",
		req.Status, req.ActiveTasks, req.TotalDownloaded, req.TotalFailed, req.TotalBytes, workerID,
	)
	c.JSON(http.StatusOK, gin.H{"message": "ok"})
}

// WorkerConfig 下发配置给 worker。
func (h *Handler) WorkerConfig(c *gin.Context) {
	workerID := c.Param("id")
	var w models.Worker
	if err := h.DB.Get(&w, "SELECT * FROM workers WHERE id=?", workerID); err != nil {
		c.JSON(http.StatusOK, gin.H{"error": "Worker 不存在"})
		return
	}
	cfg := h.Config()
	conc := w.MaxConcurrency
	if conc <= 0 {
		conc = cfg.Download.Concurrency
	}
	c.JSON(http.StatusOK, gin.H{
		"worker_id":   workerID,
		"concurrency": conc,
		"quality":     cfg.Download.Quality,
		"proxy":       cfg.Proxy,
		"s3":          cfg.EnabledS3(),
		"download":    cfg.Download,
	})
}

type updateWorkerReq struct {
	Name           *string `json:"name"`
	MaxConcurrency *int    `json:"max_concurrency"`
}

// UpdateWorker 管理端更新 worker(并发数等)。worker 每 30s 同步一次配置,热生效无需重启。
func (h *Handler) UpdateWorker(c *gin.Context) {
	workerID := c.Param("id")
	var req updateWorkerReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.MaxConcurrency != nil {
		if *req.MaxConcurrency < 1 || *req.MaxConcurrency > 10000 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "max_concurrency 必须在 1-10000 之间"})
			return
		}
		if _, err := h.DB.Exec("UPDATE workers SET max_concurrency=? WHERE id=?", *req.MaxConcurrency, workerID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}
	if req.Name != nil && *req.Name != "" {
		if _, err := h.DB.Exec("UPDATE workers SET name=? WHERE id=?", *req.Name, workerID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{"message": "ok"})
}

// ListWorkers 管理端列表。心跳超过 90s 视为 offline(status 字段是心跳时写入的,进程死掉不会自己更新)。
func (h *Handler) ListWorkers(c *gin.Context) {
	var workers []models.Worker
	_ = h.DB.Select(&workers, "SELECT * FROM workers ORDER BY last_heartbeat DESC")
	for i := range workers {
		if workers[i].LastHeartbeat == nil || time.Since(*workers[i].LastHeartbeat) > 90*time.Second {
			workers[i].Status = "offline"
		}
	}
	c.JSON(http.StatusOK, workers)
}

// DeleteWorker 删除 worker 记录(管理端)。
func (h *Handler) DeleteWorker(c *gin.Context) {
	_, _ = h.DB.Exec("DELETE FROM workers WHERE id=?", c.Param("id"))
	c.JSON(http.StatusOK, gin.H{"message": "Worker 已删除"})
}

func randHex(n int) string {
	b := make([]byte, n/2)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
