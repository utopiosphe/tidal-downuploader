package handlers

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"

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

// ListWorkers 管理端列表。
func (h *Handler) ListWorkers(c *gin.Context) {
	var workers []models.Worker
	_ = h.DB.Select(&workers, "SELECT * FROM workers ORDER BY last_heartbeat DESC")
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
