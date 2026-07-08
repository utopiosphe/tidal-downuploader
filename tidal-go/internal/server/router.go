// Package server 组装 HTTP 路由与后台服务。
package server

import (
	"os"
	"path/filepath"

	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"

	"tidal-go/internal/server/handlers"
)

// NewRouter 构建 Gin 路由。
func NewRouter(db *sqlx.DB) (*gin.Engine, *handlers.Handler) {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery()) // panic 不会拖垮整个进程

	h := handlers.New(db)

	api := r.Group("/api")
	{
		api.GET("/health", h.Health)
		api.GET("/dashboard", h.Dashboard)

		// config
		api.GET("/config", h.GetConfig)
		api.PUT("/config", h.UpdateConfig)

		// worker
		api.POST("/workers/register", h.RegisterWorker)
		api.POST("/workers/:id/heartbeat", h.Heartbeat)
		api.GET("/workers/:id/config", h.WorkerConfig)
		api.GET("/workers", h.ListWorkers)
		api.PUT("/workers/:id", h.UpdateWorker)
		api.DELETE("/workers/:id", h.DeleteWorker)

		// 任务热路径
		api.POST("/tasks/fetch", h.Fetch)
		api.POST("/tasks/report", h.Report)
		api.GET("/tasks/trend", h.GetTrend)

		// jobs
		api.GET("/jobs", h.ListJobs)
		api.POST("/jobs/import", h.ImportJSON)
		api.POST("/jobs/import-csv", h.ImportCSV)
		api.GET("/jobs/:id", h.GetJob)
		api.GET("/jobs/:id/tasks", h.GetJobTasks)
		api.POST("/jobs/:id/retry-failed", h.RetryFailed)
		api.POST("/jobs/:id/pause", h.PauseJob)
		api.POST("/jobs/:id/resume", h.ResumeJob)

		// accounts
		api.GET("/accounts", h.ListAccounts)
		api.GET("/accounts/available", h.AvailableAccounts)
		api.POST("/accounts", h.CreateAccount)
		api.POST("/accounts/import-token", h.ImportToken)
		api.GET("/accounts/:id", h.GetAccount)
		api.DELETE("/accounts/:id", h.DeleteAccount)
		api.POST("/accounts/:id/report", h.ReportAccount)
		api.POST("/accounts/:id/refresh", h.RefreshAccountToken)

		// auth(设备码授权 + token 刷新)
		api.POST("/auth/device-code", h.StartDeviceAuth)
		api.GET("/auth/device-code/:session_id", h.CheckDeviceAuth)
		api.POST("/auth/refresh/:id", h.RefreshAccountToken) // 前端账号页"刷新token"用此路径

		// exports
		api.GET("/exports/groups", h.GetExportGroups)
		api.POST("/exports/groups/refresh", h.RefreshExportGroups)
		api.GET("/exports/groups/download", h.DownloadGroupCSV)
		api.POST("/exports/groups/:id/cleanup", h.CleanupGroupS3)
		api.GET("/exports/groups/:id/cleanup-status", h.GetCleanupStatus)
		api.GET("/exports/storage-stats", h.StorageStats)
	}

	// 挂载前端静态文件(如果存在)
	if dist := findWebDist(); dist != "" {
		r.Use(staticFallback(dist))
	}

	return r, h
}

// findWebDist 查找前端 dist 目录。
func findWebDist() string {
	candidates := []string{
		os.Getenv("TIDAL_WEB_DIST"),
		"/opt/tidal-dl/web/dist",
		"web/dist",
	}
	for _, d := range candidates {
		if d == "" {
			continue
		}
		if fi, err := os.Stat(filepath.Join(d, "index.html")); err == nil && !fi.IsDir() {
			return d
		}
	}
	return ""
}

// staticFallback 提供 SPA 静态文件:存在的文件直接返回,其余回退 index.html。
func staticFallback(dist string) gin.HandlerFunc {
	indexPath := filepath.Join(dist, "index.html")
	return func(c *gin.Context) {
		if len(c.Request.URL.Path) >= 4 && c.Request.URL.Path[:4] == "/api" {
			c.Next()
			return
		}
		p := filepath.Join(dist, filepath.Clean(c.Request.URL.Path))
		if fi, err := os.Stat(p); err == nil && !fi.IsDir() {
			c.File(p)
			c.Abort()
			return
		}
		c.File(indexPath)
		c.Abort()
	}
}
