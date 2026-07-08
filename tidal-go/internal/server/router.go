// Package server 组装 HTTP 路由与后台服务。
package server

import (
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

		// worker 相关
		api.POST("/workers/register", h.RegisterWorker)
		api.POST("/workers/:id/heartbeat", h.Heartbeat)
		api.GET("/workers/:id/config", h.WorkerConfig)
		api.GET("/workers", h.ListWorkers)

		// 任务热路径
		api.POST("/tasks/fetch", h.Fetch)
		api.POST("/tasks/report", h.Report)

		// 账号
		api.GET("/accounts/available", h.AvailableAccounts)
		api.POST("/accounts/:id/report", h.ReportAccount)
	}

	return r, h
}
