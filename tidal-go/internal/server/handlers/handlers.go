// Package handlers 实现 server 的 HTTP 接口。
package handlers

import (
	"sync"

	"github.com/jmoiron/sqlx"

	"tidal-go/internal/config"
)

// Handler 持有共享依赖。
type Handler struct {
	DB *sqlx.DB

	// jobs 计数攒批:内存累加,后台定时 flush,消除几百 worker 抢同一行的锁竞争。
	counterMu   sync.Mutex
	jobDoneInc  map[int64]int // job_id -> completed 增量
	jobFailInc  map[int64]int // job_id -> failed 增量

	// 业务配置缓存(定时刷新)
	cfgMu sync.RWMutex
	cfg   config.BizConfig
}

// New 创建 Handler。
func New(db *sqlx.DB) *Handler {
	h := &Handler{
		DB:         db,
		jobDoneInc: make(map[int64]int),
		jobFailInc: make(map[int64]int),
	}
	if cfg, err := config.LoadBiz(db); err == nil {
		h.cfg = cfg
	} else {
		h.cfg = config.Defaults()
	}
	return h
}

// Config 返回当前业务配置(读缓存)。
func (h *Handler) Config() config.BizConfig {
	h.cfgMu.RLock()
	defer h.cfgMu.RUnlock()
	return h.cfg
}

// ReloadConfig 重新加载业务配置到缓存。
func (h *Handler) ReloadConfig() {
	if cfg, err := config.LoadBiz(h.DB); err == nil {
		h.cfgMu.Lock()
		h.cfg = cfg
		h.cfgMu.Unlock()
	}
}

// addJobCounter 累加 job 计数增量(线程安全)。
func (h *Handler) addJobCounter(jobID int64, done, fail int) {
	h.counterMu.Lock()
	if done > 0 {
		h.jobDoneInc[jobID] += done
	}
	if fail > 0 {
		h.jobFailInc[jobID] += fail
	}
	h.counterMu.Unlock()
}

// FlushJobCounters 把累积的 job 计数增量写库(后台定时调用)。
func (h *Handler) FlushJobCounters() {
	h.counterMu.Lock()
	done := h.jobDoneInc
	fail := h.jobFailInc
	h.jobDoneInc = make(map[int64]int)
	h.jobFailInc = make(map[int64]int)
	h.counterMu.Unlock()

	ids := make(map[int64]struct{})
	for id := range done {
		ids[id] = struct{}{}
	}
	for id := range fail {
		ids[id] = struct{}{}
	}
	for id := range ids {
		d, f := done[id], fail[id]
		if d == 0 && f == 0 {
			continue
		}
		_, _ = h.DB.Exec(
			"UPDATE jobs SET completed = completed + ?, failed = failed + ?, updated_at = NOW() WHERE id = ?",
			d, f, id,
		)
		// 检查是否全部完成
		var total, completed, failed int
		if err := h.DB.QueryRow(
			"SELECT total_tracks, completed, failed FROM jobs WHERE id = ?", id,
		).Scan(&total, &completed, &failed); err == nil {
			if completed+failed >= total && total > 0 {
				_, _ = h.DB.Exec("UPDATE jobs SET status = 'completed' WHERE id = ?", id)
			}
		}
	}
}
