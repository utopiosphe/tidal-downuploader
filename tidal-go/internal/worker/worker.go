// Package worker 实现下载 worker:单进程 + goroutine 池,流式下载,攒批上报。
package worker

import (
	"context"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"tidal-go/internal/config"
	"tidal-go/internal/models"
	"tidal-go/internal/storage"
	"tidal-go/internal/tidal"
)

// Worker 下载 worker。
type Worker struct {
	name      string
	server    *ServerClient
	workerID  string
	tmpDir    string

	mu          sync.RWMutex
	cfg         *WorkerConfig
	tidalClient *tidal.Client
	uploader    *storage.MultiUploader
	concurrency int

	pool     *AccountPool
	statusCh chan StatusUpdate

	// 统计(原子)
	totalDownloaded int64
	totalFailed     int64
	totalBytes      int64
	activeTasks     int64
}

// New 创建 worker。
func New(name, serverURL, tmpDir string) *Worker {
	return &Worker{
		name:        name,
		server:      NewServerClient(serverURL),
		tmpDir:      tmpDir,
		pool:        NewAccountPool(),
		statusCh:    make(chan StatusUpdate, 4096),
		concurrency: 10,
	}
}

// Run 启动 worker,阻塞直到 ctx 取消。
func (w *Worker) Run(ctx context.Context) error {
	if err := w.register(); err != nil {
		return err
	}
	if err := w.loadConfig(); err != nil {
		return err
	}

	// 启动时清理上次残留的临时文件(修复 Python 版 690G 泄漏)
	w.cleanupOrphanTemps()

	go w.heartbeatLoop(ctx)
	go w.reporterLoop(ctx)
	return w.mainLoop(ctx)
}

func (w *Worker) register() error {
	host, _ := os.Hostname()
	name := w.name
	if name == "" {
		name = host
	}
	r, err := w.server.Register(name, host, localIP(), w.concurrency)
	if err != nil {
		return err
	}
	w.workerID = r.WorkerID
	log.Printf("✅ 注册成功: %s", w.workerID)
	return nil
}

func (w *Worker) loadConfig() error {
	cfg, err := w.server.GetConfig(w.workerID)
	if err != nil {
		return err
	}
	w.applyConfig(cfg)

	// 账号池初始化
	if accts, err := w.server.FetchAccounts(); err == nil {
		w.pool.Sync(accts)
		log.Printf("🔑 账号池初始化: %d 个可用账号", len(accts))
	} else {
		log.Printf("账号获取失败: %v", err)
	}
	w.mu.RLock()
	proxyHost := w.cfg.Proxy.Host
	nStore := 0
	if w.uploader != nil && w.uploader.Enabled() {
		nStore = 1
	}
	w.mu.RUnlock()
	log.Printf("📋 配置: 并发=%d, 代理=%s, 存储=%d个", w.concurrency, proxyHost, nStore)
	return nil
}

// applyConfig 应用/热更新配置(代理、S3、并发)。
func (w *Worker) applyConfig(cfg *WorkerConfig) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.cfg = cfg
	if cfg.Concurrency > 0 {
		w.concurrency = cfg.Concurrency
	}
	// 代理 transport
	tr, err := buildTransport(cfg.Proxy)
	if err != nil {
		log.Printf("代理构建失败: %v", err)
		tr = nil
	}
	w.tidalClient = tidal.NewClient(tr, w.tmpDir)
	// S3
	if w.uploader == nil {
		w.uploader = storage.NewMultiUploader(cfg.S3)
	} else {
		w.uploader.Update(cfg.S3)
	}
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	count := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = w.server.Heartbeat(w.workerID,
				int(atomic.LoadInt64(&w.activeTasks)),
				int(atomic.LoadInt64(&w.totalDownloaded)),
				int(atomic.LoadInt64(&w.totalFailed)),
				atomic.LoadInt64(&w.totalBytes))
			count++
			if count%3 == 0 { // 每 30s 同步配置
				if cfg, err := w.server.GetConfig(w.workerID); err == nil {
					w.applyConfig(cfg)
				}
				if accts, err := w.server.FetchAccounts(); err == nil {
					w.pool.Sync(accts)
				}
			}
		}
	}
}

// reporterLoop 攒批上报:每 50 条或每 2 秒 flush 一次。成功和失败都走这里。
func (w *Worker) reporterLoop(ctx context.Context) {
	batch := make([]StatusUpdate, 0, 64)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := w.server.ReportBatch(batch); err != nil {
			log.Printf("批量上报失败(将重试): %v", err)
			return // 保留 batch 下次再试
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case u := <-w.statusCh:
			batch = append(batch, u)
			if len(batch) >= 50 {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// currentConc 读取当前并发配置(心跳每 30s 同步,热生效)。
func (w *Worker) currentConc() int {
	w.mu.RLock()
	conc := w.concurrency
	w.mu.RUnlock()
	if conc < 1 {
		conc = 10
	}
	return conc
}

// mainLoop 拉任务 + 分发给 goroutine 池。
// 并发闸门 = activeTasks 原子计数对比 currentConc():
// 每轮只拉「并发余量」个任务,余量为 0 就等——保持背压(内存有界),
// 且并发数可由 server 热更新(无需重启,30s 内生效)。
func (w *Worker) mainLoop(ctx context.Context) error {
	log.Printf("🚀 开始工作循环 (并发: %d)", w.currentConc())
	var wg sync.WaitGroup
	lastConc := w.currentConc()

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return nil
		default:
		}

		conc := w.currentConc()
		if conc != lastConc {
			log.Printf("🔧 并发热更新: %d → %d", lastConc, conc)
			lastConc = conc
		}
		avail := conc - int(atomic.LoadInt64(&w.activeTasks))
		if avail <= 0 {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		resp, err := w.server.FetchTasks(w.workerID, avail)
		if err != nil {
			log.Printf("拉取任务失败: %v", err)
			time.Sleep(3 * time.Second)
			continue
		}
		if len(resp.Tasks) == 0 {
			time.Sleep(3 * time.Second)
			continue
		}
		log.Printf("📥 拉取 %d 个任务", len(resp.Tasks))

		for _, t := range resp.Tasks {
			// 挑账号
			acct, ok := w.pool.Pick()
			if !ok {
				total, cooling := w.pool.Summary()
				log.Printf("无可用账号,等待冷却... (账号池: %d个, %d冷却中)", total, cooling)
				time.Sleep(5 * time.Second)
				// 账号不可用时,任务未被处理,下轮 fetch 超时回收会重新分配
				break
			}
			// 在派发点计数,保证下一轮 avail 计算准确(不等 goroutine 启动)
			atomic.AddInt64(&w.activeTasks, 1)
			wg.Add(1)
			go func(task models.Task, account models.Account) {
				defer wg.Done()
				defer atomic.AddInt64(&w.activeTasks, -1)
				w.processTask(ctx, task, account)
			}(t, acct)
		}
	}
}

// processTask 处理单个任务:下载→上传→上报。
func (w *Worker) processTask(ctx context.Context, task models.Task, acct models.Account) {
	// activeTasks 计数已在 mainLoop 派发点维护
	defer w.pool.Release(acct.ID)

	accID := acct.ID
	quality := "LOSSLESS"
	w.mu.RLock()
	if w.cfg != nil && w.cfg.Quality != "" {
		quality = w.cfg.Quality
	}
	tc := w.tidalClient
	up := w.uploader
	w.mu.RUnlock()

	token := ""
	if acct.AccessToken != nil {
		token = *acct.AccessToken
	}
	country := acct.CountryCode
	if country == "" {
		country = "NG"
	}

	title, artist := "?", "?"
	if task.Title != nil {
		title = *task.Title
	}
	if task.Artist != nil {
		artist = *task.Artist
	}
	log.Printf("🎵 [%d] %s - %s (track=%d, acc=%d)", task.ID, artist, title, task.TrackID, accID)

	// 上报开始下载
	w.statusCh <- StatusUpdate{TaskID: task.ID, Status: "downloading", AccountID: &accID}

	res, err := tc.Download(ctx, task.TrackID, token, quality, country)
	if err != nil {
		w.handleDownloadError(task, accID, err)
		return
	}
	// 保证临时文件一定被删除(修复泄漏)
	defer os.Remove(res.FilePath)

	fi, statErr := os.Stat(res.FilePath)
	if statErr != nil {
		w.failTask(task.ID, accID, "DOWNLOAD_ERROR", statErr.Error())
		return
	}
	size := fi.Size()
	ext := strings.TrimPrefix(filepath.Ext(res.FilePath), ".")

	// 过滤:M4A / 小文件(与 Python 版一致,标记 completed 但记原因)
	if ext == "m4a" || ext == "mp4" {
		w.statusCh <- StatusUpdate{TaskID: task.ID, Status: "completed", ErrorMessage: "Skipped M4A format", AccountID: &accID}
		atomic.AddInt64(&w.totalDownloaded, 1)
		w.pool.ReportSuccess(accID)
		return
	}
	if size <= 5*1024*1024 {
		w.statusCh <- StatusUpdate{TaskID: task.ID, Status: "completed", ErrorMessage: "Skipped small file", AccountID: &accID}
		atomic.AddInt64(&w.totalDownloaded, 1)
		w.pool.ReportSuccess(accID)
		return
	}

	// 上传 S3
	var s3Key, storageID string
	if up != nil && up.Enabled() {
		w.statusCh <- StatusUpdate{TaskID: task.ID, Status: "uploading", AccountID: &accID}
		storageID, s3Key, err = up.Upload(ctx, res.FilePath, task.TrackID, ext)
		if err != nil {
			w.failTask(task.ID, accID, "UPLOAD_ERROR", err.Error())
			return
		}
	} else {
		s3Key = res.FilePath
	}

	// 上报完成
	w.statusCh <- StatusUpdate{
		TaskID: task.ID, Status: "completed", FileSize: size,
		ActualQuality: res.ActualQuality, Codec: res.Codec,
		S3Key: s3Key, StorageID: storageID, AccountID: &accID,
	}
	atomic.AddInt64(&w.totalDownloaded, 1)
	atomic.AddInt64(&w.totalBytes, size)
	w.pool.ReportSuccess(accID)
}

// handleDownloadError 按错误类型处理(重试/冷却/移除账号),全部走攒批上报。
func (w *Worker) handleDownloadError(task models.Task, accID int64, err error) {
	switch {
	case errors.Is(err, tidal.ErrTokenExpired):
		w.failTask(task.ID, accID, "TOKEN_EXPIRED", "Access token expired")
		w.pool.ReportUnavailable(accID)
	case errors.Is(err, tidal.ErrAccountBanned):
		w.failTask(task.ID, accID, "FORBIDDEN", "Track forbidden (403)")
		atomic.AddInt64(&w.totalFailed, 1)
	case errors.Is(err, tidal.ErrTrackNotFound):
		w.failTask(task.ID, accID, "TRACK_NOT_FOUND", "Track not found")
	case errors.Is(err, tidal.ErrRateLimited):
		w.failTask(task.ID, accID, "RATE_LIMITED", "Rate limited (429)")
		w.pool.ReportRateLimited(accID)
	default:
		msg := err.Error()
		if len(msg) > 500 {
			msg = msg[:500]
		}
		w.failTask(task.ID, accID, "DOWNLOAD_ERROR", msg)
		atomic.AddInt64(&w.totalFailed, 1)
	}
}

func (w *Worker) failTask(taskID, accID int64, code, msg string) {
	log.Printf("❌ [%d] %s: %s (acc=%d)", taskID, code, msg, accID)
	w.statusCh <- StatusUpdate{
		TaskID: taskID, Status: "failed", AccountID: &accID,
		ErrorCode: code, ErrorMessage: msg,
	}
}

// cleanupOrphanTemps 删除临时目录里上次残留的 tidal_* 文件。
func (w *Worker) cleanupOrphanTemps() {
	matches, _ := filepath.Glob(filepath.Join(w.tmpDir, "tidal_*"))
	n := 0
	for _, m := range matches {
		if os.Remove(m) == nil {
			n++
		}
	}
	if n > 0 {
		log.Printf("🧹 清理残留临时文件: %d 个", n)
	}
}

var _ = config.Defaults // 保持 config import(供未来直接读默认值)
