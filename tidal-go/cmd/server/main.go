// tidal-go server 入口。
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tidal-go/internal/config"
	"tidal-go/internal/db"
	"tidal-go/internal/server"
)

func main() {
	appcfg := config.LoadApp()

	database, err := db.Open(appcfg.DSN)
	if err != nil {
		log.Fatalf("数据库连接失败: %v", err)
	}
	defer database.Close()

	router, h := server.NewRouter(database)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 后台:token 刷新(独立 goroutine,与请求处理隔离)
	go server.RunTokenRefresher(ctx, database)

	// 后台:导出分组构建(每 5 分钟,对 running jobs 追加固化组号)
	go h.RunGroupBuilder(ctx)

	// 后台:job 计数攒批 flush + 配置热更新
	go func() {
		flushTicker := time.NewTicker(2 * time.Second)
		cfgTicker := time.NewTicker(30 * time.Second)
		defer flushTicker.Stop()
		defer cfgTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				h.FlushJobCounters() // 退出前最后 flush
				return
			case <-flushTicker.C:
				h.FlushJobCounters()
			case <-cfgTicker.C:
				h.ReloadConfig()
			}
		}
	}()

	// 优雅退出
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Println("收到退出信号,flush 计数中...")
		cancel()
		time.Sleep(1 * time.Second)
		os.Exit(0)
	}()

	log.Printf("🚀 tidal-go server 监听 %s", appcfg.ListenAddr)
	if err := router.Run(appcfg.ListenAddr); err != nil {
		log.Fatalf("server 启动失败: %v", err)
	}
}
