// tidal-go worker 入口。单进程 + goroutine 池,取代 Python 多进程 worker。
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"tidal-go/internal/config"
	"tidal-go/internal/worker"
)

func main() {
	appcfg := config.LoadApp()

	serverURL := flag.String("server", appcfg.ServerURL, "Server URL")
	name := flag.String("name", appcfg.WorkerName, "Worker 名称")
	flag.Parse()

	w := worker.New(*name, *serverURL, appcfg.TmpDir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Println("收到退出信号,停止 worker...")
		cancel()
	}()

	if err := w.Run(ctx); err != nil {
		log.Fatalf("worker 运行失败: %v", err)
		os.Exit(1)
	}
}
