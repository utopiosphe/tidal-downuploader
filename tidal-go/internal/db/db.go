// Package db 提供 MySQL 连接(sqlx)。
package db

import (
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// Open 打开 MySQL 连接池。Go 的 database/sql 连接池远比 Python 版健壮:
// 单进程复用连接,无 blocking 阻塞死锁,连接数由 SetMaxOpenConns 控制。
func Open(dsn string) (*sqlx.DB, error) {
	d, err := sqlx.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	// 连接池:MySQL max_connections=300,单 server 给 100 上限,留余量。
	d.SetMaxOpenConns(100)
	d.SetMaxIdleConns(20)
	d.SetConnMaxLifetime(5 * time.Minute)
	d.SetConnMaxIdleTime(90 * time.Second)

	if err := d.Ping(); err != nil {
		return nil, err
	}
	return d, nil
}
