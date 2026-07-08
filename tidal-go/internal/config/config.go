// Package config 负责运行时配置:进程级(环境变量)+ 业务级(config 表 JSON)。
package config

import (
	"encoding/json"
	"os"

	"github.com/jmoiron/sqlx"
)

// AppConfig 进程级配置,来自环境变量。
type AppConfig struct {
	DSN        string // MySQL DSN
	ListenAddr string // server 监听地址
	ServerURL  string // worker 连接的 server 地址
	WorkerName string // worker 名称
	TmpDir     string // worker 临时文件目录
}

// LoadApp 从环境变量加载进程级配置。
func LoadApp() AppConfig {
	return AppConfig{
		DSN:        env("TIDAL_DSN", "tidal:tidal_dl_2026@tcp(127.0.0.1:3306)/tidal_dl?charset=utf8mb4&parseTime=true&loc=Local"),
		ListenAddr: env("TIDAL_LISTEN", "0.0.0.0:8000"),
		ServerURL:  env("TIDAL_SERVER", "http://127.0.0.1:8000"),
		WorkerName: env("TIDAL_WORKER_NAME", ""),
		TmpDir:     env("TMPDIR", "/tmp"),
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// ProxyConfig SOCKS5/HTTP 代理配置。
type ProxyConfig struct {
	Host       string `json:"host"`
	Socks5Port int    `json:"socks5_port"`
	HTTPPort   int    `json:"http_port"`
	Username   string `json:"username"`
	Password   string `json:"password"`
	Protocol   string `json:"protocol"`
}

// S3Config 单个 S3/GCS 存储配置。
type S3Config struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	Provider       string `json:"provider"` // aws / gcs
	Enabled        bool   `json:"enabled"`
	Endpoint       string `json:"endpoint"`
	AccessKey      string `json:"access_key"`
	SecretKey      string `json:"secret_key"`
	Bucket         string `json:"bucket"`
	Region         string `json:"region"`
	Prefix         string `json:"prefix"`
	DownloadDomain string `json:"download_domain"`
}

// DownloadConfig 下载相关配置。
type DownloadConfig struct {
	Quality         string `json:"quality"`
	Concurrency     int    `json:"concurrency"`
	MaxRetries      int    `json:"max_retries"`
	RetryDelay      int    `json:"retry_delay"`
	RateLimitDelay  int    `json:"rate_limit_delay"`
	TaskTimeout     int    `json:"task_timeout"`
	CountryCode     string `json:"country_code"`
}

// BizConfig 业务级配置(存于 config 表)。
type BizConfig struct {
	Proxy    ProxyConfig    `json:"proxy"`
	S3       []S3Config     `json:"s3"`
	Download DownloadConfig `json:"download"`
}

// Defaults 返回默认业务配置。
func Defaults() BizConfig {
	return BizConfig{
		Download: DownloadConfig{
			Quality: "LOSSLESS", Concurrency: 10, MaxRetries: 3,
			RetryDelay: 5, RateLimitDelay: 30, TaskTimeout: 300, CountryCode: "NG",
		},
	}
}

// LoadBiz 从 config 表读取业务配置(key -> JSON value),缺失用默认值补齐。
func LoadBiz(db *sqlx.DB) (BizConfig, error) {
	cfg := Defaults()
	rows, err := db.Query("SELECT `key`, `value` FROM config")
	if err != nil {
		return cfg, err
	}
	defer rows.Close()

	for rows.Next() {
		var key, val string
		if err := rows.Scan(&key, &val); err != nil {
			return cfg, err
		}
		switch key {
		case "proxy":
			_ = json.Unmarshal([]byte(val), &cfg.Proxy)
		case "download":
			_ = json.Unmarshal([]byte(val), &cfg.Download)
		case "s3":
			// s3 可能是 list 或旧格式 dict,先试 list
			if err := json.Unmarshal([]byte(val), &cfg.S3); err != nil {
				var single S3Config
				if json.Unmarshal([]byte(val), &single) == nil {
					if single.ID == "" {
						single.ID = "aws-eu"
					}
					cfg.S3 = []S3Config{single}
				}
			}
		}
	}
	return cfg, rows.Err()
}

// EnabledS3 返回启用的存储列表。
func (c BizConfig) EnabledS3() []S3Config {
	var out []S3Config
	for _, s := range c.S3 {
		if s.Enabled {
			out = append(out, s)
		}
	}
	return out
}
