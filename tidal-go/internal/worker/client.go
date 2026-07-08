package worker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"tidal-go/internal/config"
	"tidal-go/internal/models"
)

// ServerClient 与 server 通信。单进程复用一个 http.Client(keep-alive),
// 因此整台机器对 server 只保持极少量连接(对比 Python 每 worker 独立连接池)。
type ServerClient struct {
	base string
	http *http.Client
}

// NewServerClient 创建。
func NewServerClient(base string) *ServerClient {
	return &ServerClient{
		base: base,
		http: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        16,
				MaxIdleConnsPerHost: 16,
				IdleConnTimeout:     60 * time.Second,
			},
		},
	}
}

func (c *ServerClient) postJSON(path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, _ := json.Marshal(in)
		body = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(http.MethodPost, c.base+path, body)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("http %d: %s", resp.StatusCode, string(b))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func (c *ServerClient) getJSON(path string, out any) error {
	resp, err := c.http.Get(c.base + path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("http %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// RegisterResp 注册响应。
type RegisterResp struct {
	WorkerID string `json:"worker_id"`
}

// Register 注册 worker。
func (c *ServerClient) Register(name, hostname, ip string, concurrency int) (*RegisterResp, error) {
	var r RegisterResp
	err := c.postJSON("/api/workers/register", map[string]any{
		"name": name, "hostname": hostname, "ip": ip, "max_concurrency": concurrency,
	}, &r)
	return &r, err
}

// WorkerConfig server 下发的配置。
type WorkerConfig struct {
	Concurrency int                   `json:"concurrency"`
	Quality     string                `json:"quality"`
	Proxy       config.ProxyConfig    `json:"proxy"`
	S3          []config.S3Config     `json:"s3"`
	Download    config.DownloadConfig `json:"download"`
}

// GetConfig 拉取配置。
func (c *ServerClient) GetConfig(workerID string) (*WorkerConfig, error) {
	var cfg WorkerConfig
	err := c.getJSON("/api/workers/"+workerID+"/config", &cfg)
	return &cfg, err
}

// Heartbeat 心跳。
func (c *ServerClient) Heartbeat(workerID string, active, downloaded, failed int, bytes int64) error {
	return c.postJSON("/api/workers/"+workerID+"/heartbeat", map[string]any{
		"status": "online", "active_tasks": active,
		"total_downloaded": downloaded, "total_failed": failed, "total_bytes": bytes,
	}, nil)
}

// FetchResp 拉取任务响应。
type FetchResp struct {
	Tasks []models.Task `json:"tasks"`
}

// FetchTasks 拉取待下载任务。
func (c *ServerClient) FetchTasks(workerID string, batchSize int) (*FetchResp, error) {
	var r FetchResp
	err := c.postJSON("/api/tasks/fetch", map[string]any{
		"worker_id": workerID, "batch_size": batchSize,
	}, &r)
	return &r, err
}

// StatusUpdate 单条状态上报(用于攒批)。
type StatusUpdate struct {
	TaskID        int64  `json:"task_id"`
	Status        string `json:"status"`
	AccountID     *int64 `json:"account_id,omitempty"`
	ErrorCode     string `json:"error_code,omitempty"`
	ErrorMessage  string `json:"error_message,omitempty"`
	FileSize      int64  `json:"file_size,omitempty"`
	ActualQuality string `json:"actual_quality,omitempty"`
	Codec         string `json:"codec,omitempty"`
	S3Key         string `json:"s3_key,omitempty"`
	StorageID     string `json:"storage_id,omitempty"`
}

// ReportBatch 批量上报状态(成功+失败都走这里,掐掉失败风暴)。
func (c *ServerClient) ReportBatch(updates []StatusUpdate) error {
	return c.postJSON("/api/tasks/report", map[string]any{"updates": updates}, nil)
}

// FetchAccounts 拉取可用账号。
func (c *ServerClient) FetchAccounts() ([]models.Account, error) {
	var accts []models.Account
	err := c.getJSON("/api/accounts/available", &accts)
	return accts, err
}
