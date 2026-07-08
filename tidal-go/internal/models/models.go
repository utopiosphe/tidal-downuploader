// Package models 定义与数据库表对应的数据结构。
package models

import "time"

// Task 对应 tasks 表(活跃任务)。completed/dead 会归档到 tasks_archive。
type Task struct {
	ID                int64      `db:"id" json:"id"`
	JobID             int64      `db:"job_id" json:"job_id"`
	TrackID           int64      `db:"track_id" json:"track_id"`
	Title             *string    `db:"title" json:"title"`
	Artist            *string    `db:"artist" json:"artist"`
	Album             *string    `db:"album" json:"album"`
	AlbumID           *int64     `db:"album_id" json:"album_id"`
	TrackNumber       int        `db:"track_number" json:"track_number"`
	Duration          int        `db:"duration" json:"duration"`
	AudioQuality      string     `db:"audio_quality" json:"audio_quality"`
	ISRC              *string    `db:"isrc" json:"isrc"`
	Status            string     `db:"status" json:"status"`
	AssignedWorkerID  *string    `db:"assigned_worker_id" json:"assigned_worker_id"`
	AssignedAccountID *int64     `db:"assigned_account_id" json:"assigned_account_id"`
	RetryCount        int        `db:"retry_count" json:"retry_count"`
	MaxRetries        int        `db:"max_retries" json:"max_retries"`
	ErrorCode         *string    `db:"error_code" json:"error_code"`
	ErrorMessage      *string    `db:"error_message" json:"error_message"`
	FileSize          int64      `db:"file_size" json:"file_size"`
	ActualQuality     *string    `db:"actual_quality" json:"actual_quality"`
	Codec             *string    `db:"codec" json:"codec"`
	S3Key             *string    `db:"s3_key" json:"s3_key"`
	StorageID         *string    `db:"storage_id" json:"storage_id"`
	CreatedAt         time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt         time.Time  `db:"updated_at" json:"updated_at"`
	CompletedAt       *time.Time `db:"completed_at" json:"completed_at"`
	ExportGroupIdx    *int       `db:"export_group_idx" json:"export_group_idx"`
}

// Job 对应 jobs 表(下载批次)。
type Job struct {
	ID            int64     `db:"id" json:"id"`
	Name          string    `db:"name" json:"name"`
	SourceFile    *string   `db:"source_file" json:"source_file"`
	TotalTracks   int       `db:"total_tracks" json:"total_tracks"`
	Completed     int       `db:"completed" json:"completed"`
	Failed        int       `db:"failed" json:"failed"`
	TargetQuality string    `db:"target_quality" json:"target_quality"`
	Status        string    `db:"status" json:"status"`
	CreatedAt     time.Time `db:"created_at" json:"created_at"`
	UpdatedAt     time.Time `db:"updated_at" json:"updated_at"`
}

// Account 对应 tidal_accounts 表。
type Account struct {
	ID             int64      `db:"id" json:"id"`
	Email          *string    `db:"email" json:"email"`
	UserID         *int64     `db:"user_id" json:"user_id"`
	CountryCode    string     `db:"country_code" json:"country_code"`
	AccessToken    *string    `db:"access_token" json:"access_token"`
	RefreshToken   *string    `db:"refresh_token" json:"refresh_token"`
	TokenExpiresAt *time.Time `db:"token_expires_at" json:"token_expires_at"`
	ClientID       int        `db:"client_id" json:"client_id"`
	OAuthClientID  *string    `db:"oauth_client_id" json:"oauth_client_id"`
	Status         string     `db:"status" json:"status"`
	ErrorMessage   *string    `db:"error_message" json:"error_message"`
	LastUsedAt     *time.Time `db:"last_used_at" json:"last_used_at"`
	TotalDownloads int        `db:"total_downloads" json:"total_downloads"`
	CooldownUntil  *time.Time `db:"cooldown_until" json:"cooldown_until"`
	RateLimitCount int        `db:"rate_limit_count" json:"rate_limit_count"`
	// 下面5列此前缺失,导致 SELECT * 的 sqlx 扫描报 missing destination name,
	// ListAccounts/GetAccount/手动刷新token 全部静默失败
	SubscriptionType    *string    `db:"subscription_type" json:"subscription_type"`
	SubscriptionExpires *time.Time `db:"subscription_expires" json:"subscription_expires"`
	HighestQuality      *string    `db:"highest_quality" json:"highest_quality"`
	CreatedAt           *time.Time `db:"created_at" json:"created_at"`
	UpdatedAt           *time.Time `db:"updated_at" json:"updated_at"`
}

// Worker 对应 workers 表。
type Worker struct {
	ID                string     `db:"id" json:"id"`
	Name              *string    `db:"name" json:"name"`
	Hostname          *string    `db:"hostname" json:"hostname"`
	IP                *string    `db:"ip" json:"ip"`
	Status            string     `db:"status" json:"status"`
	MaxConcurrency    int        `db:"max_concurrency" json:"max_concurrency"`
	ActiveTasks       int        `db:"active_tasks" json:"active_tasks"`
	AssignedAccountID *int64     `db:"assigned_account_id" json:"assigned_account_id"`
	TotalDownloaded   int        `db:"total_downloaded" json:"total_downloaded"`
	TotalFailed       int        `db:"total_failed" json:"total_failed"`
	TotalBytes        int64      `db:"total_bytes" json:"total_bytes"`
	LastHeartbeat     *time.Time `db:"last_heartbeat" json:"last_heartbeat"`
	RegisteredAt      time.Time  `db:"registered_at" json:"registered_at"`
}
