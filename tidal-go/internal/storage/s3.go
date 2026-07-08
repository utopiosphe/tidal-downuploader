// Package storage 提供多 S3/GCS 存储上传(随机负载均衡)。
package storage

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"tidal-go/internal/config"
)

// Uploader 单个 S3 存储。
type Uploader struct {
	cfg    config.S3Config
	client *s3.Client
}

func newUploader(c config.S3Config) (*Uploader, error) {
	ctx := context.Background()
	awsConf, err := awscfg.LoadDefaultConfig(ctx,
		awscfg.WithRegion(defaultStr(c.Region, "us-east-1")),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.AccessKey, c.SecretKey, "")),
	)
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(awsConf, func(o *s3.Options) {
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
		}
		o.UsePathStyle = c.Provider == "gcs"
	})
	return &Uploader{cfg: c, client: client}, nil
}

// upload 流式上传本地文件到 S3(SDK 内部走 multipart,不整文件进内存)。
func (u *Uploader) upload(ctx context.Context, localPath, key string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer f.Close()

	ctx2, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	_, err = u.client.PutObject(ctx2, &s3.PutObjectInput{
		Bucket: aws.String(u.cfg.Bucket),
		Key:    aws.String(key),
		Body:   f,
	})
	return err
}

// MultiUploader 管理多个存储,随机选取上传。
type MultiUploader struct {
	mu        sync.RWMutex
	uploaders map[string]*Uploader
}

// NewMultiUploader 从配置构建。
func NewMultiUploader(cfgs []config.S3Config) *MultiUploader {
	m := &MultiUploader{uploaders: make(map[string]*Uploader)}
	m.Update(cfgs)
	return m
}

// Update 热更新存储列表。
func (m *MultiUploader) Update(cfgs []config.S3Config) {
	m.mu.Lock()
	defer m.mu.Unlock()
	next := make(map[string]*Uploader)
	for _, c := range cfgs {
		if !c.Enabled || c.Endpoint == "" || c.Bucket == "" {
			continue
		}
		if u, err := newUploader(c); err == nil {
			next[c.ID] = u
		}
	}
	m.uploaders = next
}

// Enabled 是否有可用存储。
func (m *MultiUploader) Enabled() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.uploaders) > 0
}

// Upload 随机选一个存储上传,返回 (storageID, s3Key)。
func (m *MultiUploader) Upload(ctx context.Context, localPath string, trackID int64, ext string) (string, string, error) {
	m.mu.RLock()
	var ids []string
	for id := range m.uploaders {
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		m.mu.RUnlock()
		return "", "", fmt.Errorf("no storage available")
	}
	id := ids[rand.Intn(len(ids))]
	u := m.uploaders[id]
	m.mu.RUnlock()

	prefix := u.cfg.Prefix
	if prefix == "" {
		prefix = "flac/"
	}
	// 去掉尾斜杠再拼
	for len(prefix) > 0 && prefix[len(prefix)-1] == '/' {
		prefix = prefix[:len(prefix)-1]
	}
	key := fmt.Sprintf("%s/%d.%s", prefix, trackID, ext)

	if err := u.upload(ctx, localPath, key); err != nil {
		return "", "", err
	}
	return id, key, nil
}

func defaultStr(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
