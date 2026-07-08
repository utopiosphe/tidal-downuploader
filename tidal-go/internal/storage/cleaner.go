package storage

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	appcfg "tidal-go/internal/config"
)

// Cleaner 批量删除 S3 对象。跳过 GCS(已关闭)和禁用的存储。
type Cleaner struct {
	clients map[string]*cleanTarget
}

type cleanTarget struct {
	client *s3.Client
	bucket string
}

// NewCleaner 从配置构建,返回 (cleaner, 第一个存储的 sid 作为 default)。
func NewCleaner(cfgs []appcfg.S3Config) (*Cleaner, string) {
	c := &Cleaner{clients: make(map[string]*cleanTarget)}
	defaultSID := ""
	for _, cfg := range cfgs {
		if defaultSID == "" {
			defaultSID = cfg.ID
		}
		// 硬编码跳过 GCS(已彻底关闭)+ 禁用的存储
		if cfg.Provider == "gcs" || !cfg.Enabled {
			continue
		}
		conf, err := awscfg.LoadDefaultConfig(context.Background(),
			awscfg.WithRegion(defaultStr(cfg.Region, "us-east-1")),
			awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")))
		if err != nil {
			continue
		}
		client := s3.NewFromConfig(conf, func(o *s3.Options) {
			if cfg.Endpoint != "" {
				o.BaseEndpoint = aws.String(cfg.Endpoint)
			}
		})
		c.clients[cfg.ID] = &cleanTarget{client: client, bucket: cfg.Bucket}
	}
	return c, defaultSID
}

// DeleteBatches 按 storage_id 分组批量删除(每批 1000)。progress 回调累计删除数。
// 返回 (已删数, 是否有存储失败)。
func (c *Cleaner) DeleteBatches(ctx context.Context, batches map[string][]string, progress func(int)) (int, bool) {
	cleaned := 0
	hadErr := false
	for sid, keys := range batches {
		target, ok := c.clients[sid]
		if !ok {
			// 未配置/已禁用(如 GCS),跳过,不计失败
			continue
		}
		for i := 0; i < len(keys); i += 1000 {
			end := i + 1000
			if end > len(keys) {
				end = len(keys)
			}
			objs := make([]s3types.ObjectIdentifier, 0, end-i)
			for _, k := range keys[i:end] {
				objs = append(objs, s3types.ObjectIdentifier{Key: aws.String(k)})
			}
			_, err := target.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(target.bucket),
				Delete: &s3types.Delete{Objects: objs, Quiet: aws.Bool(true)},
			})
			if err != nil {
				hadErr = true
				break // 该存储失败,不中断其他存储
			}
			cleaned += len(objs)
			if progress != nil {
				progress(cleaned)
			}
		}
	}
	return cleaned, hadErr
}
