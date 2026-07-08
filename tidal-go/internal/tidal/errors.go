package tidal

import "errors"

// 下载相关的分类错误,与 Python 版语义一致,worker 据此决定重试/冷却/移除账号。
var (
	ErrTokenExpired   = errors.New("token expired")     // 401 真过期
	ErrAccountBanned  = errors.New("account forbidden") // 403
	ErrTrackNotFound  = errors.New("track not found")   // 404 / 4005
	ErrRateLimited    = errors.New("rate limited")      // 429
	ErrDownloadFailed = errors.New("download failed")   // 其他
)
