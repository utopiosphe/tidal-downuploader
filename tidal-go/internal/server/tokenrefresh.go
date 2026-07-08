package server

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/jmoiron/sqlx"

	"tidal-go/internal/tidal"
)

// RunTokenRefresher 后台 token 刷新服务。
//
// 关键设计(修复 Python 版死亡螺旋):
//   - 独立 goroutine,panic 隔离 —— server 处理请求再忙/出错也不影响它;
//   - Go 无 GIL,刷新和请求处理并行,不会互相拖累;
//   - MySQL GET_LOCK 咨询锁 —— 多 server 实例时只有一个真正刷新,避免重复;
//   - 立即先跑一次,再定时(不像 Python 版先 sleep 5 分钟留空窗)。
func RunTokenRefresher(ctx context.Context, db *sqlx.DB) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("token refresher panic 恢复: %v,10s 后重启", r)
			time.Sleep(10 * time.Second)
			go RunTokenRefresher(ctx, db) // 自愈重启
		}
	}()

	httpc := &http.Client{Timeout: 15 * time.Second}
	log.Printf("🔄 Token 自动刷新服务已启动(每 3 分钟检查)")

	// 立即跑一次,再定时
	refreshOnce(db, httpc)
	ticker := time.NewTicker(3 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refreshOnce(db, httpc)
		}
	}
}

func refreshOnce(db *sqlx.DB, httpc *http.Client) {
	// 咨询锁:拿不到说明别的实例在刷,跳过
	var got int
	if err := db.Get(&got, "SELECT GET_LOCK('tidal_token_refresh', 0)"); err != nil || got != 1 {
		return
	}
	defer db.Exec("SELECT RELEASE_LOCK('tidal_token_refresh')")

	// 查即将过期(30 分钟内)且有 refresh_token 的账号
	type acct struct {
		ID            int64   `db:"id"`
		RefreshToken  string  `db:"refresh_token"`
		OAuthClientID *string `db:"oauth_client_id"`
	}
	var accts []acct
	err := db.Select(&accts,
		"SELECT id, refresh_token, oauth_client_id FROM tidal_accounts "+
			"WHERE status IN ('active','token_expired') AND refresh_token IS NOT NULL AND refresh_token<>'' "+
			"AND token_expires_at < DATE_ADD(NOW(), INTERVAL 30 MINUTE)")
	if err != nil || len(accts) == 0 {
		return
	}

	log.Printf("🔄 需刷新 %d 个账号 token", len(accts))
	ok, fail := 0, 0
	for _, a := range accts {
		oauthID := ""
		if a.OAuthClientID != nil {
			oauthID = *a.OAuthClientID
		}
		res, err := tidal.RefreshToken(httpc, a.RefreshToken, oauthID)
		if err != nil {
			fail++
			continue
		}
		exp := time.Now().Add(time.Duration(res.ExpiresIn) * time.Second)
		_, _ = db.Exec(
			"UPDATE tidal_accounts SET access_token=?, token_expires_at=?, status='active', error_message='', updated_at=NOW() WHERE id=?",
			res.AccessToken, exp.Format("2006-01-02 15:04:05"), a.ID)
		if res.RefreshToken != "" {
			_, _ = db.Exec("UPDATE tidal_accounts SET refresh_token=? WHERE id=?", res.RefreshToken, a.ID)
		}
		ok++
	}
	log.Printf("🔄 token 刷新完成: 成功=%d 失败=%d", ok, fail)
}
