package handlers

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"tidal-go/internal/tidal"
)

// 设备码授权会话(内存态,单进程 OK;Go 单进程无 Python 多进程共享问题)。
type authSession struct {
	deviceCode string
	userCode   string
	verifyURL  string
	status     string // pending / completed / expired / error
	accountID  int64
	errMsg     string
}

var (
	authMu       sync.Mutex
	authSessions = map[string]*authSession{}
)

// StartDeviceAuth 发起设备码授权,返回验证链接,后台 goroutine 轮询。
func (h *Handler) StartDeviceAuth(c *gin.Context) {
	dc, err := tidal.StartDeviceAuth()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "获取设备码失败: " + err.Error()})
		return
	}
	sessionID := dc.DeviceCode
	if len(sessionID) > 16 {
		sessionID = sessionID[:16]
	}
	sess := &authSession{
		deviceCode: dc.DeviceCode, userCode: dc.UserCode,
		verifyURL: dc.VerifyURL, status: "pending",
	}
	authMu.Lock()
	authSessions[sessionID] = sess
	authMu.Unlock()

	go h.pollDeviceAuth(sessionID, dc.Interval, dc.ExpiresIn)

	c.JSON(http.StatusOK, gin.H{
		"session_id": sessionID, "user_code": dc.UserCode,
		"verify_url": dc.VerifyURL, "expires_in": dc.ExpiresIn,
		"message": "请在浏览器中打开链接并登录 TIDAL 账号",
	})
}

func (h *Handler) pollDeviceAuth(sessionID string, interval, expiresIn int) {
	deadline := time.Now().Add(time.Duration(expiresIn) * time.Second)
	authMu.Lock()
	deviceCode := authSessions[sessionID].deviceCode
	authMu.Unlock()

	for time.Now().Before(deadline) {
		time.Sleep(time.Duration(interval) * time.Second)
		res, err := tidal.PollDeviceToken(deviceCode)
		if err != nil {
			continue
		}
		if res.Pending {
			continue
		}
		authMu.Lock()
		sess := authSessions[sessionID]
		authMu.Unlock()
		if sess == nil {
			return
		}
		if res.Expired {
			authMu.Lock()
			sess.status = "expired"
			sess.errMsg = "设备码已过期"
			authMu.Unlock()
			return
		}
		// 成功:保存账号
		accID, err := h.saveDeviceAccount(res)
		authMu.Lock()
		if err != nil {
			sess.status = "error"
			sess.errMsg = err.Error()
		} else {
			sess.status = "completed"
			sess.accountID = accID
		}
		authMu.Unlock()
		return
	}
	authMu.Lock()
	if s := authSessions[sessionID]; s != nil {
		s.status = "expired"
		s.errMsg = "授权超时"
	}
	authMu.Unlock()
}

// saveDeviceAccount 保存设备码授权得到的账号(存在则更新)。
func (h *Handler) saveDeviceAccount(r *tidal.DeviceTokenResult) (int64, error) {
	exp := time.Now().Add(time.Duration(r.ExpiresIn) * time.Second).Format("2006-01-02 15:04:05")
	if r.UserID != 0 {
		var existID int64
		if err := h.DB.Get(&existID, "SELECT id FROM tidal_accounts WHERE user_id=?", r.UserID); err == nil {
			_, err := h.DB.Exec(
				"UPDATE tidal_accounts SET access_token=?, refresh_token=?, token_expires_at=?, status='active', error_message='', updated_at=NOW() WHERE id=?",
				r.AccessToken, r.RefreshToken, exp, existID)
			return existID, err
		}
	}
	country := r.CountryCode
	if country == "" {
		country = "NG"
	}
	res, err := h.DB.Exec(
		"INSERT INTO tidal_accounts (email,user_id,country_code,access_token,refresh_token,token_expires_at,subscription_type,highest_quality,status) "+
			"VALUES (?,?,?,?,?,?,'PREMIUM','LOSSLESS','active')",
		r.Email, r.UserID, country, r.AccessToken, r.RefreshToken, exp)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// CheckDeviceAuth 查询设备码授权状态。
func (h *Handler) CheckDeviceAuth(c *gin.Context) {
	sessionID := c.Param("session_id")
	authMu.Lock()
	sess := authSessions[sessionID]
	authMu.Unlock()
	if sess == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "会话不存在或已过期"})
		return
	}
	authMu.Lock()
	defer authMu.Unlock()
	switch sess.status {
	case "completed":
		id := sess.accountID
		delete(authSessions, sessionID)
		c.JSON(http.StatusOK, gin.H{"status": "completed", "message": "授权成功,账号已添加", "account_id": id})
	case "error":
		msg := sess.errMsg
		delete(authSessions, sessionID)
		c.JSON(http.StatusOK, gin.H{"status": "error", "error": msg})
	default:
		c.JSON(http.StatusOK, gin.H{
			"status": sess.status, "error": sess.errMsg,
			"user_code": sess.userCode, "verify_url": sess.verifyURL,
		})
	}
}
