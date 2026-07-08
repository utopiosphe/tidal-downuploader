package handlers

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"tidal-go/internal/models"
	"tidal-go/internal/tidal"
)

// ListAccounts 账号列表(管理端)。
func (h *Handler) ListAccounts(c *gin.Context) {
	accts := []models.Account{}
	if err := h.DB.Select(&accts, "SELECT * FROM tidal_accounts ORDER BY id"); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, accts)
}

// GetAccount 账号详情。
func (h *Handler) GetAccount(c *gin.Context) {
	var a models.Account
	if err := h.DB.Get(&a, "SELECT * FROM tidal_accounts WHERE id=?", c.Param("id")); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "账号不存在"})
		return
	}
	c.JSON(http.StatusOK, a)
}

// DeleteAccount 删除账号。
func (h *Handler) DeleteAccount(c *gin.Context) {
	_, _ = h.DB.Exec("DELETE FROM tidal_accounts WHERE id=?", c.Param("id"))
	c.JSON(http.StatusOK, gin.H{"message": "账号已删除"})
}

type createAccountReq struct {
	Email         string `json:"email"`
	AccessToken   string `json:"access_token"`
	RefreshToken  string `json:"refresh_token"`
	CountryCode   string `json:"country_code"`
	OAuthClientID string `json:"oauth_client_id"`
}

// CreateAccount 添加账号(等同 import-token,兼容 POST "")。
func (h *Handler) CreateAccount(c *gin.Context) {
	h.ImportToken(c)
}

// ImportToken 手动导入 token:解析 JWT 提取 uid/cc/cid/exp,存 oauth_client_id 供续期。
func (h *Handler) ImportToken(c *gin.Context) {
	var req createAccountReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	token := strings.TrimSpace(req.AccessToken)
	userID, clientID := int64(0), 0
	country := "NG"
	expiresAt := time.Now().Add(4 * time.Hour)

	// 解析 JWT payload
	if parts := strings.Split(token, "."); len(parts) == 3 {
		if claims := decodeJWT(parts[1]); claims != nil {
			if v, ok := claims["uid"].(float64); ok {
				userID = int64(v)
			}
			if v, ok := claims["cc"].(string); ok && v != "" {
				country = v
			}
			if v, ok := claims["cid"].(float64); ok {
				clientID = int(v)
			}
			if v, ok := claims["exp"].(float64); ok {
				expiresAt = time.Unix(int64(v), 0)
			}
		}
	}
	var oauthID any
	if req.OAuthClientID != "" {
		oauthID = strings.TrimSpace(req.OAuthClientID)
	}
	expStr := expiresAt.Format("2006-01-02 15:04:05")

	// 已存在则更新
	if userID != 0 {
		var existID int64
		if err := h.DB.Get(&existID, "SELECT id FROM tidal_accounts WHERE user_id=?", userID); err == nil {
			_, _ = h.DB.Exec(
				"UPDATE tidal_accounts SET access_token=?, refresh_token=?, token_expires_at=?, client_id=?, "+
					"oauth_client_id=COALESCE(?,oauth_client_id), status='active', error_message='', rate_limit_count=0, updated_at=NOW() WHERE id=?",
				token, req.RefreshToken, expStr, clientID, oauthID, existID)
			c.JSON(http.StatusOK, gin.H{"message": "Token 已更新", "id": existID, "user_id": userID})
			return
		}
	}

	res, err := h.DB.Exec(
		"INSERT INTO tidal_accounts (email,user_id,country_code,access_token,refresh_token,token_expires_at,client_id,oauth_client_id,subscription_type,highest_quality,status) "+
			"VALUES (?,?,?,?,?,?,?,?,'PREMIUM','LOSSLESS','active')",
		req.Email, userID, country, token, req.RefreshToken, expStr, clientID, oauthID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	id, _ := res.LastInsertId()
	c.JSON(http.StatusOK, gin.H{"message": "账号添加成功", "id": id, "user_id": userID, "country_code": country})
}

// RefreshAccountToken 手动刷新指定账号 token(调用 TIDAL)。
func (h *Handler) RefreshAccountToken(c *gin.Context) {
	id := c.Param("id")
	var a models.Account
	if err := h.DB.Get(&a, "SELECT * FROM tidal_accounts WHERE id=?", id); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "账号不存在"})
		return
	}
	if a.RefreshToken == nil || *a.RefreshToken == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "该账号没有 refresh_token"})
		return
	}
	oauthID := ""
	if a.OAuthClientID != nil {
		oauthID = *a.OAuthClientID
	}
	res, err := tidal.RefreshToken(nil, *a.RefreshToken, oauthID)
	if err != nil {
		_, _ = h.DB.Exec("UPDATE tidal_accounts SET status='refresh_failed', error_message=?, updated_at=NOW() WHERE id=?", err.Error(), id)
		c.JSON(http.StatusBadRequest, gin.H{"error": "刷新失败: " + err.Error()})
		return
	}
	exp := time.Now().Add(time.Duration(res.ExpiresIn) * time.Second)
	_, _ = h.DB.Exec(
		"UPDATE tidal_accounts SET access_token=?, token_expires_at=?, status='active', error_message='', updated_at=NOW() WHERE id=?",
		res.AccessToken, exp.Format("2006-01-02 15:04:05"), id)
	if res.RefreshToken != "" {
		_, _ = h.DB.Exec("UPDATE tidal_accounts SET refresh_token=? WHERE id=?", res.RefreshToken, id)
	}
	c.JSON(http.StatusOK, gin.H{"message": "Token 刷新成功", "expires_at": exp.Format("2006-01-02 15:04:05")})
}

// decodeJWT 解码 JWT payload(base64url,补 padding)。
func decodeJWT(payload string) map[string]any {
	if m := len(payload) % 4; m != 0 {
		payload += strings.Repeat("=", 4-m)
	}
	data, err := base64.URLEncoding.DecodeString(payload)
	if err != nil {
		return nil
	}
	var claims map[string]any
	if json.Unmarshal(data, &claims) != nil {
		return nil
	}
	return claims
}
