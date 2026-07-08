package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"tidal-go/internal/models"
)

// AvailableAccounts 返回可用账号(worker 用)。只返回 active 且未过期的。
func (h *Handler) AvailableAccounts(c *gin.Context) {
	var accts []models.Account
	err := h.DB.Select(&accts,
		"SELECT id,email,user_id,country_code,access_token,refresh_token,token_expires_at,"+
			"client_id,oauth_client_id,status,total_downloads,rate_limit_count,cooldown_until "+
			"FROM tidal_accounts WHERE status='active' AND token_expires_at > NOW() ORDER BY id",
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, accts)
}

type accountReportReq struct {
	Status       string `json:"status"`
	ErrorMessage string `json:"error_message"`
}

// ReportAccount 上报账号异常(兼容旧接口;新版 worker 走 report 攒批,但保留此端点)。
func (h *Handler) ReportAccount(c *gin.Context) {
	id := c.Param("id")
	var req accountReportReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	status := "token_expired"
	switch req.Status {
	case "rate_limited":
		// 429:设冷却,但不改 active 状态
		_, _ = h.DB.Exec(
			"UPDATE tidal_accounts SET rate_limit_count=rate_limit_count+1, "+
				"cooldown_until=DATE_ADD(NOW(), INTERVAL 60 SECOND), updated_at=NOW() WHERE id=?", id)
		c.JSON(http.StatusOK, gin.H{"message": "ok"})
		return
	case "token_expired":
		status = "token_expired"
	default:
		status = req.Status
	}
	_, _ = h.DB.Exec(
		"UPDATE tidal_accounts SET status=?, error_message=?, updated_at=NOW() WHERE id=?",
		status, req.ErrorMessage, id)
	c.JSON(http.StatusOK, gin.H{"message": "ok"})
}
