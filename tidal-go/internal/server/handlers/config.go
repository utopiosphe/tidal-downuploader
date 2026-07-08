package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
)

// GetConfig 返回完整业务配置(管理端)。
func (h *Handler) GetConfig(c *gin.Context) {
	c.JSON(http.StatusOK, h.Config())
}

// UpdateConfig 部分更新配置(写 config 表 + 刷新缓存)。
func (h *Handler) UpdateConfig(c *gin.Context) {
	var body map[string]json.RawMessage
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	for key, val := range body {
		_, _ = h.DB.Exec(
			"INSERT INTO config (`key`,`value`) VALUES (?,?) ON DUPLICATE KEY UPDATE `value`=VALUES(`value`)",
			key, string(val))
	}
	h.ReloadConfig()
	c.JSON(http.StatusOK, gin.H{"message": "配置已更新"})
}
