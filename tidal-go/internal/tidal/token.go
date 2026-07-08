package tidal

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// TV 客户端凭证(设备码授权账号用);Web 账号用各自的 oauth_client_id 刷新。
const (
	tvClientID  = "7m7Ap0JC9j1cOM3n"
	tvSecret    = "vRAdA108tlvkJpTsGZS8rGZ7xTlbJ0qaZ2K9saEzsgY="
	authTokenURL = "https://auth.tidal.com/v1/oauth2/token"
)

// RefreshResult token 刷新结果。
type RefreshResult struct {
	AccessToken  string
	RefreshToken string // 可能为空(TIDAL 不总是返回新的)
	ExpiresIn    int
}

// RefreshToken 用 refresh_token 换新的 access_token。
// oauthClientID 非空 → Web/公开客户端(只需 client_id);为空 → TV 客户端(client_id+secret)。
// 这个函数不依赖 server 存活,可被独立 goroutine 或独立进程调用。
func RefreshToken(httpc *http.Client, refreshToken, oauthClientID string) (*RefreshResult, error) {
	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", refreshToken)
	if oauthClientID != "" {
		form.Set("client_id", oauthClientID)
	} else {
		form.Set("client_id", tvClientID)
		form.Set("client_secret", tvSecret)
	}

	req, _ := http.NewRequest(http.MethodPost, authTokenURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := httpc
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var body struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int    `json:"expires_in"`
		Error        string `json:"error"`
		ErrorDesc    string `json:"error_description"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, err
	}
	if body.AccessToken == "" {
		return nil, &TokenError{Code: body.Error, Desc: body.ErrorDesc}
	}
	if body.ExpiresIn == 0 {
		body.ExpiresIn = 86400
	}
	return &RefreshResult{
		AccessToken:  body.AccessToken,
		RefreshToken: body.RefreshToken,
		ExpiresIn:    body.ExpiresIn,
	}, nil
}

// TokenError 刷新失败错误。
type TokenError struct {
	Code string
	Desc string
}

func (e *TokenError) Error() string {
	if e.Desc != "" {
		return e.Desc
	}
	if e.Code != "" {
		return e.Code
	}
	return "token refresh failed"
}
