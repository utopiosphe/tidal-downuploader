package tidal

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const deviceAuthURL = "https://auth.tidal.com/v1/oauth2/device_authorization"

// DeviceCode 设备码授权信息。
type DeviceCode struct {
	DeviceCode string
	UserCode   string
	VerifyURL  string
	Interval   int
	ExpiresIn  int
}

// StartDeviceAuth 发起设备码授权。
func StartDeviceAuth() (*DeviceCode, error) {
	form := url.Values{}
	form.Set("client_id", tvClientID)
	form.Set("scope", "r_usr w_usr")

	req, _ := http.NewRequest(http.MethodPost, deviceAuthURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var body struct {
		DeviceCode              string `json:"deviceCode"`
		UserCode                string `json:"userCode"`
		VerificationURIComplete string `json:"verificationUriComplete"`
		Interval                int    `json:"interval"`
		ExpiresIn               int    `json:"expiresIn"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, err
	}
	verify := body.VerificationURIComplete
	if verify == "" {
		verify = "https://link.tidal.com/" + body.UserCode
	}
	if !strings.HasPrefix(verify, "http") {
		verify = "https://" + verify
	}
	if body.Interval == 0 {
		body.Interval = 5
	}
	if body.ExpiresIn == 0 {
		body.ExpiresIn = 300
	}
	return &DeviceCode{
		DeviceCode: body.DeviceCode, UserCode: body.UserCode,
		VerifyURL: verify, Interval: body.Interval, ExpiresIn: body.ExpiresIn,
	}, nil
}

// DeviceTokenResult 设备码换 token 的结果。
type DeviceTokenResult struct {
	AccessToken  string
	RefreshToken string
	ExpiresIn    int
	UserID       int64
	CountryCode  string
	Email        string
	Pending      bool // 仍在等待用户授权
	Expired      bool // 设备码已过期
}

// PollDeviceToken 轮询一次设备码授权结果。
func PollDeviceToken(deviceCode string) (*DeviceTokenResult, error) {
	form := url.Values{}
	form.Set("client_id", tvClientID)
	form.Set("client_secret", tvSecret)
	form.Set("device_code", deviceCode)
	form.Set("grant_type", "urn:ietf:params:oauth:grant-type:device_code")
	form.Set("scope", "r_usr w_usr")

	req, _ := http.NewRequest(http.MethodPost, authTokenURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		var body struct {
			AccessToken  string `json:"access_token"`
			RefreshToken string `json:"refresh_token"`
			ExpiresIn    int    `json:"expires_in"`
			User         struct {
				UserID      int64  `json:"userId"`
				CountryCode string `json:"countryCode"`
				Email       string `json:"email"`
			} `json:"user"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&body)
		return &DeviceTokenResult{
			AccessToken: body.AccessToken, RefreshToken: body.RefreshToken,
			ExpiresIn: body.ExpiresIn, UserID: body.User.UserID,
			CountryCode: body.User.CountryCode, Email: body.User.Email,
		}, nil
	}

	var errBody struct {
		SubStatus int `json:"sub_status"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&errBody)
	switch errBody.SubStatus {
	case 1002: // authorization_pending
		return &DeviceTokenResult{Pending: true}, nil
	case 1000: // expired
		return &DeviceTokenResult{Expired: true}, nil
	default:
		return &DeviceTokenResult{Expired: true}, nil
	}
}
