package worker

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/net/proxy"

	"tidal-go/internal/config"
)

// buildTransport 根据代理配置构建 http.RoundTripper。
// SOCKS5 用 golang.org/x/net/proxy;HTTP 代理用标准 Transport.Proxy。
// 连接池参数针对高并发下载调大;跳过 TLS 校验(与 Python verify=False 一致)。
func buildTransport(p config.ProxyConfig) (http.RoundTripper, error) {
	// 必须走 HTTP/1.1:代理按单条 TCP 连接限速(~0.5-2.6MB/s),
	// HTTP/2 会把所有请求多路复用进极少数连接,总带宽被掐死
	// (实测 200 并发只建了 40 条连接,18MB/s;Python 版 500 连接池能跑 270MB/s)。
	// TLSNextProto 置空 map 彻底禁用 h2,一个 in-flight 请求一条连接,靠连接数堆总带宽。
	tr := &http.Transport{
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
		MaxIdleConns:        0, // 不限总空闲连接
		MaxIdleConnsPerHost: 2000,
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   false,
		TLSNextProto:        make(map[string]func(string, *tls.Conn) http.RoundTripper),
	}

	if p.Host == "" {
		return tr, nil
	}

	if p.Protocol == "socks5" || p.Protocol == "" {
		port := p.Socks5Port
		if port == 0 {
			port = 41003
		}
		var auth *proxy.Auth
		if p.Username != "" {
			auth = &proxy.Auth{User: p.Username, Password: p.Password}
		}
		dialer, err := proxy.SOCKS5("tcp", fmt.Sprintf("%s:%d", p.Host, port), auth, proxy.Direct)
		if err != nil {
			return nil, err
		}
		// 用 SOCKS5 dialer 作为底层拨号
		tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dialer.Dial(network, addr)
		}
		return tr, nil
	}

	// HTTP 代理
	port := p.HTTPPort
	if port == 0 {
		port = 41002
	}
	proxyURL := &url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%d", p.Host, port),
	}
	if p.Username != "" {
		proxyURL.User = url.UserPassword(p.Username, p.Password)
	}
	tr.Proxy = http.ProxyURL(proxyURL)
	return tr, nil
}

// localIP 返回本机对外 IP(尽力而为,失败返回 "unknown")。
func localIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "unknown"
	}
	defer conn.Close()
	if addr, ok := conn.LocalAddr().(*net.UDPAddr); ok {
		return addr.IP.String()
	}
	return "unknown"
}
