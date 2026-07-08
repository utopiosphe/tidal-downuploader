// Package tidal 封装 TIDAL API:获取播放信息、DASH/BTS 流式下载、token 刷新。
package tidal

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const apiBase = "https://api.tidal.com/v1"

// Client 是一个带代理的 TIDAL HTTP 客户端,可被多个 goroutine 复用。
type Client struct {
	http   *http.Client
	tmpDir string
}

// NewClient 创建客户端。transport 已配置好代理(见 worker/session.go)。
func NewClient(transport http.RoundTripper, tmpDir string) *Client {
	return &Client{
		http:   &http.Client{Transport: transport, Timeout: 0}, // 每请求单独设超时
		tmpDir: tmpDir,
	}
}

type playbackInfo struct {
	AudioQuality     string `json:"audioQuality"`
	Manifest         string `json:"manifest"`
	ManifestMimeType string `json:"manifestMimeType"`
}

// getPlaybackInfo 获取曲目播放信息,并把 HTTP 状态映射为分类错误。
func (c *Client) getPlaybackInfo(ctx context.Context, trackID int64, token, quality, country string) (*playbackInfo, error) {
	u := fmt.Sprintf("%s/tracks/%d/playbackinfopostpaywall?audioquality=%s&playbackmode=STREAM&assetpresentation=FULL&countryCode=%s",
		apiBase, trackID, url.QueryEscape(quality), url.QueryEscape(country))

	ctx2, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx2, http.MethodGet, u, nil)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDownloadFailed, err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case 200:
		var pb playbackInfo
		if err := json.NewDecoder(resp.Body).Decode(&pb); err != nil {
			return nil, fmt.Errorf("%w: decode: %v", ErrDownloadFailed, err)
		}
		return &pb, nil
	case 401:
		// subStatus 4005 = asset not ready(非 token 问题),其余当作 token 过期
		var body struct {
			SubStatus int `json:"subStatus"`
		}
		b, _ := io.ReadAll(resp.Body)
		_ = json.Unmarshal(b, &body)
		if body.SubStatus == 4005 {
			return nil, ErrTrackNotFound
		}
		return nil, ErrTokenExpired
	case 403:
		return nil, ErrAccountBanned
	case 404:
		return nil, ErrTrackNotFound
	case 429:
		return nil, ErrRateLimited
	default:
		return nil, fmt.Errorf("%w: http %d", ErrDownloadFailed, resp.StatusCode)
	}
}

// DownloadResult 下载结果。
type DownloadResult struct {
	FilePath      string // 本地临时文件(调用方负责删除)
	Codec         string
	ActualQuality string
}

// Download 下载曲目到本地临时文件(流式,内存占用极低)。
func (c *Client) Download(ctx context.Context, trackID int64, token, quality, country string) (*DownloadResult, error) {
	pb, err := c.getPlaybackInfo(ctx, trackID, token, quality, country)
	if err != nil {
		return nil, err
	}

	mime := strings.ToLower(pb.ManifestMimeType)
	var rawPath, codec string
	switch {
	case strings.Contains(mime, "dash"):
		rawPath, codec, err = c.downloadDASH(ctx, pb.Manifest)
	case strings.Contains(mime, "vnd.tidal.bts"):
		rawPath, codec, err = c.downloadBTS(ctx, pb.Manifest)
	default:
		return nil, fmt.Errorf("%w: unsupported manifest %q", ErrDownloadFailed, pb.ManifestMimeType)
	}
	if err != nil {
		return nil, err
	}

	// 转封装:flac 需 mp4->flac,mp4a 保持 m4a
	lc := strings.ToLower(codec)
	var finalExt string
	switch {
	case strings.Contains(lc, "flac"):
		finalExt = "flac"
	case strings.Contains(lc, "mp4a"):
		finalExt = "m4a"
	default:
		finalExt = "mp4"
	}

	rawExt := strings.TrimPrefix(filepath.Ext(rawPath), ".")
	if rawExt != finalExt {
		finalPath := strings.TrimSuffix(rawPath, filepath.Ext(rawPath)) + "." + finalExt
		if ffmpegRemux(rawPath, finalPath) == nil {
			_ = os.Remove(rawPath)
			return &DownloadResult{FilePath: finalPath, Codec: codec, ActualQuality: pb.AudioQuality}, nil
		}
		// 转封装失败,退回原始文件
		return &DownloadResult{FilePath: rawPath, Codec: codec, ActualQuality: pb.AudioQuality}, nil
	}
	return &DownloadResult{FilePath: rawPath, Codec: codec, ActualQuality: pb.AudioQuality}, nil
}

// ---- DASH ----

type mpd struct {
	Periods []struct {
		AdaptationSets []struct {
			Representations []struct {
				Codecs          string `xml:"codecs,attr"`
				SegmentTemplate struct {
					Initialization  string `xml:"initialization,attr"`
					Media           string `xml:"media,attr"`
					SegmentTimeline struct {
						S []struct {
							R int `xml:"r,attr"`
						} `xml:"S"`
					} `xml:"SegmentTimeline"`
				} `xml:"SegmentTemplate"`
			} `xml:"Representation"`
		} `xml:"AdaptationSet"`
	} `xml:"Period"`
}

// downloadDASH 解析 DASH manifest,并发下载分段,直接写入磁盘临时文件(不在内存拼接)。
func (c *Client) downloadDASH(ctx context.Context, manifestB64 string) (string, string, error) {
	raw, err := base64.StdEncoding.DecodeString(manifestB64)
	if err != nil {
		return "", "", fmt.Errorf("%w: manifest b64: %v", ErrDownloadFailed, err)
	}
	var m mpd
	if err := xml.Unmarshal(raw, &m); err != nil {
		return "", "", fmt.Errorf("%w: manifest xml: %v", ErrDownloadFailed, err)
	}

	for _, p := range m.Periods {
		for _, as := range p.AdaptationSets {
			for _, rep := range as.Representations {
				st := rep.SegmentTemplate
				if st.Media == "" {
					continue
				}
				codec := rep.Codecs
				if codec == "" {
					codec = "unknown"
				}

				// 展开分段编号(SegmentTimeline 的 r 表示额外重复次数)
				var segNums []int
				n := 1
				for _, s := range st.SegmentTimeline.S {
					repeat := s.R + 1
					for i := 0; i < repeat; i++ {
						segNums = append(segNums, n)
						n++
					}
				}

				// 临时文件
				tmp, err := os.CreateTemp(c.tmpDir, "tidal_*.mp4")
				if err != nil {
					return "", "", fmt.Errorf("%w: temp: %v", ErrDownloadFailed, err)
				}
				tmpPath := tmp.Name()

				// 先下 init 段
				initData, err := c.fetchSegment(ctx, st.Initialization)
				if err != nil {
					tmp.Close()
					os.Remove(tmpPath)
					return "", "", err
				}
				if _, err := tmp.Write(initData); err != nil {
					tmp.Close()
					os.Remove(tmpPath)
					return "", "", fmt.Errorf("%w: write init: %v", ErrDownloadFailed, err)
				}

				// 并发下载各分段到内存,但**按序写盘**(每段下完即可释放)
				// 限制并发 10,和 Python 版一致,避免对代理压力过大。
				type segResult struct {
					data []byte
					err  error
				}
				results := make([]segResult, len(segNums))
				sem := make(chan struct{}, 10)
				var wg sync.WaitGroup
				for i, num := range segNums {
					wg.Add(1)
					sem <- struct{}{}
					go func(idx, segNum int) {
						defer wg.Done()
						defer func() { <-sem }()
						segURL := strings.ReplaceAll(st.Media, "$Number$", strconv.Itoa(segNum))
						d, e := c.fetchSegment(ctx, segURL)
						results[idx] = segResult{data: d, err: e}
					}(i, num)
				}
				wg.Wait()

				// 按序写盘并释放内存
				for i := range results {
					if results[i].err != nil {
						tmp.Close()
						os.Remove(tmpPath)
						return "", "", results[i].err
					}
					if _, err := tmp.Write(results[i].data); err != nil {
						tmp.Close()
						os.Remove(tmpPath)
						return "", "", fmt.Errorf("%w: write seg: %v", ErrDownloadFailed, err)
					}
					results[i].data = nil // 尽早释放
				}
				tmp.Close()
				return tmpPath, codec, nil
			}
		}
	}
	return "", "", fmt.Errorf("%w: no representation in manifest", ErrDownloadFailed)
}

// fetchSegment 下载单个分段,带 3 次重试。
func (c *Client) fetchSegment(ctx context.Context, url string) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		ctx2, cancel := context.WithTimeout(ctx, 60*time.Second)
		req, _ := http.NewRequestWithContext(ctx2, http.MethodGet, url, nil)
		resp, err := c.http.Do(req)
		if err != nil {
			cancel()
			lastErr = err
			continue
		}
		if resp.StatusCode != 200 {
			resp.Body.Close()
			cancel()
			lastErr = fmt.Errorf("http %d", resp.StatusCode)
			continue
		}
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		return data, nil
	}
	return nil, fmt.Errorf("%w: segment after 3 retries: %v", ErrDownloadFailed, lastErr)
}

// ---- BTS ----

func (c *Client) downloadBTS(ctx context.Context, manifestB64 string) (string, string, error) {
	raw, err := base64.StdEncoding.DecodeString(manifestB64)
	if err != nil {
		return "", "", fmt.Errorf("%w: bts b64: %v", ErrDownloadFailed, err)
	}
	var mf struct {
		URLs   []string `json:"urls"`
		Codecs string   `json:"codecs"`
	}
	if err := json.Unmarshal(raw, &mf); err != nil {
		return "", "", fmt.Errorf("%w: bts json: %v", ErrDownloadFailed, err)
	}
	if len(mf.URLs) == 0 {
		return "", "", fmt.Errorf("%w: bts no urls", ErrDownloadFailed)
	}

	tmp, err := os.CreateTemp(c.tmpDir, "tidal_*.mp4")
	if err != nil {
		return "", "", fmt.Errorf("%w: temp: %v", ErrDownloadFailed, err)
	}
	tmpPath := tmp.Name()

	ctx2, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx2, http.MethodGet, mf.URLs[0], nil)
	resp, err := c.http.Do(req)
	if err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return "", "", fmt.Errorf("%w: bts fetch: %v", ErrDownloadFailed, err)
	}
	defer resp.Body.Close()

	// 流式拷贝到磁盘,不进内存
	if _, err := io.Copy(tmp, resp.Body); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return "", "", fmt.Errorf("%w: bts copy: %v", ErrDownloadFailed, err)
	}
	tmp.Close()

	codec := mf.Codecs
	if codec == "" {
		codec = "unknown"
	}
	return tmpPath, codec, nil
}

// ffmpegRemux 用 ffmpeg -c copy 转封装(不重编码)。
func ffmpegRemux(in, out string) error {
	cmd := exec.Command("ffmpeg", "-y", "-i", in, "-c", "copy", out)
	return cmd.Run()
}
