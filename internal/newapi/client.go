// Package newapi 是子站 NewAPI 管理接口的最小客户端，只用到同步需要的三个接口：
//
//   - GET  /api/status          读取版本号（公开接口）
//   - POST /api/channel/fix     让子站按自身版本逻辑重建能力表并刷新渠道内存缓存（需要 channel:operate）
//   - PUT  /api/option/         写入一项设置（分组设置、模型价格），子站会校验并立即更新内存（需要 option:write）
package newapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Client 是子站管理接口客户端。
type Client struct {
	baseURL string
	token   string
	userID  int
	http    *http.Client
}

// New 创建客户端。
func New(baseURL, token string, userID int, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   strings.TrimSpace(token),
		userID:  userID,
		http:    &http.Client{Timeout: timeout},
	}
}

// envelope 是 NewAPI 接口的统一返回格式。
type envelope struct {
	Success bool            `json:"success"`
	Message string          `json:"message"`
	Code    string          `json:"code"`
	Data    json.RawMessage `json:"data"`
}

// APIError 是接口返回的错误。
type APIError struct {
	Status  int
	Message string
	Code    string
	Path    string
}

func (e *APIError) Error() string {
	msg := e.Message
	if msg == "" {
		msg = http.StatusText(e.Status)
	}
	hint := ""
	switch e.Status {
	case http.StatusUnauthorized:
		hint = "（访问令牌无效或已过期）"
	case http.StatusForbidden:
		hint = "（访问令牌权限不足：需要 channel:operate 与 option:write）"
	}
	if e.Code != "" {
		return fmt.Sprintf("子站接口 %s 返回 %d %s：%s%s", e.Path, e.Status, e.Code, msg, hint)
	}
	return fmt.Sprintf("子站接口 %s 返回 %d：%s%s", e.Path, e.Status, msg, hint)
}

// IsAuthError 报告错误是否为认证/授权问题（重试也不会好）。
func IsAuthError(err error) bool {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.Status == http.StatusUnauthorized || apiErr.Status == http.StatusForbidden
	}
	return false
}

func (c *Client) do(ctx context.Context, method, path string, body any, auth bool) (*envelope, error) {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "newapi-sync")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if auth {
		req.Header.Set("Authorization", "Bearer "+c.token)
		if c.userID > 0 {
			// 旧版本 NewAPI 用这个请求头识别访问令牌的所属用户；新版本会忽略它。
			req.Header.Set("New-Api-User", strconv.Itoa(c.userID))
		}
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求子站接口 %s 失败：%w", path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, fmt.Errorf("读取子站接口 %s 响应失败：%w", path, err)
	}
	var env envelope
	jsonErr := json.Unmarshal(raw, &env)
	if resp.StatusCode >= 300 {
		e := &APIError{Status: resp.StatusCode, Path: path}
		if jsonErr == nil {
			e.Message, e.Code = env.Message, env.Code
		} else {
			e.Message = truncate(string(raw), 200)
		}
		return nil, e
	}
	if jsonErr != nil {
		return nil, fmt.Errorf("子站接口 %s 返回的不是 JSON：%s", path, truncate(string(raw), 200))
	}
	if !env.Success {
		return nil, &APIError{Status: resp.StatusCode, Path: path, Message: env.Message, Code: env.Code}
	}
	return &env, nil
}

// Version 读取子站版本号。
func (c *Client) Version(ctx context.Context) (string, error) {
	env, err := c.do(ctx, http.MethodGet, "/api/status", nil, false)
	if err != nil {
		return "", err
	}
	var data struct {
		Version string `json:"version"`
	}
	_ = json.Unmarshal(env.Data, &data)
	return data.Version, nil
}

// FixAbilities 让子站重建能力表并刷新渠道缓存，返回成功与失败的渠道数。
func (c *Client) FixAbilities(ctx context.Context) (success, fails int, err error) {
	env, err := c.do(ctx, http.MethodPost, "/api/channel/fix", nil, true)
	if err != nil {
		return 0, 0, err
	}
	var data struct {
		Success int `json:"success"`
		Fails   int `json:"fails"`
	}
	_ = json.Unmarshal(env.Data, &data)
	return data.Success, data.Fails, nil
}

// UpdateOption 写入一项设置。
func (c *Client) UpdateOption(ctx context.Context, key, value string) error {
	_, err := c.do(ctx, http.MethodPut, "/api/option/", map[string]string{"key": key, "value": value}, true)
	return err
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
