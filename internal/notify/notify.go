// Package notify 发送告警。目前支持通用 Webhook（JSON POST），可以对接
// 企业微信/飞书/钉钉机器人的中转、Uptime Kuma、n8n 等。
package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// Event 是一条告警事件。
type Event struct {
	Type    string    `json:"type"` // sync_failed / sync_recovered / schema_changed / paused
	Child   string    `json:"child,omitempty"`
	Message string    `json:"message"`
	Time    time.Time `json:"time"`
}

// 事件类型。
const (
	EventSyncFailed    = "sync_failed"
	EventSyncRecovered = "sync_recovered"
	EventSchemaChanged = "schema_changed"
	EventPaused        = "paused"
)

// Notifier 发送告警。
type Notifier interface {
	Notify(ctx context.Context, e Event)
}

// New 根据配置创建通知器；未配置时返回只写日志的通知器。
func New(webhookURL string, log *slog.Logger) Notifier {
	if webhookURL == "" {
		return logOnly{log: log}
	}
	return &webhook{url: webhookURL, log: log, client: &http.Client{Timeout: 10 * time.Second}}
}

type logOnly struct{ log *slog.Logger }

func (l logOnly) Notify(_ context.Context, e Event) {
	l.log.Warn("告警", "type", e.Type, "child", e.Child, "message", e.Message)
}

type webhook struct {
	url    string
	log    *slog.Logger
	client *http.Client
}

func (w *webhook) Notify(ctx context.Context, e Event) {
	w.log.Warn("告警", "type", e.Type, "child", e.Child, "message", e.Message)
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	body, _ := json.Marshal(map[string]any{
		"type":    e.Type,
		"child":   e.Child,
		"message": e.Message,
		"time":    e.Time.Format(time.RFC3339),
		// text 字段方便直接对接只认 text 的机器人
		"text": fmt.Sprintf("[newapi-sync] %s %s：%s", e.Type, e.Child, e.Message),
	})
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, bytes.NewReader(body))
	if err != nil {
		w.log.Error("发送告警失败", "error", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.client.Do(req)
	if err != nil {
		w.log.Error("发送告警失败", "error", err)
		return
	}
	_ = resp.Body.Close()
	if resp.StatusCode >= 300 {
		w.log.Error("发送告警失败", "status", resp.StatusCode)
	}
}
