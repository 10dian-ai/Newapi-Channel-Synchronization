// Package httpserver 提供健康检查、状态与 Prometheus 指标接口。
package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/10dian-ai/newapi-channel-synchronization/internal/engine"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/version"
)

// Server 是状态接口服务。
type Server struct {
	srv          *http.Server
	status       *engine.Status
	log          *slog.Logger
	healthMaxAge time.Duration
}

// New 创建服务。healthMaxAge 是主站多久没读取成功就判定为不健康。
func New(addr string, status *engine.Status, healthMaxAge time.Duration, log *slog.Logger) *Server {
	s := &Server{status: status, log: log, healthMaxAge: healthMaxAge}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.health)
	mux.HandleFunc("/status", s.statusJSON)
	mux.HandleFunc("/metrics", s.metrics)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		fmt.Fprintf(w, "newapi-sync %s\n\n/healthz  健康检查\n/status   同步状态（JSON）\n/metrics  Prometheus 指标\n", version.Version)
	})
	s.srv = &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return s
}

// Run 启动服务，ctx 结束时优雅关闭。
func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		s.log.Info("状态接口已启动", "listen", s.srv.Addr)
		if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	ok, reason := s.status.MasterHealthy(s.healthMaxAge)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if !ok {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	fmt.Fprintln(w, reason)
}

func (s *Server) statusJSON(w http.ResponseWriter, _ *http.Request) {
	snap := s.status.Snapshot()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	_ = enc.Encode(map[string]any{
		"version": version.Version,
		"status":  snap,
	})
}

func (s *Server) metrics(w http.ResponseWriter, _ *http.Request) {
	snap := s.status.Snapshot()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	var b strings.Builder
	gauge := func(name, help string) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n", name, help, name)
	}
	counter := func(name, help string) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s counter\n", name, help, name)
	}
	ts := func(t *time.Time) float64 {
		if t == nil {
			return 0
		}
		return float64(t.UnixMilli()) / 1000
	}

	healthy, _ := s.status.MasterHealthy(s.healthMaxAge)
	gauge("newapi_sync_master_up", "主站最近是否读取成功（1=是）")
	fmt.Fprintf(&b, "newapi_sync_master_up %d\n", boolInt(healthy))
	gauge("newapi_sync_master_channels", "主站渠道数")
	fmt.Fprintf(&b, "newapi_sync_master_channels %d\n", snap.Master.Channels)
	counter("newapi_sync_master_probes_total", "主站探测次数")
	fmt.Fprintf(&b, "newapi_sync_master_probes_total %d\n", snap.Master.Probes)
	counter("newapi_sync_master_changes_total", "检测到的主站变更次数")
	fmt.Fprintf(&b, "newapi_sync_master_changes_total %d\n", snap.Master.Changes)
	counter("newapi_sync_master_probe_failures_total", "主站读取失败次数")
	fmt.Fprintf(&b, "newapi_sync_master_probe_failures_total %d\n", snap.Master.ProbeFailures)

	gauge("newapi_sync_child_ok", "子站最近一次同步是否成功（1=是）")
	for _, c := range snap.Children {
		fmt.Fprintf(&b, "newapi_sync_child_ok{child=%q} %d\n", c.Name, boolInt(c.State == engine.StateOK || c.State == engine.StateDryRun))
	}
	gauge("newapi_sync_child_last_success_timestamp_seconds", "子站最近一次同步成功的时间")
	for _, c := range snap.Children {
		fmt.Fprintf(&b, "newapi_sync_child_last_success_timestamp_seconds{child=%q} %.3f\n", c.Name, ts(c.LastSuccessAt))
	}
	gauge("newapi_sync_child_lag_seconds", "最近一次变更从主站发现到子站校验通过的耗时")
	for _, c := range snap.Children {
		fmt.Fprintf(&b, "newapi_sync_child_lag_seconds{child=%q} %.3f\n", c.Name, float64(c.LastLagMillis)/1000)
	}
	gauge("newapi_sync_child_consecutive_failures", "子站连续失败次数")
	for _, c := range snap.Children {
		fmt.Fprintf(&b, "newapi_sync_child_consecutive_failures{child=%q} %d\n", c.Name, c.ConsecutiveFailures)
	}
	gauge("newapi_sync_child_channels", "子站渠道数")
	for _, c := range snap.Children {
		fmt.Fprintf(&b, "newapi_sync_child_channels{child=%q} %d\n", c.Name, c.Channels)
	}
	counter("newapi_sync_child_failures_total", "子站同步失败总次数")
	for _, c := range snap.Children {
		fmt.Fprintf(&b, "newapi_sync_child_failures_total{child=%q} %d\n", c.Name, c.Failures)
	}
	counter("newapi_sync_child_writes_total", "写入子站的变更数")
	for _, c := range snap.Children {
		fmt.Fprintf(&b, "newapi_sync_child_writes_total{child=%q,kind=\"insert\"} %d\n", c.Name, c.Inserted)
		fmt.Fprintf(&b, "newapi_sync_child_writes_total{child=%q,kind=\"update\"} %d\n", c.Name, c.Updated)
		fmt.Fprintf(&b, "newapi_sync_child_writes_total{child=%q,kind=\"delete\"} %d\n", c.Name, c.Deleted)
		fmt.Fprintf(&b, "newapi_sync_child_writes_total{child=%q,kind=\"option\"} %d\n", c.Name, c.OptionWrites)
		fmt.Fprintf(&b, "newapi_sync_child_writes_total{child=%q,kind=\"pricing\"} %d\n", c.Name, c.PricingWrites)
	}
	_, _ = w.Write([]byte(b.String()))
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
