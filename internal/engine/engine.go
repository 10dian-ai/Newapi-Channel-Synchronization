// Package engine 把各个部件串起来：
//
//	主库探测 ──变化──▶ 防抖合并 ──▶ 读取完整快照 ──▶ 分发给每个子站 worker
//	                                                  │
//	         每个子站独立：计算差异 → 事务写入 → 让子站立即生效 → 写后校验 → 失败退避重试
//
// 子站之间互不影响；同一个子站的同步串行执行，新快照到来时只保留最新的一份。
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/10dian-ai/newapi-channel-synchronization/internal/config"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/newapidb"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/notify"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/reconcile"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/snapshot"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/target"
)

// MasterSource 是主站数据源（只读）。
type MasterSource interface {
	RefreshSchema(ctx context.Context) (newapidb.Schema, error)
	Probe(ctx context.Context) (string, bool, error)
	Load(ctx context.Context, withOptions bool) (*snapshot.Master, error)
}

// ChildStore 是子站数据库。
type ChildStore interface {
	Schema(ctx context.Context) (newapidb.Schema, error)
	Load(ctx context.Context, schema newapidb.Schema, withOptions bool) (*snapshot.Child, error)
	Apply(ctx context.Context, plan *reconcile.Plan, schema newapidb.Schema, opt target.ApplyOptions) (target.Result, error)
}

// ChildAPI 是子站管理接口。
type ChildAPI interface {
	Version(ctx context.Context) (string, error)
	FixAbilities(ctx context.Context) (int, int, error)
	UpdateOption(ctx context.Context, key, value string) error
}

// Child 是构造 worker 所需的依赖。
type Child struct {
	Config config.ChildConfig
	Store  ChildStore
	API    ChildAPI // 未配置管理接口时为 nil
}

// Engine 是同步引擎。
type Engine struct {
	cfg      *config.Config
	master   MasterSource
	workers  []*worker
	log      *slog.Logger
	notifier notify.Notifier
	status   *Status
}

// New 创建引擎。
func New(cfg *config.Config, master MasterSource, children []Child, log *slog.Logger, notifier notify.Notifier, status *Status) *Engine {
	e := &Engine{cfg: cfg, master: master, log: log, notifier: notifier, status: status}
	for _, ch := range children {
		w := newWorker(cfg, ch, log.With("child", ch.Config.Name), notifier, status)
		e.workers = append(e.workers, w)
	}
	return e
}

// Run 运行直到 ctx 结束。
func (e *Engine) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for _, w := range e.workers {
		wg.Add(1)
		go func(w *worker) {
			defer wg.Done()
			w.run(ctx)
		}(w)
	}
	defer wg.Wait()

	syncOptions := e.cfg.Options.IsEnabled()
	poll := e.cfg.Sync.PollInterval.D()
	e.log.Info("同步服务启动",
		"children", len(e.workers),
		"poll_interval", poll,
		"reconcile_interval", e.cfg.Sync.ReconcileInterval.D(),
		"status_mode", e.cfg.Channels.StatusMode)

	// 首次全量：失败就退避重试，直到成功或退出。
	backoff := newBackoff(e.cfg.Sync.RetryMin.D(), e.cfg.Sync.RetryMax.D())
	var lastChecksum, lastFingerprint string
	for {
		snap, checksum, err := e.loadFull(ctx, syncOptions)
		if err == nil {
			lastChecksum, lastFingerprint = checksum, snap.Fingerprint()
			e.broadcast(snap)
			break
		}
		if ctx.Err() != nil {
			return nil
		}
		wait := backoff.next()
		e.log.Error("读取主站失败，稍后重试", "error", err, "retry_in", wait)
		if !sleepCtx(ctx, wait) {
			return nil
		}
	}

	pollT := time.NewTicker(poll)
	defer pollT.Stop()
	reconT := time.NewTicker(e.cfg.Sync.ReconcileInterval.D())
	defer reconT.Stop()
	probeWarned := false

	for {
		select {
		case <-ctx.Done():
			e.log.Info("同步服务停止")
			return nil

		case <-reconT.C:
			snap, checksum, err := e.loadFull(ctx, syncOptions)
			if err != nil {
				e.log.Error("定期对账读取主站失败", "error", err)
				continue
			}
			lastChecksum, lastFingerprint = checksum, snap.Fingerprint()
			e.status.updateMaster(func(m *MasterStatus) { m.Reconciles++ })
			e.broadcast(snap)

		case <-pollT.C:
			checksum, ok, err := e.master.Probe(ctx)
			now := time.Now()
			e.status.updateMaster(func(m *MasterStatus) { m.Probes++; m.LastProbeAt = ptrTime(now) })
			if err != nil && ok {
				e.markProbeError(err)
				continue
			}
			if !ok {
				// 校验和探测不可用：退回到完整读取并比较内容指纹。
				if err != nil && !probeWarned {
					e.log.Warn("校验和探测不可用，改为每次完整读取主站（功能不受影响，开销略大）", "error", err)
					probeWarned = true
				}
				e.status.updateMaster(func(m *MasterStatus) { m.ProbeMode = "full" })
				snap, err := e.master.Load(ctx, syncOptions)
				if err != nil {
					e.markProbeError(err)
					continue
				}
				e.markMasterOK(snap, "")
				if fp := snap.Fingerprint(); fp != lastFingerprint {
					lastFingerprint = fp
					snap.DetectedAt = now
					e.onChange(snap)
				}
				continue
			}
			e.status.updateMaster(func(m *MasterStatus) {
				m.LastOKAt = ptrTime(now)
				m.LastError = ""
				m.Checksum = checksum
				m.ProbeMode = "checksum"
			})
			if checksum == lastChecksum {
				continue
			}
			// 发现变化：等一小会儿，把管理员连续的几次修改合并成一次同步。
			if !sleepCtx(ctx, e.cfg.Sync.Debounce.D()) {
				return nil
			}
			if latest, ok2, err2 := e.master.Probe(ctx); err2 == nil && ok2 {
				checksum = latest
			}
			snap, err := e.master.Load(ctx, syncOptions)
			if err != nil {
				e.markProbeError(err)
				continue
			}
			lastChecksum = checksum
			snap.Checksum = checksum
			snap.DetectedAt = now
			e.markMasterOK(snap, checksum)
			fp := snap.Fingerprint()
			if fp == lastFingerprint {
				// 只是被忽略的运行时字段之外的无关变化（或改了又改回去），无需同步。
				continue
			}
			lastFingerprint = fp
			e.onChange(snap)
		}
	}
}

func (e *Engine) onChange(snap *snapshot.Master) {
	e.status.updateMaster(func(m *MasterStatus) {
		m.Changes++
		m.LastChangeAt = ptrTime(snap.DetectedAt)
	})
	e.log.Info("主站渠道/分组有变化，开始同步", "channels", len(snap.Channels.Rows), "options", len(snap.Options))
	e.broadcast(snap)
}

func (e *Engine) loadFull(ctx context.Context, withOptions bool) (*snapshot.Master, string, error) {
	if _, err := e.master.RefreshSchema(ctx); err != nil {
		e.markProbeError(err)
		return nil, "", err
	}
	checksum, ok, err := e.master.Probe(ctx)
	if err != nil && ok {
		e.markProbeError(err)
		return nil, "", err
	}
	if !ok {
		checksum = ""
	}
	snap, err := e.master.Load(ctx, withOptions)
	if err != nil {
		e.markProbeError(err)
		return nil, "", err
	}
	now := time.Now()
	snap.Full = true
	snap.Checksum = checksum
	snap.DetectedAt = now
	e.markMasterOK(snap, checksum)
	return snap, checksum, nil
}

func (e *Engine) markMasterOK(snap *snapshot.Master, checksum string) {
	now := time.Now()
	e.status.updateMaster(func(m *MasterStatus) {
		m.LastOKAt = ptrTime(now)
		m.LastError = ""
		m.Channels = len(snap.Channels.Rows)
		m.Options = len(snap.Options)
		m.Fingerprint = snap.Fingerprint()
		if checksum != "" {
			m.Checksum = checksum
		}
	})
}

func (e *Engine) markProbeError(err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	e.status.updateMaster(func(m *MasterStatus) { m.ProbeFailures++; m.LastError = err.Error() })
	e.log.Error("读取主站失败", "error", err)
}

func (e *Engine) broadcast(snap *snapshot.Master) {
	for _, w := range e.workers {
		w.offer(snap)
	}
}

// SyncOnce 对所有子站执行一次同步并等待完成（用于 check 命令的演练）。
func (e *Engine) SyncOnce(ctx context.Context) error {
	snap, _, err := e.loadFull(ctx, e.cfg.Options.IsEnabled())
	if err != nil {
		return err
	}
	var errs []error
	for _, w := range e.workers {
		if err := w.syncOnce(ctx, snap); err != nil {
			errs = append(errs, fmt.Errorf("%s：%w", w.name, err))
		}
	}
	return errors.Join(errs...)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

type backoff struct {
	min, max, cur time.Duration
}

func newBackoff(min, max time.Duration) *backoff { return &backoff{min: min, max: max} }

func (b *backoff) next() time.Duration {
	if b.cur == 0 {
		b.cur = b.min
	} else {
		b.cur *= 2
		if b.cur > b.max {
			b.cur = b.max
		}
	}
	return b.cur
}

func (b *backoff) reset() { b.cur = 0 }
