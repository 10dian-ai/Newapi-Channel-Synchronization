package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/10dian-ai/newapi-channel-synchronization/internal/config"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/newapi"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/newapidb"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/notify"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/reconcile"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/snapshot"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/target"
)

// maxDetails 是状态页保留的变更明细条数。
const maxDetails = 50

// worker 负责一个子站。同一子站的同步严格串行。
type worker struct {
	name     string
	cfg      *config.Config
	child    config.ChildConfig
	store    ChildStore
	api      ChildAPI
	log      *slog.Logger
	notifier notify.Notifier
	status   *Status
	dryRun   bool

	mu      sync.Mutex
	pending *snapshot.Master
	wake    chan struct{}

	// 以下字段只在 run 的 goroutine 中访问
	current            *snapshot.Master
	schema             newapidb.Schema
	schemaFP           string
	needSchema         bool
	paused             bool
	abilitiesWritable  bool
	abilitiesReason    string
	pendingRefresh     bool
	consecutiveFailure int
	backoff            *backoff
}

func newWorker(cfg *config.Config, ch Child, log *slog.Logger, notifier notify.Notifier, status *Status) *worker {
	w := &worker{
		name:       ch.Config.Name,
		cfg:        cfg,
		child:      ch.Config,
		store:      ch.Store,
		api:        ch.API,
		log:        log,
		notifier:   notifier,
		status:     status,
		dryRun:     cfg.ChildDryRun(ch.Config),
		wake:       make(chan struct{}, 1),
		needSchema: true,
		backoff:    newBackoff(cfg.Sync.RetryMin.D(), cfg.Sync.RetryMax.D()),
	}
	status.updateChild(w.name, func(c *ChildStatus) {
		c.DryRun = w.dryRun
		c.RefreshChannels = ch.Config.API.RefreshChannels
		c.OptionsVia = ch.Config.API.OptionsVia
	})
	return w
}

// offer 投递新快照。只保留最新的一份；只要其中有一份是全量对账，就按全量处理。
func (w *worker) offer(s *snapshot.Master) {
	w.mu.Lock()
	if w.pending != nil && w.pending.Full && !s.Full {
		cp := *s
		cp.Full = true
		s = &cp
	}
	w.pending = s
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *worker) take() *snapshot.Master {
	w.mu.Lock()
	defer w.mu.Unlock()
	s := w.pending
	w.pending = nil
	return s
}

func (w *worker) run(ctx context.Context) {
	var retry <-chan time.Time
	var retryTimer *time.Timer
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.wake:
		case <-retry:
		}
		if s := w.take(); s != nil {
			w.current = s
		}
		if w.current == nil {
			continue
		}
		err := w.syncOnce(ctx, w.current)
		if ctx.Err() != nil {
			return
		}
		if retryTimer != nil {
			retryTimer.Stop()
			retry = nil
		}
		if err != nil {
			w.onFailure(ctx, err)
			wait := w.backoff.next()
			w.log.Warn("将重试同步", "retry_in", wait)
			retryTimer = time.NewTimer(wait)
			retry = retryTimer.C
			continue
		}
		w.onSuccess(ctx)
	}
}

func (w *worker) onFailure(ctx context.Context, err error) {
	w.consecutiveFailure++
	w.needSchema = true
	w.log.Error("同步失败", "error", err, "consecutive_failures", w.consecutiveFailure)
	w.status.updateChild(w.name, func(c *ChildStatus) {
		c.State = StateError
		c.LastError = err.Error()
		c.ConsecutiveFailures = w.consecutiveFailure
		c.Failures++
	})
	if w.consecutiveFailure == w.cfg.Notify.FailureThreshold {
		w.notifier.Notify(ctx, notify.Event{
			Type:    notify.EventSyncFailed,
			Child:   w.name,
			Message: fmt.Sprintf("连续 %d 次同步失败：%v", w.consecutiveFailure, err),
			Time:    time.Now(),
		})
	}
}

func (w *worker) onSuccess(ctx context.Context) {
	if w.consecutiveFailure >= w.cfg.Notify.FailureThreshold {
		w.notifier.Notify(ctx, notify.Event{
			Type:    notify.EventSyncRecovered,
			Child:   w.name,
			Message: fmt.Sprintf("在连续失败 %d 次后恢复正常", w.consecutiveFailure),
			Time:    time.Now(),
		})
	}
	w.consecutiveFailure = 0
	w.backoff.reset()
	w.status.updateChild(w.name, func(c *ChildStatus) { c.ConsecutiveFailures = 0 })
}

// syncOnce 执行一次完整的同步：差异 → 写入 → 生效 → 校验。
func (w *worker) syncOnce(ctx context.Context, snap *snapshot.Master) error {
	if w.paused {
		return nil
	}
	start := time.Now()
	w.status.updateChild(w.name, func(c *ChildStatus) {
		c.LastAttemptAt = ptrTime(start)
		c.Syncs++
		if c.State != StateError {
			c.State = StateSyncing
		}
	})

	if snap.Full || w.needSchema {
		if paused, err := w.refreshSchema(ctx); err != nil {
			return err
		} else if paused {
			return nil
		}
	}

	syncOptions := w.cfg.Options.IsEnabled()
	cp := reconcile.BuildColumnPlan(snap.Channels.Columns, w.schema.Channels, w.cfg.Channels.OwnedColumns())
	diffOpt := reconcile.DiffOptions{
		StatusMode:    w.cfg.Channels.StatusMode,
		DeleteMissing: w.cfg.Sync.ShouldDeleteMissing(),
		SyncOptions:   syncOptions && len(w.schema.Options) > 0,
	}
	state, err := w.store.Load(ctx, w.schema, diffOpt.SyncOptions)
	if err != nil {
		return fmt.Errorf("读取子站失败：%w", err)
	}
	plan := reconcile.Diff(snap, state, cp, diffOpt)
	for _, warn := range plan.Warnings {
		w.log.Warn(warn)
	}
	if len(plan.OrphanOptions) > 0 && snap.Full {
		w.log.Info("子站有主站没有的分组设置项，已保留不动", "keys", plan.OrphanOptions)
	}

	if plan.Empty() && !w.pendingRefresh {
		w.markSynced(snap, plan, state, false)
		return nil
	}

	details := plan.Details()
	w.log.Info("计算出需要同步的变更", "plan", plan.Summary(), "dry_run", w.dryRun)
	for i, d := range details {
		if i >= 20 {
			w.log.Info(fmt.Sprintf("……另有 %d 项变更", len(details)-20))
			break
		}
		w.log.Info("  " + d)
	}

	if w.dryRun {
		w.status.updateChild(w.name, func(c *ChildStatus) {
			c.State = StateDryRun
			c.LastPlan = "（演练，未写入）" + plan.Summary()
			c.LastDetails = limit(details, maxDetails)
			c.LastWarnings = plan.Warnings
			c.LastError = ""
		})
		return nil
	}

	if plan.ChannelsChanged() && !w.abilitiesWritable && w.child.API.RefreshChannels != config.RefreshFix {
		return fmt.Errorf("无法安全重建子站能力表（%s），请为该子站配置管理接口并使用 refresh_channels: fix", w.abilitiesReason)
	}

	optionsViaDB := w.child.API.OptionsVia == config.OptionsViaDB
	res, err := w.store.Apply(ctx, plan, w.schema, target.ApplyOptions{
		RebuildAbilities: w.abilitiesWritable,
		WriteOptions:     optionsViaDB,
	})
	if err != nil {
		return err
	}
	if plan.ChannelsChanged() && w.child.API.RefreshChannels == config.RefreshFix {
		w.pendingRefresh = true
	}

	// 让改动在子站立即生效
	if w.pendingRefresh {
		if err := w.refreshChannels(ctx); err != nil {
			return err
		}
	}
	if !optionsViaDB && len(plan.Options) > 0 {
		for _, o := range plan.Options {
			if err := w.api.UpdateOption(ctx, o.Key, o.Value); err != nil {
				return fmt.Errorf("通过子站接口写入设置 %s 失败：%w", o.Key, err)
			}
			res.Options++
		}
	}

	// 写后校验：重新读取子站，确认已经与主站一致
	after, err := w.store.Load(ctx, w.schema, diffOpt.SyncOptions)
	if err != nil {
		return fmt.Errorf("写后校验读取子站失败：%w", err)
	}
	if remain := reconcile.Diff(snap, after, cp, diffOpt); !remain.Empty() {
		return fmt.Errorf("写后校验未通过，仍有差异：%s", remain.Summary())
	}

	w.status.updateChild(w.name, func(c *ChildStatus) {
		c.Inserted += int64(res.Inserted)
		c.Updated += int64(res.Updated)
		c.Deleted += int64(res.Deleted)
		c.OptionWrites += int64(res.Options)
		c.LastDetails = limit(details, maxDetails)
		c.LastChangeAppliedAt = ptrTime(time.Now())
	})
	w.markSynced(snap, plan, after, true)
	w.log.Info("同步完成并已校验", "plan", plan.Summary(), "lag", lagOf(snap).Truncate(time.Millisecond))
	return nil
}

func (w *worker) refreshChannels(ctx context.Context) error {
	if w.api == nil {
		w.pendingRefresh = false
		return nil
	}
	ok, fails, err := w.api.FixAbilities(ctx)
	if err != nil {
		if newapi.IsAuthError(err) {
			return fmt.Errorf("刷新子站渠道缓存失败（请检查访问令牌）：%w", err)
		}
		return fmt.Errorf("刷新子站渠道缓存失败：%w", err)
	}
	w.pendingRefresh = false
	if fails > 0 {
		w.log.Warn("子站重建能力表时有渠道失败", "success", ok, "fails", fails)
	} else {
		w.log.Debug("子站已重建能力表并刷新缓存", "channels", ok)
	}
	return nil
}

// refreshSchema 重新读取子站结构，发现结构变化（通常是子站升级）时按配置处理。
func (w *worker) refreshSchema(ctx context.Context) (paused bool, err error) {
	schema, err := w.store.Schema(ctx)
	if err != nil {
		return false, fmt.Errorf("读取子站表结构失败：%w", err)
	}
	fp := schema.Fingerprint()
	version := ""
	if w.api != nil {
		if v, verr := w.api.Version(ctx); verr == nil {
			version = v
		}
	}
	if w.schemaFP != "" && fp != w.schemaFP {
		msg := fmt.Sprintf("子站表结构发生变化（可能刚升级，版本 %s），已按新结构重新计算字段归属", orUnknown(version))
		if w.cfg.Sync.OnSchemaChange == config.SchemaChangePause {
			msg = fmt.Sprintf("子站表结构发生变化（可能刚升级，版本 %s），按配置暂停该子站的同步，确认后重启服务即可恢复", orUnknown(version))
		}
		w.notifier.Notify(ctx, notify.Event{Type: notify.EventSchemaChanged, Child: w.name, Message: msg, Time: time.Now()})
		if w.cfg.Sync.OnSchemaChange == config.SchemaChangePause {
			w.paused = true
			w.status.updateChild(w.name, func(c *ChildStatus) { c.State = StatePaused; c.LastError = msg })
			return true, nil
		}
	}
	w.schema = schema
	w.schemaFP = fp
	w.needSchema = false
	w.abilitiesWritable, w.abilitiesReason = target.AbilitiesWritable(schema.Abilities)
	if !w.abilitiesWritable {
		w.log.Warn("同步服务无法自行重建该子站能力表，将完全依赖子站修复接口", "reason", w.abilitiesReason)
	}
	w.status.updateChild(w.name, func(c *ChildStatus) {
		c.SchemaFingerprint = fp
		if version != "" {
			c.Version = version
		}
	})
	return false, nil
}

func (w *worker) markSynced(snap *snapshot.Master, plan *reconcile.Plan, state *snapshot.Child, applied bool) {
	now := time.Now()
	cp := reconcile.BuildColumnPlan(snap.Channels.Columns, w.schema.Channels, w.cfg.Channels.OwnedColumns())
	w.status.updateChild(w.name, func(c *ChildStatus) {
		c.State = StateOK
		c.LastError = ""
		c.LastSuccessAt = ptrTime(now)
		c.Channels = len(state.Channels.Rows)
		c.MissingInChild = cp.MissingInChild
		c.ChildOnlyColumns = cp.ChildOnly
		c.LastWarnings = plan.Warnings
		if applied {
			c.LastLagMillis = lagOf(snap).Milliseconds()
			c.LastPlan = plan.Summary()
		}
	})
}

func lagOf(snap *snapshot.Master) time.Duration {
	from := snap.DetectedAt
	if from.IsZero() {
		from = snap.LoadedAt
	}
	if from.IsZero() {
		return 0
	}
	return time.Since(from)
}

func limit(s []string, n int) []string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func orUnknown(s string) string {
	if s == "" {
		return "未知"
	}
	return s
}
