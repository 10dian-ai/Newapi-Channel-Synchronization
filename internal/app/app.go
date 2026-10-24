// Package app 负责组装：打开数据库、做安全检查、创建引擎。
// main 只负责命令行与信号处理，具体部件都在这里接起来，方便测试与替换。
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/10dian-ai/newapi-channel-synchronization/internal/config"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/dbx"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/engine"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/newapi"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/newapidb"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/notify"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/pricing"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/source"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/target"
)

// App 是组装好的服务。
type App struct {
	Engine  *engine.Engine
	Status  *engine.Status
	closers []func() error
}

// Close 关闭所有数据库连接。
func (a *App) Close() error {
	var errs []error
	for i := len(a.closers) - 1; i >= 0; i-- {
		errs = append(errs, a.closers[i]())
	}
	return errors.Join(errs...)
}

// Build 打开主库（只读）与各子库，做身份检查，然后创建引擎。
func Build(ctx context.Context, cfg *config.Config, log *slog.Logger) (*App, error) {
	a := &App{Status: engine.NewStatus(cfg.Master.Name)}
	ok := false
	defer func() {
		if !ok {
			_ = a.Close()
		}
	}()

	matcher := newapidb.OptionMatcher{}
	if cfg.Options.IsEnabled() {
		matcher = newapidb.OptionMatcher{Keys: cfg.Options.Keys, Prefixes: cfg.Options.KeyPrefixes}
	}
	pricingMatcher := newapidb.OptionMatcher{}
	if cfg.Pricing.IsEnabled() {
		pricingMatcher = newapidb.OptionMatcher{Keys: pricing.ReadKeys()}
	}

	masterDB, err := dbx.Open(ctx, dbx.Options{
		Type:         cfg.Master.Database.Type,
		DSN:          cfg.Master.Database.DSN,
		MaxOpenConns: cfg.Master.Database.MaxOpenConns,
		ReadOnly:     true,
	})
	if err != nil {
		return nil, fmt.Errorf("主站：%w", err)
	}
	a.closers = append(a.closers, masterDB.Close)
	masterID, err := masterDB.Identities(ctx)
	if err != nil {
		return nil, fmt.Errorf("主站：%w", err)
	}
	log.Info("已连接主站（只读）", "type", masterDB.Dialect.Name())

	master, err := source.New(masterDB, matcher, pricingMatcher, cfg.Channels.OwnedColumns())
	if err != nil {
		return nil, err
	}
	if _, err := master.RefreshSchema(ctx); err != nil {
		return nil, fmt.Errorf("主站：%w", err)
	}

	var children []engine.Child
	for _, ch := range cfg.EnabledChildren() {
		db, err := dbx.Open(ctx, dbx.Options{
			Type:         ch.Database.Type,
			DSN:          ch.Database.DSN,
			MaxOpenConns: ch.Database.MaxOpenConns,
		})
		if err != nil {
			return nil, fmt.Errorf("子站 %s：%w", ch.Name, err)
		}
		a.closers = append(a.closers, db.Close)
		// 防呆：子站绝不能指向主库。无法确认时同样拒绝启动。
		childID, err := db.Identities(ctx)
		if err != nil {
			return nil, fmt.Errorf("子站 %s：%w", ch.Name, err)
		}
		if db.Dialect.Name() == masterDB.Dialect.Name() {
			same, err := dbx.SameDatabase(masterID, childID)
			if err != nil {
				return nil, fmt.Errorf("子站 %s：%w", ch.Name, err)
			}
			if same {
				return nil, fmt.Errorf("子站 %s：%w", ch.Name, target.ErrSameDatabase)
			}
		}
		store := target.New(db, matcher, pricingMatcher)
		if _, err := store.Schema(ctx); err != nil {
			return nil, fmt.Errorf("子站 %s：%w", ch.Name, err)
		}
		child := engine.Child{Config: ch, Store: store}
		if ch.API.Configured() {
			child.API = newapi.New(ch.API.BaseURL, ch.API.AccessToken, ch.API.UserID, ch.API.Timeout.D())
		}
		log.Info("已连接子站", "child", ch.Name, "type", db.Dialect.Name(),
			"api", ch.API.Configured(), "refresh_channels", ch.API.RefreshChannels, "options_via", ch.API.OptionsVia,
			"dry_run", cfg.ChildDryRun(ch))
		children = append(children, child)
	}

	a.Engine = engine.New(cfg, master, children, log, notify.New(cfg.Notify.WebhookURL, log), a.Status)
	ok = true
	return a, nil
}
