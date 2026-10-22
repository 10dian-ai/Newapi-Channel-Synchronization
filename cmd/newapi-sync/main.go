// newapi-sync：把一个主 NewAPI 的渠道与分组设置实时同步到一个或多个子 NewAPI。
//
// 用法：
//
//	newapi-sync [-config config.yaml] [run]   启动同步服务（默认）
//	newapi-sync [-config config.yaml] check   检查配置与连接，并演练一次同步（不写入）
//	newapi-sync version                       显示版本
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/10dian-ai/newapi-channel-synchronization/internal/app"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/config"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/httpserver"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/logx"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/version"
)

func main() {
	os.Exit(run())
}

func run() int {
	defaultConfig := os.Getenv("NEWAPI_SYNC_CONFIG")
	if defaultConfig == "" {
		defaultConfig = "config.yaml"
	}
	fs := flag.NewFlagSet("newapi-sync", flag.ContinueOnError)
	configPath := fs.String("config", defaultConfig, "配置文件路径（也可用环境变量 NEWAPI_SYNC_CONFIG）")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "用法：newapi-sync [-config 路径] [run|check|version]\n\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(os.Args[1:]); err != nil {
		return 2
	}
	cmd := "run"
	if fs.NArg() > 0 {
		cmd = fs.Arg(0)
	}

	switch cmd {
	case "version":
		fmt.Printf("newapi-sync %s (commit %s, built %s)\n", version.Version, version.Commit, version.Date)
		return 0
	case "run", "check":
	default:
		fmt.Fprintf(os.Stderr, "未知命令 %q\n", cmd)
		fs.Usage()
		return 2
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	log := logx.New(os.Stderr, cfg.Log.Level, cfg.Log.Format)
	log.Info("newapi-sync", "version", version.Version, "commit", version.Commit)
	for _, warn := range cfg.Warnings() {
		log.Warn("配置提醒：" + warn)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cmd == "check" {
		// 演练：强制所有子站不写入
		t := true
		for i := range cfg.Children {
			cfg.Children[i].DryRun = &t
		}
	}

	a, err := app.Build(ctx, cfg, log)
	if err != nil {
		log.Error("启动失败", "error", err)
		return 1
	}
	defer func() { _ = a.Close() }()

	if cmd == "check" {
		return runCheck(ctx, a)
	}

	errCh := make(chan error, 2)
	if listen := strings.TrimSpace(cfg.HTTP.Listen); listen != "" && listen != "off" {
		maxAge := 3*cfg.Sync.PollInterval.D() + 60*time.Second
		srv := httpserver.New(listen, a.Status, maxAge, log)
		go func() {
			if err := srv.Run(ctx); err != nil {
				errCh <- fmt.Errorf("状态接口：%w", err)
			}
		}()
	}
	go func() { errCh <- a.Engine.Run(ctx) }()

	select {
	case <-ctx.Done():
		// 等引擎退出
		<-errCh
		return 0
	case err := <-errCh:
		if err != nil {
			log.Error("服务异常退出", "error", err)
			return 1
		}
		return 0
	}
}

func runCheck(ctx context.Context, a *app.App) int {
	err := a.Engine.SyncOnce(ctx)
	snap := a.Status.Snapshot()
	fmt.Println()
	fmt.Println("========== 检查结果 ==========")
	fmt.Printf("主站：渠道 %d 条，分组设置 %d 项\n", snap.Master.Channels, snap.Master.Options)
	for _, c := range snap.Children {
		fmt.Printf("\n子站 %s\n", c.Name)
		if c.Version != "" {
			fmt.Printf("  版本：%s\n", c.Version)
		}
		fmt.Printf("  渠道刷新方式：%s，分组设置写入方式：%s\n", c.RefreshChannels, c.OptionsVia)
		if len(c.MissingInChild) > 0 {
			fmt.Printf("  主站有、子站没有的列（无法同步，子站版本较旧）：%s\n", strings.Join(c.MissingInChild, ", "))
		}
		if len(c.ChildOnlyColumns) > 0 {
			fmt.Printf("  子站独有的列（保持子站自己的值）：%s\n", strings.Join(c.ChildOnlyColumns, ", "))
		}
		if c.LastError != "" {
			fmt.Printf("  错误：%s\n", c.LastError)
			continue
		}
		if c.LastPlan == "" {
			fmt.Println("  已与主站一致，无需变更")
			continue
		}
		fmt.Printf("  %s\n", c.LastPlan)
		for _, d := range c.LastDetails {
			fmt.Printf("    - %s\n", d)
		}
		for _, w := range c.LastWarnings {
			fmt.Printf("  注意：%s\n", w)
		}
	}
	if os.Getenv("NEWAPI_SYNC_CHECK_JSON") == "1" {
		b, _ := json.MarshalIndent(snap, "", "  ")
		fmt.Println(string(b))
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n检查未通过：%v\n", err)
		return 1
	}
	fmt.Println("\n检查通过（以上为演练，未写入任何数据）。用 run 命令启动即可开始同步。")
	return 0
}
