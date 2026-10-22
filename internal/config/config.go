// Package config 负责加载、补全默认值并校验同步服务的配置。
//
// 配置文件为 YAML，支持 ${VAR} 与 ${VAR:-默认值} 形式的环境变量展开，
// 方便把数据库密码、访问令牌等敏感信息放在环境变量或 .env 文件里。
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// 渠道状态同步模式。
const (
	// StatusModeSmart：主站手动禁用会同步到子站；子站自己自动禁用的渠道保持禁用；
	// 主站的自动禁用属于主站自身的运行时状态，不同步。
	StatusModeSmart = "smart"
	// StatusModeMirror：子站渠道状态与主站完全一致。
	StatusModeMirror = "mirror"
)

// 子站渠道缓存刷新方式。
const (
	// RefreshFix：写库后调用子站 POST /api/channel/fix，由子站按自身版本逻辑重建能力表并刷新内存缓存。
	RefreshFix = "fix"
	// RefreshNone：只写库，等待子站按 SYNC_FREQUENCY 自行刷新（或子站未开启内存缓存）。
	RefreshNone = "none"
)

// 分组设置（options）写入方式。
const (
	OptionsViaAPI = "api" // 走子站管理接口，立即生效并经过子站自身的校验
	OptionsViaDB  = "db"  // 直接写子库，等子站 SYNC_FREQUENCY 周期后生效
)

// 子站表结构变化时的处理方式。
const (
	SchemaChangeContinue = "continue"
	SchemaChangePause    = "pause"
)

// DefaultChildOwnedColumns 是默认由子站自己维护、同步时永不覆盖的渠道字段（运行时数据）。
var DefaultChildOwnedColumns = []string{
	"used_quota",
	"balance",
	"balance_updated_time",
	"response_time",
	"test_time",
	"other_info",
}

// DefaultOptionKeys 是默认同步的分组相关设置项。
var DefaultOptionKeys = []string{
	"GroupRatio",
	"GroupGroupRatio",
	"UserUsableGroups",
	"AutoGroups",
	"DefaultUseAutoGroup",
}

// DefaultOptionKeyPrefixes 是默认同步的分组相关设置前缀（新版本以 group_ratio_setting.* 存储）。
var DefaultOptionKeyPrefixes = []string{
	"group_ratio_setting.",
}

// Config 是整个服务的配置。
type Config struct {
	Master   MasterConfig   `yaml:"master"`
	Children []ChildConfig  `yaml:"children"`
	Sync     SyncConfig     `yaml:"sync"`
	Channels ChannelsConfig `yaml:"channels"`
	Options  OptionsConfig  `yaml:"options"`
	HTTP     HTTPConfig     `yaml:"http"`
	Log      LogConfig      `yaml:"log"`
	Notify   NotifyConfig   `yaml:"notify"`
}

// DatabaseConfig 描述一个 NewAPI 数据库连接。DSN 与 NewAPI 的 SQL_DSN 写法一致。
type DatabaseConfig struct {
	// Type 可选 mysql / postgres；留空时根据 DSN 自动识别。
	Type string `yaml:"type"`
	DSN  string `yaml:"dsn"`
	// MaxOpenConns 连接池上限，默认 4。
	MaxOpenConns int `yaml:"max_open_conns"`
}

// MasterConfig 是主站配置。主站只读，服务不会向主库写入任何数据。
type MasterConfig struct {
	Name     string         `yaml:"name"`
	Database DatabaseConfig `yaml:"database"`
}

// ChildConfig 是一个子站的配置。
type ChildConfig struct {
	Name     string         `yaml:"name"`
	Enabled  *bool          `yaml:"enabled"`
	Database DatabaseConfig `yaml:"database"`
	API      APIConfig      `yaml:"api"`
	// DryRun 为 nil 时继承 sync.dry_run。
	DryRun *bool `yaml:"dry_run"`
}

// IsEnabled 报告子站是否启用（默认启用）。
func (c ChildConfig) IsEnabled() bool { return c.Enabled == nil || *c.Enabled }

// APIConfig 是子站管理接口配置，用于让改动立即生效。
type APIConfig struct {
	// BaseURL 子站地址，例如 https://child.example.com。留空表示不使用管理接口。
	BaseURL string `yaml:"base_url"`
	// AccessToken 子站管理员的访问令牌，需要 channel:operate 与 option:write 权限。
	AccessToken string `yaml:"access_token"`
	// UserID 旧版本 NewAPI 需要 New-Api-User 请求头时填写，新版本可留空。
	UserID  int      `yaml:"user_id"`
	Timeout Duration `yaml:"timeout"`
	// RefreshChannels 渠道写入后的刷新方式：fix / none。默认：配置了 API 时为 fix。
	RefreshChannels string `yaml:"refresh_channels"`
	// OptionsVia 分组设置写入方式：api / db。默认：配置了 API 时为 api。
	OptionsVia string `yaml:"options_via"`
}

// Configured 报告是否配置了管理接口。
func (a APIConfig) Configured() bool { return strings.TrimSpace(a.BaseURL) != "" }

// SyncConfig 控制同步节奏与行为。
type SyncConfig struct {
	// PollInterval 主库变更探测间隔，默认 1s。探测只执行一条校验和查询，开销很小。
	PollInterval Duration `yaml:"poll_interval"`
	// Debounce 发现变更后等待多久再读取完整数据，用于合并连续修改，默认 300ms。
	Debounce Duration `yaml:"debounce"`
	// ReconcileInterval 全量对账间隔，默认 5m。
	ReconcileInterval Duration `yaml:"reconcile_interval"`
	// RetryMin / RetryMax 失败重试的退避区间，默认 2s ~ 1m。
	RetryMin Duration `yaml:"retry_min"`
	RetryMax Duration `yaml:"retry_max"`
	// DryRun 只计算并记录差异，不写入子站。
	DryRun bool `yaml:"dry_run"`
	// OnSchemaChange 子站表结构变化（通常是升级）后的处理：continue / pause，默认 continue。
	OnSchemaChange string `yaml:"on_schema_change"`
	// DeleteMissing 删除主站已不存在的渠道，默认 true。
	DeleteMissing *bool `yaml:"delete_missing"`
	// DeleteGuardRatio / DeleteGuardMin 防误删：一次同步要删除的渠道数同时达到
	// DeleteGuardMin 条、并超过子站渠道总数的 DeleteGuardRatio 时，跳过删除并告警。
	// 默认 0.5 与 5；DeleteGuardRatio 设为 1 即关闭（例如首次接入、确认要清理子站旧渠道时）。
	DeleteGuardRatio *float64 `yaml:"delete_guard_ratio"`
	DeleteGuardMin   int      `yaml:"delete_guard_min"`
	// Timeout 单个子站一次同步（含写入、刷新、校验）的最长时间，默认 2m。
	Timeout Duration `yaml:"timeout"`
}

// GuardRatio 返回防误删比例。
func (s SyncConfig) GuardRatio() float64 {
	if s.DeleteGuardRatio == nil {
		return 0.5
	}
	return *s.DeleteGuardRatio
}

// ShouldDeleteMissing 报告是否删除子站多余的渠道。
func (s SyncConfig) ShouldDeleteMissing() bool { return s.DeleteMissing == nil || *s.DeleteMissing }

// ChannelsConfig 控制渠道字段归属。
type ChannelsConfig struct {
	// ChildOwnedColumns 由子站自己维护的字段；留空使用默认列表。
	ChildOwnedColumns []string `yaml:"child_owned_columns"`
	// ExtraChildOwnedColumns 在默认列表基础上追加的字段。
	ExtraChildOwnedColumns []string `yaml:"extra_child_owned_columns"`
	// StatusMode 渠道状态同步模式：smart / mirror，默认 smart。
	StatusMode string `yaml:"status_mode"`
	// ExcludeTypes 不参与同步的渠道类型（NewAPI 的渠道类型编号）。这些类型的渠道
	// 在子站完全由子站自己管理：不新增、不修改、不删除。
	ExcludeTypes []int `yaml:"exclude_types"`
}

// Excluded 报告某个渠道类型是否不参与同步。
func (c ChannelsConfig) Excluded(t int64) bool {
	for _, x := range c.ExcludeTypes {
		if int64(x) == t {
			return true
		}
	}
	return false
}

// OwnedColumns 返回最终生效的子站自有字段列表。
func (c ChannelsConfig) OwnedColumns() []string {
	base := c.ChildOwnedColumns
	if len(base) == 0 {
		base = DefaultChildOwnedColumns
	}
	out := make([]string, 0, len(base)+len(c.ExtraChildOwnedColumns))
	seen := map[string]bool{}
	for _, col := range append(append([]string{}, base...), c.ExtraChildOwnedColumns...) {
		col = strings.ToLower(strings.TrimSpace(col))
		if col == "" || seen[col] {
			continue
		}
		seen[col] = true
		out = append(out, col)
	}
	return out
}

// OptionsConfig 控制同步哪些分组设置。
type OptionsConfig struct {
	// Enabled 是否同步分组设置，默认 true。
	Enabled *bool `yaml:"enabled"`
	// Keys 精确匹配的 option key；留空使用默认列表。
	Keys []string `yaml:"keys"`
	// KeyPrefixes 前缀匹配的 option key；留空使用默认列表。
	KeyPrefixes []string `yaml:"key_prefixes"`
}

// IsEnabled 报告是否同步分组设置。
func (o OptionsConfig) IsEnabled() bool { return o.Enabled == nil || *o.Enabled }

// HTTPConfig 是状态接口配置。
type HTTPConfig struct {
	// Listen 监听地址，默认 :8090；设为 "off" 关闭。
	Listen string `yaml:"listen"`
}

// LogConfig 是日志配置。
type LogConfig struct {
	Level  string `yaml:"level"`  // debug / info / warn / error，默认 info
	Format string `yaml:"format"` // text / json，默认 text
}

// NotifyConfig 是告警配置。
type NotifyConfig struct {
	// WebhookURL 收到 JSON POST 的告警地址，留空关闭。
	WebhookURL string `yaml:"webhook_url"`
	// FailureThreshold 连续失败多少次后告警，默认 3。
	FailureThreshold int `yaml:"failure_threshold"`
}

// Duration 支持在 YAML 中写 "1s"、"300ms"、"5m"。
type Duration time.Duration

// UnmarshalYAML 实现 yaml.Unmarshaler。
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var raw string
	if err := node.Decode(&raw); err != nil {
		return err
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		*d = 0
		return nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("无法解析时长 %q：%w", raw, err)
	}
	*d = Duration(parsed)
	return nil
}

// D 返回 time.Duration。
func (d Duration) D() time.Duration { return time.Duration(d) }

// Load 读取配置文件，展开环境变量，补全默认值并校验。
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件 %s 失败：%w", path, err)
	}
	return Parse(raw)
}

// Parse 解析配置内容。
func Parse(raw []byte) (*Config, error) {
	expanded, err := ExpandEnv(string(raw))
	if err != nil {
		return nil, err
	}
	cfg := &Config{}
	dec := yaml.NewDecoder(bytes.NewReader([]byte(expanded)))
	dec.KnownFields(true) // 拼错的字段直接报错，避免配置悄悄不生效
	if err := dec.Decode(cfg); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("配置文件为空")
		}
		return nil, fmt.Errorf("解析配置失败：%w", err)
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

var envPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}`)

// ExpandEnv 展开 ${VAR} 与 ${VAR:-默认值}。未设置且没有默认值的变量会报错，
// 防止因为忘记设置环境变量而连到错误的数据库。只识别 ${...} 形式，
// 所以密码里出现的单独 $ 不受影响。
func ExpandEnv(s string) (string, error) {
	var missing []string
	out := envPattern.ReplaceAllStringFunc(s, func(m string) string {
		parts := envPattern.FindStringSubmatch(m)
		name, hasDefault, def := parts[1], parts[2] != "", parts[3]
		if v, ok := os.LookupEnv(name); ok && v != "" {
			return v
		}
		if hasDefault {
			return def
		}
		missing = append(missing, name)
		return ""
	})
	if len(missing) > 0 {
		return "", fmt.Errorf("配置引用了未设置的环境变量：%s", strings.Join(missing, ", "))
	}
	return out, nil
}

// ApplyDefaults 补全默认值。
func (c *Config) ApplyDefaults() {
	if c.Master.Name == "" {
		c.Master.Name = "master"
	}
	if c.Master.Database.MaxOpenConns <= 0 {
		c.Master.Database.MaxOpenConns = 4
	}
	for i := range c.Children {
		ch := &c.Children[i]
		ch.Name = strings.TrimSpace(ch.Name)
		if ch.Database.MaxOpenConns <= 0 {
			ch.Database.MaxOpenConns = 4
		}
		ch.API.BaseURL = strings.TrimRight(strings.TrimSpace(ch.API.BaseURL), "/")
		if ch.API.Timeout <= 0 {
			ch.API.Timeout = Duration(15 * time.Second)
		}
		if ch.API.RefreshChannels == "" {
			if ch.API.Configured() {
				ch.API.RefreshChannels = RefreshFix
			} else {
				ch.API.RefreshChannels = RefreshNone
			}
		}
		if ch.API.OptionsVia == "" {
			if ch.API.Configured() {
				ch.API.OptionsVia = OptionsViaAPI
			} else {
				ch.API.OptionsVia = OptionsViaDB
			}
		}
	}
	s := &c.Sync
	if s.PollInterval <= 0 {
		s.PollInterval = Duration(time.Second)
	}
	if s.Debounce < 0 {
		s.Debounce = 0
	} else if s.Debounce == 0 {
		s.Debounce = Duration(300 * time.Millisecond)
	}
	if s.ReconcileInterval <= 0 {
		s.ReconcileInterval = Duration(5 * time.Minute)
	}
	if s.RetryMin <= 0 {
		s.RetryMin = Duration(2 * time.Second)
	}
	if s.RetryMax <= 0 {
		s.RetryMax = Duration(time.Minute)
	}
	if s.OnSchemaChange == "" {
		s.OnSchemaChange = SchemaChangeContinue
	}
	if s.DeleteGuardMin <= 0 {
		s.DeleteGuardMin = 5
	}
	if s.Timeout <= 0 {
		s.Timeout = Duration(2 * time.Minute)
	}
	if c.Channels.StatusMode == "" {
		c.Channels.StatusMode = StatusModeSmart
	}
	if len(c.Options.Keys) == 0 {
		c.Options.Keys = append([]string{}, DefaultOptionKeys...)
	}
	if len(c.Options.KeyPrefixes) == 0 {
		c.Options.KeyPrefixes = append([]string{}, DefaultOptionKeyPrefixes...)
	}
	if c.HTTP.Listen == "" {
		c.HTTP.Listen = ":8090"
	}
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if c.Log.Format == "" {
		c.Log.Format = "text"
	}
	if c.Notify.FailureThreshold <= 0 {
		c.Notify.FailureThreshold = 3
	}
}

var namePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)

// Validate 校验配置，尽量在启动前就发现会导致误操作的配置。
func (c *Config) Validate() error {
	var errs []string
	add := func(format string, args ...any) { errs = append(errs, fmt.Sprintf(format, args...)) }

	masterDSN := strings.TrimSpace(c.Master.Database.DSN)
	if masterDSN == "" {
		add("master.database.dsn 不能为空")
	}
	if err := validateDBType(c.Master.Database.Type); err != nil {
		add("master.database.type：%v", err)
	}
	if len(c.Children) == 0 {
		add("至少需要配置一个子站（children）")
	}
	names := map[string]bool{}
	enabled := 0
	for i, ch := range c.Children {
		label := fmt.Sprintf("children[%d]", i)
		if ch.Name == "" {
			add("%s.name 不能为空", label)
		} else if !namePattern.MatchString(ch.Name) {
			add("%s.name %q 只能包含字母、数字、点、下划线和短横线", label, ch.Name)
		} else if names[ch.Name] {
			add("子站名称 %q 重复", ch.Name)
		}
		names[ch.Name] = true
		if ch.IsEnabled() {
			enabled++
		}
		dsn := strings.TrimSpace(ch.Database.DSN)
		if dsn == "" {
			add("%s.database.dsn 不能为空", label)
		} else if dsn == masterDSN {
			add("%s 的数据库与主站相同：主库必须保持只读，不能作为子站", label)
		}
		if err := validateDBType(ch.Database.Type); err != nil {
			add("%s.database.type：%v", label, err)
		}
		if ch.API.Configured() {
			u, err := url.Parse(ch.API.BaseURL)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
				add("%s.api.base_url %q 不是有效的 http(s) 地址", label, ch.API.BaseURL)
			}
			if strings.TrimSpace(ch.API.AccessToken) == "" {
				add("%s.api.access_token 不能为空（配置了 base_url 时必填）", label)
			}
		}
		switch ch.API.RefreshChannels {
		case RefreshFix:
			if !ch.API.Configured() {
				add("%s.api.refresh_channels=fix 需要配置 api.base_url", label)
			}
		case RefreshNone:
		default:
			add("%s.api.refresh_channels 只能是 fix 或 none", label)
		}
		switch ch.API.OptionsVia {
		case OptionsViaAPI:
			if !ch.API.Configured() {
				add("%s.api.options_via=api 需要配置 api.base_url", label)
			}
		case OptionsViaDB:
		default:
			add("%s.api.options_via 只能是 api 或 db", label)
		}
	}
	if len(c.Children) > 0 && enabled == 0 {
		add("所有子站都被禁用了")
	}

	s := c.Sync
	if s.PollInterval.D() < 200*time.Millisecond {
		add("sync.poll_interval 不能小于 200ms")
	}
	if s.ReconcileInterval.D() < 10*time.Second {
		add("sync.reconcile_interval 不能小于 10s")
	}
	if s.RetryMax < s.RetryMin {
		add("sync.retry_max 不能小于 sync.retry_min")
	}
	if s.OnSchemaChange != SchemaChangeContinue && s.OnSchemaChange != SchemaChangePause {
		add("sync.on_schema_change 只能是 continue 或 pause")
	}
	if r := s.GuardRatio(); r <= 0 || r > 1 {
		add("sync.delete_guard_ratio 必须在 (0, 1] 之间（设为 1 表示关闭防误删）")
	}
	if s.Timeout.D() < 10*time.Second {
		add("sync.timeout 不能小于 10s")
	}

	switch c.Channels.StatusMode {
	case StatusModeSmart, StatusModeMirror:
	default:
		add("channels.status_mode 只能是 smart 或 mirror")
	}
	for _, col := range c.Channels.OwnedColumns() {
		if col == "id" || col == "type" {
			add("channels 字段 %s 不能设为子站自有", col)
		}
	}
	for _, k := range c.Options.Keys {
		if strings.TrimSpace(k) == "" {
			add("options.keys 中不能有空值")
		}
	}
	for _, p := range c.Options.KeyPrefixes {
		if strings.TrimSpace(p) == "" {
			add("options.key_prefixes 中不能有空值（空前缀会匹配全部设置）")
		}
	}
	switch strings.ToLower(c.Log.Level) {
	case "debug", "info", "warn", "error":
	default:
		add("log.level 只能是 debug / info / warn / error")
	}
	switch strings.ToLower(c.Log.Format) {
	case "text", "json":
	default:
		add("log.format 只能是 text 或 json")
	}
	if c.Notify.WebhookURL != "" {
		u, err := url.Parse(c.Notify.WebhookURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
			add("notify.webhook_url 不是有效的 http(s) 地址")
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("配置校验失败：\n  - %s", strings.Join(errs, "\n  - "))
	}
	return nil
}

// Warnings 返回不阻止启动、但很可能不是本意的配置。
func (c *Config) Warnings() []string {
	var out []string
	owned := map[string]bool{}
	for _, col := range c.Channels.OwnedColumns() {
		owned[col] = true
	}
	if len(c.Channels.ChildOwnedColumns) > 0 {
		var missing []string
		for _, col := range DefaultChildOwnedColumns {
			if !owned[col] {
				missing = append(missing, col)
			}
		}
		if len(missing) > 0 {
			out = append(out, fmt.Sprintf("channels.child_owned_columns 会替换默认列表，以下运行时字段将被主站的值覆盖：%s（只想追加请用 extra_child_owned_columns）", strings.Join(missing, ", ")))
		}
	}
	for _, col := range []string{"key", "models", "group", "status", "base_url"} {
		if owned[col] {
			out = append(out, fmt.Sprintf("渠道字段 %s 被设为子站自有，主站对它的修改不会同步到子站", col))
		}
	}
	for _, ch := range c.EnabledChildren() {
		if ch.API.RefreshChannels == RefreshFix {
			continue
		}
		out = append(out, fmt.Sprintf("子站 %s 未使用修复接口刷新：如果它开启了 Redis 或 MEMORY_CACHE_ENABLED，渠道改动要等它的 SYNC_FREQUENCY 周期后才生效", ch.Name))
	}
	return out
}

func validateDBType(t string) error {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case "", "mysql", "postgres", "postgresql":
		return nil
	case "sqlite", "sqlite3":
		return errors.New("第一版暂不支持 SQLite，请使用 MySQL 或 PostgreSQL")
	default:
		return fmt.Errorf("不支持的数据库类型 %q", t)
	}
}

// EnabledChildren 返回启用的子站。
func (c *Config) EnabledChildren() []ChildConfig {
	out := make([]ChildConfig, 0, len(c.Children))
	for _, ch := range c.Children {
		if ch.IsEnabled() {
			out = append(out, ch)
		}
	}
	return out
}

// ChildDryRun 返回某个子站最终是否为演练模式。
func (c *Config) ChildDryRun(ch ChildConfig) bool {
	if ch.DryRun != nil {
		return *ch.DryRun
	}
	return c.Sync.DryRun
}
