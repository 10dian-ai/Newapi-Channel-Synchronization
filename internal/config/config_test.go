package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

const minimal = `
master:
  database:
    dsn: "ro:pw@tcp(master:3306)/new-api"
children:
  - name: child-a
    database:
      dsn: "rw:pw@tcp(child-a:3306)/new-api"
    api:
      base_url: "https://child-a.example.com/"
      access_token: "${CHILD_A_TOKEN}"
  - name: child-b
    database:
      dsn: "postgres://rw:pw@child-b:5432/new-api"
`

func TestParseMinimalWithDefaults(t *testing.T) {
	t.Setenv("CHILD_A_TOKEN", "nap_xxx")
	cfg, err := Parse([]byte(minimal))
	if err != nil {
		t.Fatal(err)
	}
	a, b := cfg.Children[0], cfg.Children[1]
	if a.API.AccessToken != "nap_xxx" || a.API.BaseURL != "https://child-a.example.com" {
		t.Errorf("环境变量展开或地址规范化失败：%+v", a.API)
	}
	if a.API.RefreshChannels != RefreshFix || a.API.OptionsVia != OptionsViaAPI {
		t.Errorf("配置了 API 的子站默认应走 fix/api：%+v", a.API)
	}
	if b.API.RefreshChannels != RefreshNone || b.API.OptionsVia != OptionsViaDB {
		t.Errorf("没配 API 的子站默认应走 none/db：%+v", b.API)
	}
	if cfg.Sync.PollInterval.D() != time.Second || cfg.Sync.Debounce.D() != 300*time.Millisecond || cfg.Sync.ReconcileInterval.D() != 5*time.Minute {
		t.Errorf("默认同步节奏不对：%+v", cfg.Sync)
	}
	if !cfg.Sync.ShouldDeleteMissing() || cfg.Channels.StatusMode != StatusModeSmart {
		t.Errorf("默认行为不对")
	}
	if len(cfg.Options.Keys) == 0 || cfg.Options.KeyPrefixes[0] != "group_ratio_setting." {
		t.Errorf("默认分组设置范围不对：%+v", cfg.Options)
	}
	if got := strings.Join(cfg.Channels.OwnedColumns(), ","); !strings.Contains(got, "used_quota") {
		t.Errorf("默认子站自有列：%s", got)
	}
}

func TestMissingEnvIsAnError(t *testing.T) {
	os.Unsetenv("CHILD_A_TOKEN")
	_, err := Parse([]byte(minimal))
	if err == nil || !strings.Contains(err.Error(), "CHILD_A_TOKEN") {
		t.Fatalf("应提示缺少环境变量，got %v", err)
	}
}

func TestEnvDefaultAndDollarInPassword(t *testing.T) {
	out, err := ExpandEnv(`a=${NOPE_X:-fallback} b=p$ss c=$HOME`)
	if err != nil {
		t.Fatal(err)
	}
	if out != `a=fallback b=p$ss c=$HOME` {
		t.Fatalf("只应展开 ${...}：%s", out)
	}
}

func TestValidationCatchesDangerousConfig(t *testing.T) {
	cases := map[string]string{
		"同一个数据库": `
master: {database: {dsn: "u:p@tcp(h:3306)/db"}}
children: [{name: c, database: {dsn: "u:p@tcp(h:3306)/db"}}]`,
		"缺少令牌": `
master: {database: {dsn: "u:p@tcp(m:3306)/db"}}
children: [{name: c, database: {dsn: "u:p@tcp(c:3306)/db"}, api: {base_url: "https://c"}}]`,
		"fix 需要 API": `
master: {database: {dsn: "u:p@tcp(m:3306)/db"}}
children: [{name: c, database: {dsn: "u:p@tcp(c:3306)/db"}, api: {refresh_channels: fix}}]`,
		"名称重复": `
master: {database: {dsn: "u:p@tcp(m:3306)/db"}}
children: [{name: c, database: {dsn: "u:p@tcp(c:3306)/db"}}, {name: c, database: {dsn: "u:p@tcp(d:3306)/db"}}]`,
		"拼错字段": `
master: {database: {dsn: "u:p@tcp(m:3306)/db"}}
children: [{name: c, database: {dsn: "u:p@tcp(c:3306)/db"}}]
sync: {pol_interval: 1s}`,
		"SQLite": `
master: {database: {dsn: "u:p@tcp(m:3306)/db"}}
children: [{name: c, database: {type: sqlite, dsn: "/data/one-api.db"}}]`,
		"空前缀": `
master: {database: {dsn: "u:p@tcp(m:3306)/db"}}
children: [{name: c, database: {dsn: "u:p@tcp(c:3306)/db"}}]
options: {key_prefixes: [""]}`,
	}
	for name, raw := range cases {
		if _, err := Parse([]byte(raw)); err == nil {
			t.Errorf("%s：应校验失败", name)
		}
	}
}

func TestExampleConfigParses(t *testing.T) {
	raw, err := os.ReadFile("../../config.example.yaml")
	if err != nil {
		t.Skip("找不到示例配置")
	}
	for _, k := range []string{"MASTER_DSN", "CHILD1_DSN", "CHILD1_URL", "CHILD1_TOKEN", "CHILD2_DSN"} {
		t.Setenv(k, "x")
	}
	t.Setenv("MASTER_DSN", "ro:pw@tcp(master:3306)/new-api")
	t.Setenv("CHILD1_DSN", "rw:pw@tcp(child1:3306)/new-api")
	t.Setenv("CHILD1_URL", "https://child1.example.com")
	t.Setenv("CHILD2_DSN", "postgres://rw:pw@child2:5432/new-api")
	if _, err := Parse(raw); err != nil {
		t.Fatalf("示例配置应能通过校验：%v", err)
	}
}
