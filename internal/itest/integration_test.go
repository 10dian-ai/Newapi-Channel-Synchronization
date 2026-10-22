// Package itest 是连接真实 MySQL / PostgreSQL 的集成测试。
//
// 设置环境变量后运行（未设置则跳过）：
//
//	NEWAPI_SYNC_TEST_MYSQL="user:pass@tcp(127.0.0.1:3306)/"
//	NEWAPI_SYNC_TEST_POSTGRES="postgres://user:pass@127.0.0.1:5432/postgres?sslmode=disable"
//
// 账号需要有建库、建用户的权限。测试会模拟 NewAPI 的表结构，并故意让主站、子站的结构有版本差异。
package itest

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/10dian-ai/newapi-channel-synchronization/internal/app"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/config"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/dbx"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/newapidb"
)

type dialectEnv struct {
	name     string
	adminDSN func(db string) string // 用管理员账号连接某个库
	roDSN    func(db string) string // 用只读账号连接主库
	setup    func(t *testing.T, admin *sql.DB, master, child1, child2 string)
	ddl      func(newer bool) []string // newer=true 生成"新版本"子站结构
}

func TestMySQL(t *testing.T) {
	base := os.Getenv("NEWAPI_SYNC_TEST_MYSQL")
	if base == "" {
		t.Skip("未设置 NEWAPI_SYNC_TEST_MYSQL")
	}
	cfg, err := mysql.ParseDSN(base)
	if err != nil {
		t.Fatal(err)
	}
	withDB := func(user, pass, db string) string {
		c := cfg.Clone()
		c.User, c.Passwd, c.DBName = user, pass, db
		return c.FormatDSN()
	}
	env := dialectEnv{
		name:     "mysql",
		adminDSN: func(db string) string { return withDB(cfg.User, cfg.Passwd, db) },
		roDSN:    func(db string) string { return withDB("nsync_ro_test", "ro_pw", db) },
		setup: func(t *testing.T, admin *sql.DB, master, child1, child2 string) {
			for _, db := range []string{master, child1, child2} {
				mustExec(t, admin, "DROP DATABASE IF EXISTS `"+db+"`")
				mustExec(t, admin, "CREATE DATABASE `"+db+"` CHARACTER SET utf8mb4")
			}
			for _, host := range []string{"%", "localhost"} {
				mustExec(t, admin, fmt.Sprintf("CREATE USER IF NOT EXISTS 'nsync_ro_test'@'%s' IDENTIFIED BY 'ro_pw'", host))
				mustExec(t, admin, fmt.Sprintf("GRANT SELECT ON `%s`.* TO 'nsync_ro_test'@'%s'", master, host))
			}
		},
		ddl: mysqlDDL,
	}
	runSuite(t, env, "mysql")
}

func TestPostgres(t *testing.T) {
	base := os.Getenv("NEWAPI_SYNC_TEST_POSTGRES")
	if base == "" {
		t.Skip("未设置 NEWAPI_SYNC_TEST_POSTGRES")
	}
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	withDB := func(user *url.Userinfo, db string) string {
		c := *u
		c.User = user
		c.Path = "/" + db
		return c.String()
	}
	env := dialectEnv{
		name:     "postgres",
		adminDSN: func(db string) string { return withDB(u.User, db) },
		roDSN:    func(db string) string { return withDB(url.UserPassword("nsync_ro_test", "ro_pw"), db) },
		setup: func(t *testing.T, admin *sql.DB, master, child1, child2 string) {
			for _, db := range []string{master, child1, child2} {
				mustExec(t, admin, `DROP DATABASE IF EXISTS "`+db+`"`)
				mustExec(t, admin, `CREATE DATABASE "`+db+`"`)
			}
			mustExec(t, admin, `DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'nsync_ro_test') THEN CREATE ROLE nsync_ro_test LOGIN PASSWORD 'ro_pw'; END IF; END $$`)
		},
		ddl: postgresDDL,
	}
	runSuite(t, env, "pgx")
}

func runSuite(t *testing.T, env dialectEnv, driver string) {
	suffix := fmt.Sprintf("%d", time.Now().UnixNano()%1e8)
	masterDB, child1DB, child2DB := "nsync_master_"+suffix, "nsync_child1_"+suffix, "nsync_child2_"+suffix

	adminDB := ""
	if env.name == "postgres" {
		adminDB = "postgres"
	}
	admin := mustOpen(t, driver, env.adminDSN(adminDB))
	env.setup(t, admin, masterDB, child1DB, child2DB)
	t.Cleanup(func() {
		for _, db := range []string{masterDB, child1DB, child2DB} {
			if env.name == "mysql" {
				_, _ = admin.Exec("DROP DATABASE IF EXISTS `" + db + "`")
			} else {
				_, _ = admin.Exec(`DROP DATABASE IF EXISTS "` + db + `" WITH (FORCE)`)
			}
		}
	})

	m := mustOpen(t, driver, env.adminDSN(masterDB))
	c1 := mustOpen(t, driver, env.adminDSN(child1DB))
	c2 := mustOpen(t, driver, env.adminDSN(child2DB))
	for _, q := range env.ddl(false) {
		mustExec(t, m, q)
	}
	for _, db := range []*sql.DB{c1, c2} {
		for _, q := range env.ddl(true) {
			mustExec(t, db, q)
		}
	}
	if env.name == "postgres" {
		mustExec(t, m, "GRANT USAGE ON SCHEMA public TO nsync_ro_test")
		mustExec(t, m, "GRANT SELECT ON ALL TABLES IN SCHEMA public TO nsync_ro_test")
	}

	d, _ := dbx.DialectFor(map[string]string{"mysql": dbx.MySQL, "postgres": dbx.Postgres}[env.name])
	q := d.Quote
	ph := d.Placeholder

	insertChannel := func(db *sql.DB, cols []string, vals ...any) {
		t.Helper()
		quoted := make([]string, len(cols))
		phs := make([]string, len(cols))
		for i, c := range cols {
			quoted[i], phs[i] = q(c), ph(i+1)
		}
		mustExec(t, db, "INSERT INTO "+q("channels")+" ("+strings.Join(quoted, ",")+") VALUES ("+strings.Join(phs, ",")+")", vals...)
	}
	cols := []string{"id", "type", "key", "status", "name", "weight", "models", "group", "priority", "tag", "used_quota", "channel_info", "header_override"}
	// 主站：普通渠道、手动禁用渠道、多 Key 渠道（第 0 个 Key 被手动禁用，第 1 个被主站自动禁用）
	insertChannel(m, cols, 1, 1, "sk-one", 1, "openai", 0, "gpt-4o,gpt-4o-mini", "default,vip", 10, nil, 100, `{"is_multi_key":false,"multi_key_size":0,"multi_key_status_list":null,"multi_key_polling_index":0,"multi_key_mode":""}`, `{"X-A":"1"}`)
	insertChannel(m, cols, 2, 14, "sk-two", 2, "claude", 5, "claude-sonnet", "default", 0, "team", 7, nil, nil)
	insertChannel(m, cols, 3, 1, "k1\nk2\nk3", 1, "multi", 3, "o3", "vip", 5, nil, 0, `{"is_multi_key":true,"multi_key_size":3,"multi_key_status_list":{"0":2,"1":3},"multi_key_disabled_reason":{"0":"manual","1":"401"},"multi_key_polling_index":1,"multi_key_mode":"polling"}`, nil)
	setOption := func(db *sql.DB, k, v string) {
		t.Helper()
		mustExec(t, db, "DELETE FROM "+q("options")+" WHERE "+q("key")+" = "+ph(1), k)
		mustExec(t, db, "INSERT INTO "+q("options")+" ("+q("key")+","+q("value")+") VALUES ("+ph(1)+","+ph(2)+")", k, v)
	}
	setOption(m, "GroupRatio", `{"default":1,"vip":0.8}`)
	setOption(m, "UserUsableGroups", `{"default":"默认分组","vip":"VIP"}`)
	setOption(m, "group_ratio_setting.group_special_usable_group", `{"vip":{"+:svip":"超级"}}`)
	setOption(m, "SystemName", "主站名称") // 不在同步范围内

	// 子站：一条主站没有的旧渠道（应被删除），以及子站自己的站点名称（应保留）
	childCols := []string{"id", "type", "key", "status", "name", "models", "group", "routing_hint"}
	for _, c := range []*sql.DB{c1, c2} {
		insertChannel(c, childCols, 99, 1, "old", 1, "stale", "m", "default", "x")
		mustExec(t, c, "INSERT INTO "+q("abilities")+" ("+q("group")+","+q("model")+","+q("channel_id")+","+q("enabled")+") VALUES ('default','m',99,"+boolLit(env.name, true)+")")
		setOption(c, "SystemName", "子站名称")
	}

	// 模拟子站 1 的 NewAPI 管理接口：fix 按 NewAPI 规则重建全部能力，option 写库
	var fixCalls, optionCalls atomic.Int32
	var apiMu sync.Mutex
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiMu.Lock()
		defer apiMu.Unlock()
		if r.Header.Get("Authorization") != "Bearer nap_test" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"success":false,"message":"unauthorized"}`))
			return
		}
		switch r.Method + " " + r.URL.Path {
		case "GET /api/status":
			_, _ = w.Write([]byte(`{"success":true,"data":{"version":"v-fake"}}`))
		case "POST /api/channel/fix":
			fixCalls.Add(1)
			fakeFixAbilities(t, c1, d)
			_, _ = w.Write([]byte(`{"success":true,"data":{"success":1,"fails":0}}`))
		case "PUT /api/option/":
			optionCalls.Add(1)
			var body map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			setOption(c1, body["key"], body["value"])
			_, _ = w.Write([]byte(`{"success":true}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer fake.Close()

	cfgYAML := fmt.Sprintf(`
master:
  database: {type: %[1]s, dsn: %[2]q}
children:
  - name: child-api
    database: {type: %[1]s, dsn: %[3]q}
    api: {base_url: %[5]q, access_token: nap_test}
  - name: child-db
    database: {type: %[1]s, dsn: %[4]q}
sync: {poll_interval: 200ms, debounce: 50ms, retry_min: 100ms, retry_max: 500ms}
http: {listen: "off"}
`, env.name, env.roDSN(masterDB), env.adminDSN(child1DB), env.adminDSN(child2DB), fake.URL)
	cfg, err := config.Parse([]byte(cfgYAML))
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if testing.Verbose() {
		log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	ctx := context.Background()
	a, err := app.Build(ctx, cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	// ---------- 第一次同步 ----------
	if err := a.Engine.SyncOnce(ctx); err != nil {
		t.Fatalf("首次同步失败：%v", err)
	}
	for _, c := range []*sql.DB{c1, c2} {
		ids := queryStrings(t, c, "SELECT "+q("id")+" FROM "+q("channels")+" ORDER BY "+q("id"))
		if strings.Join(ids, ",") != "1,2,3" {
			t.Fatalf("子站渠道应与主站一致，got %v", ids)
		}
		// 子站新版本独有的必填列被填了零值
		if v := queryString(t, c, "SELECT "+q("routing_hint")+" FROM "+q("channels")+" WHERE "+q("id")+" = 1"); v != "" {
			t.Errorf("routing_hint 应为零值，got %q", v)
		}
		// 子站自有列不复制主站的已用额度
		if v := queryString(t, c, "SELECT COALESCE("+q("used_quota")+", 0) FROM "+q("channels")+" WHERE "+q("id")+" = 1"); v != "0" {
			t.Errorf("used_quota 不应复制主站的值，got %s", v)
		}
		if v := queryString(t, c, "SELECT "+q("status")+" FROM "+q("channels")+" WHERE "+q("id")+" = 2"); v != "2" {
			t.Errorf("手动禁用应同步，got %s", v)
		}
		info := queryString(t, c, "SELECT "+q("channel_info")+" FROM "+q("channels")+" WHERE "+q("id")+" = 3")
		if !strings.Contains(info, `"0": 2`) && !strings.Contains(info, `"0":2`) || strings.Contains(info, `"1":3`) || strings.Contains(info, `"1": 3`) {
			t.Errorf("多 Key 状态应只同步手动禁用：%s", info)
		}
		// 能力表：渠道 1 = 2 模型 × 2 分组，渠道 2 禁用但存在 1 条，渠道 3 = 1 条；旧渠道 99 的能力被清理
		abilities := queryStrings(t, c, "SELECT "+q("group")+" || ':' || "+q("model")+" || ':' || "+castText(env.name, q("channel_id"))+" FROM "+q("abilities")+" ORDER BY 1")
		if env.name == "mysql" {
			abilities = queryStrings(t, c, "SELECT CONCAT("+q("group")+", ':', "+q("model")+", ':', "+q("channel_id")+") FROM "+q("abilities")+" ORDER BY 1")
		}
		sort.Strings(abilities) // 不依赖数据库排序规则
		want := "default:claude-sonnet:2,default:gpt-4o-mini:1,default:gpt-4o:1,vip:gpt-4o-mini:1,vip:gpt-4o:1,vip:o3:3"
		if strings.Join(abilities, ",") != want {
			t.Errorf("能力表不对：\n got  %s\n want %s", strings.Join(abilities, ","), want)
		}
		if v := queryString(t, c, "SELECT "+q("value")+" FROM "+q("options")+" WHERE "+q("key")+" = 'GroupRatio'"); v != `{"default":1,"vip":0.8}` {
			t.Errorf("GroupRatio 未同步：%q", v)
		}
		if v := queryString(t, c, "SELECT "+q("value")+" FROM "+q("options")+" WHERE "+q("key")+" = 'group_ratio_setting.group_special_usable_group'"); v == "" {
			t.Errorf("前缀匹配的分组设置未同步")
		}
		if v := queryString(t, c, "SELECT "+q("value")+" FROM "+q("options")+" WHERE "+q("key")+" = 'SystemName'"); v != "子站名称" {
			t.Errorf("同步范围外的设置不应被改动：%q", v)
		}
	}
	if fixCalls.Load() != 1 || optionCalls.Load() != 3 {
		t.Errorf("子站 1 应调用 fix 1 次、option 3 次，got %d / %d", fixCalls.Load(), optionCalls.Load())
	}

	// ---------- 主库只读：即使给的是有写权限的账号，只读连接也必须拒绝写入 ----------
	for _, dsn := range []string{env.roDSN(masterDB), env.adminDSN(masterDB)} {
		ro, err := dbx.Open(ctx, dbx.Options{Type: env.name, DSN: dsn, ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ro.Exec("UPDATE " + q("channels") + " SET " + q("name") + " = 'hacked'"); err == nil {
			t.Fatal("主库连接应拒绝写入")
		}
		_ = ro.Close()
	}
	if n := queryString(t, m, "SELECT COUNT(*) FROM "+q("channels")+" WHERE "+q("name")+" = 'hacked'"); n != "0" {
		t.Fatal("主库被写入了")
	}

	// ---------- 防呆：子站指向主库（换一种写法的 DSN、有写权限的账号）必须拒绝启动 ----------
	badYAML := fmt.Sprintf(`
master:
  database: {type: %[1]s, dsn: %[2]q}
children:
  - name: oops
    database: {type: %[1]s, dsn: %[3]q}
http: {listen: "off"}
`, env.name, env.roDSN(masterDB), env.adminDSN(masterDB))
	badCfg, err := config.Parse([]byte(badYAML))
	if err != nil {
		t.Fatal(err)
	}
	if bad, err := app.Build(ctx, badCfg, log); err == nil {
		_ = bad.Close()
		t.Fatal("子站指向主库时必须拒绝启动")
	} else if !strings.Contains(err.Error(), "同一个数据库") {
		t.Fatalf("拒绝原因不对：%v", err)
	}

	// ---------- 实时同步循环 ----------
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { _ = a.Engine.Run(runCtx); close(done) }()
	defer func() { cancel(); <-done }()
	time.Sleep(500 * time.Millisecond) // 等首轮全量完成

	// 子站自己的运行时变化：自动禁用渠道 1、额度变化 —— 不应被同步覆盖
	for _, c := range []*sql.DB{c1, c2} {
		mustExec(t, c, "UPDATE "+q("channels")+" SET "+q("status")+" = 3, "+q("used_quota")+" = 777 WHERE "+q("id")+" = 1")
	}
	// 主站的运行时变化（已用额度、多 Key 轮询下标）：不应触发同步
	fixBefore := fixCalls.Load()
	changesBefore := a.Status.Snapshot().Master.Changes
	mustExec(t, m, "UPDATE "+q("channels")+" SET "+q("used_quota")+" = 123456 WHERE "+q("id")+" = 1")
	mustExec(t, m, "UPDATE "+q("channels")+" SET "+q("channel_info")+" = "+ph(1)+" WHERE "+q("id")+" = 3",
		`{"is_multi_key":true,"multi_key_size":3,"multi_key_status_list":{"0":2,"1":3},"multi_key_disabled_reason":{"0":"manual","1":"401"},"multi_key_polling_index":2,"multi_key_mode":"polling"}`)
	time.Sleep(700 * time.Millisecond)
	if fixCalls.Load() != fixBefore || a.Status.Snapshot().Master.Changes != changesBefore {
		t.Errorf("主站运行时变化不应触发同步")
	}

	// 主站修改：改优先级和模型、手动启用渠道 2、新增渠道 4、删除渠道 3、改分组倍率
	start := time.Now()
	mustExec(t, m, "UPDATE "+q("channels")+" SET "+q("priority")+" = 20, "+q("models")+" = 'gpt-4o' WHERE "+q("id")+" = 1")
	mustExec(t, m, "UPDATE "+q("channels")+" SET "+q("status")+" = 1 WHERE "+q("id")+" = 2")
	insertChannel(m, cols, 4, 1, "sk-four", 1, "new", 0, "gemini", "default", 1, nil, 0, nil, nil)
	// 只差大小写/末尾空格的模型名：在 MySQL 默认排序规则下是同一个主键，不能让整个子站同步卡住
	insertChannel(m, cols, 5, 1, "sk-five", 1, "dup", 0, "Dup-Model,dup-model,dup-model ", "default", 0, nil, 0, nil, nil)
	mustExec(t, m, "DELETE FROM "+q("channels")+" WHERE "+q("id")+" = 3")
	setOption(m, "GroupRatio", `{"default":1,"vip":0.5}`)

	waitFor(t, "子站跟上主站修改", func() bool {
		for _, c := range []*sql.DB{c1, c2} {
			ids := strings.Join(queryStrings(t, c, "SELECT "+q("id")+" FROM "+q("channels")+" ORDER BY "+q("id")), ",")
			if ids != "1,2,4,5" {
				return false
			}
			if queryString(t, c, "SELECT "+q("priority")+" FROM "+q("channels")+" WHERE "+q("id")+" = 1") != "20" {
				return false
			}
			if queryString(t, c, "SELECT "+q("status")+" FROM "+q("channels")+" WHERE "+q("id")+" = 2") != "1" {
				return false
			}
			if queryString(t, c, "SELECT "+q("value")+" FROM "+q("options")+" WHERE "+q("key")+" = 'GroupRatio'") != `{"default":1,"vip":0.5}` {
				return false
			}
		}
		return true
	})
	t.Logf("[%s] 主站修改到两个子站全部生效耗时 %v", env.name, time.Since(start))

	for _, c := range []*sql.DB{c1, c2} {
		row := queryStrings(t, c, "SELECT "+castText(env.name, q("status"))+" || '/' || "+castText(env.name, q("used_quota"))+" FROM "+q("channels")+" WHERE "+q("id")+" = 1")
		if env.name == "mysql" {
			row = queryStrings(t, c, "SELECT CONCAT("+q("status")+", '/', "+q("used_quota")+") FROM "+q("channels")+" WHERE "+q("id")+" = 1")
		}
		// 主站改了渠道 1 的配置：子站之前的自动禁用判断过时，恢复启用；子站自己的已用额度保留
		if len(row) != 1 || row[0] != "1/777" {
			t.Errorf("渠道 1 应恢复启用且保留子站已用额度，got %v", row)
		}
		if n := queryString(t, c, "SELECT COUNT(*) FROM "+q("abilities")+" WHERE "+q("channel_id")+" = 3"); n != "0" {
			t.Errorf("已删除渠道的能力应清理，剩 %s 条", n)
		}
		if n := queryString(t, c, "SELECT COUNT(*) FROM "+q("abilities")+" WHERE "+q("channel_id")+" = 1 AND "+q("enabled")+" = "+boolLit(env.name, true)); n != "2" {
			t.Errorf("渠道 1 改为单模型后应有 2 条启用的能力（2 个分组），got %s", n)
		}
		if n := queryString(t, c, "SELECT COUNT(*) FROM "+q("abilities")+" WHERE "+q("channel_id")+" = 5"); n == "0" {
			t.Errorf("大小写重复的模型也应生成能力记录")
		}
	}

	// ---------- 能力表自愈（未使用修复接口的子站由同步服务负责能力表） ----------
	cancel()
	<-done
	// 模拟：子站自动禁用了渠道 4，但能力表没跟上（仍是启用）；另有一条遗留的孤儿能力记录
	mustExec(t, c2, "UPDATE "+q("channels")+" SET "+q("status")+" = 3 WHERE "+q("id")+" = 4")
	mustExec(t, c2, "INSERT INTO "+q("abilities")+" ("+q("group")+","+q("model")+","+q("channel_id")+","+q("enabled")+") VALUES ('default','ghost',777,"+boolLit(env.name, true)+")")
	if err := a.Engine.SyncOnce(ctx); err != nil {
		t.Fatalf("修复能力表的同步失败：%v", err)
	}
	if v := queryString(t, c2, "SELECT "+q("status")+" FROM "+q("channels")+" WHERE "+q("id")+" = 4"); v != "3" {
		t.Errorf("子站自己的自动禁用应保留，got %s", v)
	}
	if n := queryString(t, c2, "SELECT COUNT(*) FROM "+q("abilities")+" WHERE ("+q("channel_id")+" = 4 AND "+q("enabled")+" = "+boolLit(env.name, true)+") OR "+q("channel_id")+" = 777"); n != "0" {
		t.Errorf("能力表应被修复（自动禁用渠道不应启用、遗留记录应清理），仍有 %s 条", n)
	}

	snap := a.Status.Snapshot()
	for _, c := range snap.Children {
		if c.State != "ok" {
			t.Errorf("子站 %s 状态 %s：%s", c.Name, c.State, c.LastError)
		}
		if len(c.MissingInChild) != 1 || c.MissingInChild[0] != "header_override" {
			t.Errorf("子站 %s 应识别出缺少 header_override 列：%v", c.Name, c.MissingInChild)
		}
	}
}

// fakeFixAbilities 模拟 NewAPI 的 FixAbility：清空能力表，按全部渠道重建（与 NewAPI 一样忽略主键冲突）。
func fakeFixAbilities(t *testing.T, db *sql.DB, d dbx.Dialect) {
	ctx := context.Background()
	schema, err := newapidb.LoadSchema(ctx, db, d)
	if err != nil {
		t.Error(err)
		return
	}
	table, err := newapidb.ReadChannels(ctx, db, d, schema.Channels)
	if err != nil {
		t.Error(err)
		return
	}
	q := d.Quote
	if _, err := db.Exec("DELETE FROM " + q("abilities")); err != nil {
		t.Error(err)
		return
	}
	for _, id := range table.IDs() {
		for _, a := range newapidb.BuildAbilities(id, table.Rows[id]) {
			_, err := db.Exec("INSERT INTO "+q("abilities")+" ("+q("group")+","+q("model")+","+q("channel_id")+","+q("enabled")+","+q("priority")+","+q("weight")+") VALUES ("+
				d.Placeholder(1)+","+d.Placeholder(2)+","+d.Placeholder(3)+","+d.Placeholder(4)+","+d.Placeholder(5)+","+d.Placeholder(6)+")"+d.OnConflictDoNothing("channel_id"),
				a.Group, a.Model, a.ChannelID, a.Enabled, a.Priority, a.Weight)
			if err != nil {
				t.Error(err)
			}
		}
	}
}

func mysqlDDL(newer bool) []string {
	extra, missing := "", "`header_override` text,"
	if newer {
		// 子站"新版本"：多了必填且无默认值的列和一个可空列，少了 header_override
		extra, missing = "`routing_hint` varchar(64) NOT NULL, `remark` varchar(255),", ""
	}
	return []string{
		"CREATE TABLE `channels` (`id` bigint NOT NULL AUTO_INCREMENT, `type` bigint DEFAULT 0, `key` longtext NOT NULL, " +
			"`open_ai_organization` longtext, `test_model` longtext, `status` bigint DEFAULT 1, `name` varchar(191), " +
			"`weight` bigint unsigned DEFAULT 0, `created_time` bigint, `test_time` bigint, `response_time` bigint, " +
			"`base_url` varchar(191) DEFAULT '', `other` longtext, `balance` double, `balance_updated_time` bigint, `models` longtext, " +
			"`group` varchar(64) DEFAULT 'default', `used_quota` bigint DEFAULT 0, `model_mapping` text, " +
			"`status_code_mapping` varchar(1024) DEFAULT '', `priority` bigint DEFAULT 0, `auto_ban` bigint DEFAULT 1, " +
			"`other_info` longtext, `tag` varchar(191), `setting` text, `param_override` text, " + missing + extra +
			"`channel_info` json, `settings` longtext, PRIMARY KEY (`id`))",
		"CREATE TABLE `abilities` (`group` varchar(64) NOT NULL, `model` varchar(255) NOT NULL, `channel_id` bigint NOT NULL, " +
			"`enabled` tinyint(1), `priority` bigint DEFAULT 0, `weight` bigint unsigned DEFAULT 0, `tag` varchar(191), " +
			"PRIMARY KEY (`group`,`model`,`channel_id`))",
		"CREATE TABLE `options` (`key` varchar(191) NOT NULL, `value` longtext, PRIMARY KEY (`key`))",
	}
}

func postgresDDL(newer bool) []string {
	extra, missing := "", `"header_override" text,`
	if newer {
		extra, missing = `"routing_hint" varchar(64) NOT NULL, "remark" varchar(255),`, ""
	}
	return []string{
		`CREATE TABLE "channels" ("id" bigserial PRIMARY KEY, "type" bigint DEFAULT 0, "key" text NOT NULL, ` +
			`"open_ai_organization" text, "test_model" text, "status" bigint DEFAULT 1, "name" text, "weight" bigint DEFAULT 0, ` +
			`"created_time" bigint, "test_time" bigint, "response_time" bigint, "base_url" text DEFAULT '', "other" text, ` +
			`"balance" decimal, "balance_updated_time" bigint, "models" text, "group" varchar(64) DEFAULT 'default', ` +
			`"used_quota" bigint DEFAULT 0, "model_mapping" text, "status_code_mapping" varchar(1024) DEFAULT '', ` +
			`"priority" bigint DEFAULT 0, "auto_ban" bigint DEFAULT 1, "other_info" text, "tag" text, "setting" text, ` +
			`"param_override" text, ` + missing + extra + ` "channel_info" json, "settings" text)`,
		`CREATE TABLE "abilities" ("group" varchar(64) NOT NULL, "model" varchar(255) NOT NULL, "channel_id" bigint NOT NULL, ` +
			`"enabled" boolean, "priority" bigint DEFAULT 0, "weight" bigint DEFAULT 0, "tag" text, PRIMARY KEY ("group","model","channel_id"))`,
		`CREATE TABLE "options" ("key" text PRIMARY KEY, "value" text)`,
	}
}

func boolLit(dialect string, b bool) string {
	if dialect == "postgres" {
		if b {
			return "true"
		}
		return "false"
	}
	if b {
		return "1"
	}
	return "0"
}

func castText(dialect, expr string) string {
	if dialect == "postgres" {
		return expr + "::text"
	}
	return expr
}

func mustOpen(t *testing.T, driver, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func mustExec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("%s\n%v", q, err)
	}
}

func queryString(t *testing.T, db *sql.DB, q string) string {
	t.Helper()
	var v sql.NullString
	if err := db.QueryRow(q).Scan(&v); err != nil {
		t.Fatalf("%s\n%v", q, err)
	}
	return v.String
}

func queryStrings(t *testing.T, db *sql.DB, q string) []string {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("%s\n%v", q, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v sql.NullString
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		out = append(out, v.String)
	}
	return out
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("等待超时：%s", what)
}
