package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/10dian-ai/newapi-channel-synchronization/internal/config"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/dbx"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/newapidb"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/notify"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/pricing"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/reconcile"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/snapshot"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/target"
)

func sv(v string) snapshot.Value { return &v }

var chanCols = dbx.Columns{
	{Name: "id", Kind: dbx.KindInt}, {Name: "key", Kind: dbx.KindText, Nullable: true},
	{Name: "status", Kind: dbx.KindInt, Nullable: true}, {Name: "models", Kind: dbx.KindText, Nullable: true},
	{Name: "group", Kind: dbx.KindText, Nullable: true}, {Name: "priority", Kind: dbx.KindInt, Nullable: true},
	{Name: "used_quota", Kind: dbx.KindInt, Nullable: true},
}

var abilityCols = dbx.Columns{
	{Name: "group", Kind: dbx.KindText}, {Name: "model", Kind: dbx.KindText}, {Name: "channel_id", Kind: dbx.KindInt},
	{Name: "enabled", Kind: dbx.KindBool, Nullable: true},
}

var optionCols = dbx.Columns{{Name: "key", Kind: dbx.KindText}, {Name: "value", Kind: dbx.KindText, Nullable: true}}

type fakeMaster struct {
	mu   sync.Mutex
	snap *snapshot.Master
	gen  int
}

func (f *fakeMaster) RefreshSchema(context.Context) (newapidb.Schema, error) {
	return newapidb.Schema{Channels: chanCols, Abilities: abilityCols, Options: optionCols}, nil
}

func (f *fakeMaster) Probe(context.Context) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snap.Fingerprint(nil), true, nil
}

func (f *fakeMaster) Load(context.Context, bool) (*snapshot.Master, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := &snapshot.Master{Channels: snapshot.Table{Columns: chanCols, Rows: map[int64]snapshot.Row{}}, Options: map[string]string{}, Pricing: map[string]string{}}
	for id, r := range f.snap.Channels.Rows {
		cp.Channels.Rows[id] = r.Clone()
	}
	for k, v := range f.snap.Options {
		cp.Options[k] = v
	}
	for k, v := range f.snap.Pricing {
		cp.Pricing[k] = v
	}
	return cp, nil
}

func (f *fakeMaster) set(fn func(m *snapshot.Master)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f.snap)
}

type fakeStore struct {
	mu          sync.Mutex
	state       *snapshot.Child
	schema      newapidb.Schema
	applies     int
	failApplies int
	dropWrites  bool // 模拟"写了但没生效"，用于验证写后校验
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		state:  &snapshot.Child{Channels: snapshot.Table{Columns: chanCols, Rows: map[int64]snapshot.Row{}}, Options: map[string]string{}, Pricing: map[string]string{}},
		schema: newapidb.Schema{Channels: chanCols, Abilities: abilityCols, Options: optionCols},
	}
}

func (f *fakeStore) Schema(context.Context) (newapidb.Schema, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.schema, nil
}

func (f *fakeStore) Load(_ context.Context, _ newapidb.Schema, opt target.LoadOptions) (*snapshot.Child, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := &snapshot.Child{Channels: snapshot.Table{Columns: chanCols, Rows: map[int64]snapshot.Row{}}, Options: map[string]string{}, Pricing: map[string]string{}}
	for id, r := range f.state.Channels.Rows {
		cp.Channels.Rows[id] = r.Clone()
	}
	for k, v := range f.state.Options {
		cp.Options[k] = v
	}
	if opt.Pricing {
		for k, v := range f.state.Pricing {
			cp.Pricing[k] = v
		}
	}
	return cp, nil
}

func (f *fakeStore) Apply(_ context.Context, plan *reconcile.Plan, _ newapidb.Schema, opt target.ApplyOptions) (target.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applies++
	if f.failApplies > 0 {
		f.failApplies--
		return target.Result{}, errors.New("模拟写入失败")
	}
	if f.dropWrites {
		return target.Result{}, nil
	}
	for _, w := range plan.Inserts {
		f.state.Channels.Rows[w.ID] = w.Values.Clone()
	}
	for _, w := range plan.Updates {
		for c, v := range w.Values {
			f.state.Channels.Rows[w.ID][c] = v
		}
	}
	for _, id := range plan.Deletes {
		delete(f.state.Channels.Rows, id)
	}
	if opt.WriteOptions {
		for _, o := range plan.Options {
			f.state.Options[o.Key] = o.Value
		}
		for _, c := range plan.Pricing.Changes {
			f.state.Pricing[c.Key] = c.Value
		}
	}
	res := target.Result{Inserted: len(plan.Inserts), Updated: len(plan.Updates), Deleted: len(plan.Deletes)}
	if opt.WriteOptions {
		res.Pricing = len(plan.Pricing.Changes)
	}
	return res, nil
}

type fakeAPI struct {
	mu      sync.Mutex
	store   *fakeStore
	fixes   int
	failFix int
	options []string
	// pricing 记录写入的模型价格设置项（按顺序）
	pricing []string
	// rejectPricing 不为空时，子站拒绝所有价格写入并返回这个错误信息
	rejectPricing string
}

func (f *fakeAPI) Version(context.Context) (string, error) { return "v-test", nil }

func (f *fakeAPI) FixAbilities(context.Context) (int, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failFix > 0 {
		f.failFix--
		return 0, 0, errors.New("模拟子站接口失败")
	}
	f.fixes++
	return 1, 0, nil
}

func (f *fakeAPI) UpdateOption(_ context.Context, k, v string) error {
	if pricing.IsKey(k) {
		return f.updatePricing(k, v)
	}
	f.mu.Lock()
	f.options = append(f.options, k)
	f.mu.Unlock()
	f.store.mu.Lock()
	f.store.state.Options[k] = v // 真实子站会写库，这里同样写入，便于写后校验
	f.store.mu.Unlock()
	return nil
}

// updatePricing 模拟新版 NewAPI 写入一项价格：按模型校验（tiered_expr 必须有表达式），
// 然后把全部 11 个价格设置项重新编码后写回库里，缺的设置项按默认值（这里用空表）补上。
func (f *fakeAPI) updatePricing(k, v string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rejectPricing != "" {
		return errors.New(f.rejectPricing)
	}
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	values := map[string]map[string]any{}
	for _, key := range pricing.Keys {
		obj := map[string]any{}
		if raw, ok := f.store.state.Pricing[key]; ok && strings.TrimSpace(raw) != "" {
			if err := json.Unmarshal([]byte(raw), &obj); err != nil {
				return err
			}
		}
		values[key] = obj
	}
	next := map[string]any{}
	if err := json.Unmarshal([]byte(v), &next); err != nil || next == nil {
		return fmt.Errorf("%s must be a JSON object", k)
	}
	values[k] = next
	for name, mode := range values[pricing.KeyBillingMode] {
		if _, ok := values[pricing.KeyBillingExpr][name]; mode == "tiered_expr" && !ok {
			return fmt.Errorf("model %s: billing expression is required", name)
		}
	}
	for key, obj := range values {
		b, _ := json.Marshal(obj)
		f.store.state.Pricing[key] = string(b)
	}
	f.pricing = append(f.pricing, k)
	return nil
}

func testConfig(withAPI bool) *config.Config {
	cfg := &config.Config{
		Master:   config.MasterConfig{Database: config.DatabaseConfig{DSN: "m"}},
		Children: []config.ChildConfig{{Name: "c1", Database: config.DatabaseConfig{DSN: "c"}}},
	}
	if withAPI {
		cfg.Children[0].API = config.APIConfig{BaseURL: "http://c1", AccessToken: "t"}
	}
	cfg.ApplyDefaults()
	cfg.Sync.PollInterval = config.Duration(20 * time.Millisecond)
	cfg.Sync.Debounce = config.Duration(5 * time.Millisecond)
	cfg.Sync.RetryMin = config.Duration(10 * time.Millisecond)
	cfg.Sync.RetryMax = config.Duration(20 * time.Millisecond)
	return cfg
}

func masterSnap() *snapshot.Master {
	return &snapshot.Master{
		Channels: snapshot.Table{Columns: chanCols, Rows: map[int64]snapshot.Row{
			1: {"id": sv("1"), "key": sv("k1"), "status": sv("1"), "models": sv("m1"), "group": sv("default"), "priority": sv("0"), "used_quota": sv("5")},
		}},
		Options: map[string]string{"GroupRatio": `{"default":1}`},
	}
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newTestWorker(cfg *config.Config, store *fakeStore, api ChildAPI) (*worker, *Status) {
	st := NewStatus("m")
	w := newWorker(cfg, Child{Config: cfg.Children[0], Store: store, API: api}, quietLog(), notify.New("", quietLog()), st)
	return w, st
}

func TestSyncOnceAppliesRefreshesAndVerifies(t *testing.T) {
	cfg := testConfig(true)
	store := newFakeStore()
	api := &fakeAPI{store: store}
	w, st := newTestWorker(cfg, store, api)
	snap := masterSnap()
	snap.Full = true
	if err := w.syncOnce(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	if store.state.Channels.Rows[1].String("key") != "k1" || store.state.Options["GroupRatio"] != `{"default":1}` {
		t.Fatalf("子站未同步：%+v", store.state)
	}
	if _, ok := store.state.Channels.Rows[1]["used_quota"]; ok {
		t.Fatal("子站自有列不应写入")
	}
	if api.fixes != 1 || len(api.options) != 1 {
		t.Fatalf("应调用一次修复接口和一次设置接口：fix=%d options=%v", api.fixes, api.options)
	}
	if c := st.Snapshot().Children[0]; c.State != StateOK || c.Inserted != 1 || c.Version != "v-test" {
		t.Fatalf("状态不对：%+v", c)
	}
	// 再同步一次：没有变化就不应写入、也不应调用接口
	if err := w.syncOnce(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	if store.applies != 1 || api.fixes != 1 {
		t.Fatalf("无变化时不应写入：applies=%d fixes=%d", store.applies, api.fixes)
	}
}

func TestVerificationFailureIsReported(t *testing.T) {
	cfg := testConfig(false)
	store := newFakeStore()
	store.dropWrites = true
	w, _ := newTestWorker(cfg, store, nil)
	err := w.syncOnce(context.Background(), masterSnap())
	if err == nil {
		t.Fatal("写入没生效时写后校验应失败")
	}
}

func TestRefreshIsRetriedAfterAPIFailure(t *testing.T) {
	cfg := testConfig(true)
	store := newFakeStore()
	api := &fakeAPI{store: store, failFix: 1}
	w, _ := newTestWorker(cfg, store, api)
	snap := masterSnap()
	if err := w.syncOnce(context.Background(), snap); err == nil {
		t.Fatal("修复接口失败应返回错误")
	}
	// 渠道已写入，下一轮差异为空，但仍必须补调修复接口让子站生效
	if err := w.syncOnce(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	if api.fixes != 1 {
		t.Fatalf("应补调一次修复接口，got %d", api.fixes)
	}
}

func TestDryRunDoesNotWrite(t *testing.T) {
	cfg := testConfig(true)
	cfg.Sync.DryRun = true
	store := newFakeStore()
	api := &fakeAPI{store: store}
	w, st := newTestWorker(cfg, store, api)
	if err := w.syncOnce(context.Background(), masterSnap()); err != nil {
		t.Fatal(err)
	}
	if store.applies != 0 || api.fixes != 0 || len(api.options) != 0 {
		t.Fatal("演练模式不应写入")
	}
	if c := st.Snapshot().Children[0]; c.State != StateDryRun || c.LastPlan == "" {
		t.Fatalf("演练结果应记录在状态里：%+v", c)
	}
}

func TestSchemaChangePause(t *testing.T) {
	cfg := testConfig(false)
	cfg.Sync.OnSchemaChange = config.SchemaChangePause
	store := newFakeStore()
	w, st := newTestWorker(cfg, store, nil)
	snap := masterSnap()
	snap.Full = true
	if err := w.syncOnce(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.schema.Channels = append(append(dbx.Columns{}, chanCols...), dbx.Column{Name: "brand_new", Kind: dbx.KindText, Nullable: true})
	store.mu.Unlock()
	if err := w.syncOnce(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	if c := st.Snapshot().Children[0]; c.State != StatePaused {
		t.Fatalf("结构变化后应暂停：%+v", c)
	}
}

// TestEngineEndToEnd 跑完整的引擎循环：主站变化 → 子站在短时间内跟上；写入失败会退避重试。
func TestEngineEndToEnd(t *testing.T) {
	cfg := testConfig(true)
	master := &fakeMaster{snap: masterSnap()}
	store := newFakeStore()
	store.failApplies = 2
	api := &fakeAPI{store: store}
	st := NewStatus("m")
	e := New(cfg, master, []Child{{Config: cfg.Children[0], Store: store, API: api}}, quietLog(), notify.New("", quietLog()), st)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = e.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	waitFor(t, "首次同步（含两次失败重试）", func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.state.Channels.Rows[1] != nil
	})

	master.set(func(m *snapshot.Master) {
		m.Channels.Rows[2] = snapshot.Row{"id": sv("2"), "key": sv("k2"), "status": sv("2"), "models": sv("m2"), "group": sv("vip"), "priority": sv("1"), "used_quota": sv("0")}
		m.Channels.Rows[1]["priority"] = sv("9")
		m.Options["GroupRatio"] = `{"default":1,"vip":2}`
		m.Pricing = map[string]string{"ModelRatio": `{"m1": 2.5, "m2": 0.5}`}
	})
	start := time.Now()
	waitFor(t, "增量同步", func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		r1, r2 := store.state.Channels.Rows[1], store.state.Channels.Rows[2]
		return r2 != nil && r2.String("status") == "2" && r1.String("priority") == "9" && store.state.Options["GroupRatio"] == `{"default":1,"vip":2}` &&
			pricing.Equal(store.state.Pricing["ModelRatio"], `{"m1":2.5,"m2":0.5}`)
	})
	t.Logf("增量同步耗时 %v", time.Since(start))

	master.set(func(m *snapshot.Master) { delete(m.Channels.Rows, 2) })
	waitFor(t, "删除同步", func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.state.Channels.Rows[2] == nil
	})
	if c := st.Snapshot().Children[0]; c.Failures != 2 {
		t.Errorf("应记录两次失败，got %d", c.Failures)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("等待超时：%s", what)
}

// masterPricing 是主站库里的模型价格：原文带缩进（旧版本或手工保存的样子），
// 一个模型用表达式计费，其他模型还是旧倍率。
func masterPricing() map[string]string {
	return map[string]string{
		"ModelRatio":                 "{\n  \"gpt-4o\": 1.25,\n  \"o3\": 5\n}",
		"CompletionRatio":            `{"gpt-4o": 4}`,
		pricing.KeyBillingMode:       `{"gpt-6-astra": "tiered_expr"}`,
		pricing.KeyBillingExpr:       `{"gpt-6-astra": "tier(\"standard\", p * 10 + c * 50)"}`,
		pricing.KeyQuotaPerUnit:      "500000",
		pricing.KeyPluginBillingExpr: `{}`,
	}
}

func pricingEqual(t *testing.T, master, child map[string]string) {
	t.Helper()
	for _, k := range pricing.Keys {
		mv, ok := master[k]
		if !ok {
			continue
		}
		if !pricing.Equal(mv, child[k]) {
			t.Fatalf("模型价格 %s 未同步：主站 %s，子站 %s", k, mv, child[k])
		}
	}
}

// TestPricingSyncViaAPIIsStable：子站接口写入后会重新编码价格（新版 NewAPI 的行为），
// 写后校验仍然通过，下一轮也不会再重复写入；子站多出来的模型价格被删除。
func TestPricingSyncViaAPIIsStable(t *testing.T) {
	cfg := testConfig(true)
	store := newFakeStore()
	store.state.Pricing = map[string]string{
		"ModelRatio":           `{"gpt-4o":9,"child-only":1}`,
		pricing.KeyBillingMode: `{"retired":"tiered_expr"}`,
		pricing.KeyBillingExpr: `{"retired":"tier(\"base\", p)"}`,
	}
	api := &fakeAPI{store: store}
	w, st := newTestWorker(cfg, store, api)
	snap := masterSnap()
	snap.Pricing = masterPricing()
	snap.Full = true

	if err := w.syncOnce(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	pricingEqual(t, snap.Pricing, store.state.Pricing)
	if strings.Contains(store.state.Pricing["ModelRatio"], "child-only") {
		t.Fatal("子站多出来的模型价格应被删除")
	}
	if got := strings.Join(api.pricing, ","); !strings.HasPrefix(got, pricing.KeyBillingExpr+",") || !strings.HasSuffix(got, ","+pricing.KeyBillingExpr) {
		t.Fatalf("表达式应先补后删：%s", got)
	}
	writes := len(api.pricing)
	c := st.Snapshot().Children[0]
	if c.State != StateOK || c.PricingWrites != int64(writes) {
		t.Fatalf("状态不对：%+v", c)
	}
	// 子站把 11 项都建出来了，主站库里只有其中几项：其余的保持子站原样并列在状态里
	if len(c.PricingNotInMaster) == 0 {
		t.Fatal("应列出主站库里没有的价格设置项")
	}

	if err := w.syncOnce(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	if len(api.pricing) != writes {
		t.Fatalf("价格已一致，不应重复写入：%v", api.pricing)
	}
}

func TestPricingSyncViaDB(t *testing.T) {
	cfg := testConfig(false)
	store := newFakeStore()
	store.state.Pricing = map[string]string{"ModelRatio": `{"old":1}`, pricing.KeyQuotaPerUnit: "1000000"}
	w, st := newTestWorker(cfg, store, nil)
	snap := masterSnap()
	snap.Pricing = masterPricing()
	if err := w.syncOnce(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	pricingEqual(t, snap.Pricing, store.state.Pricing)
	if store.state.Pricing[pricing.KeyQuotaPerUnit] != "1000000" {
		t.Fatal("QuotaPerUnit 永远不能被同步")
	}
	c := st.Snapshot().Children[0]
	if c.PricingWrites != 5 {
		t.Fatalf("直接写库应写入 5 项价格，got %d", c.PricingWrites)
	}
	var warned bool
	for _, warn := range c.LastWarnings {
		warned = warned || strings.Contains(warn, "QuotaPerUnit")
	}
	if !warned {
		t.Fatalf("QuotaPerUnit 不一致应提醒：%v", c.LastWarnings)
	}
}

func TestPricingDisabled(t *testing.T) {
	cfg := testConfig(false)
	off := false
	cfg.Pricing.Enabled = &off
	store := newFakeStore()
	store.state.Pricing = map[string]string{"ModelRatio": `{"old":1}`}
	w, _ := newTestWorker(cfg, store, nil)
	snap := masterSnap()
	snap.Pricing = masterPricing()
	if err := w.syncOnce(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	if store.state.Pricing["ModelRatio"] != `{"old":1}` {
		t.Fatal("关闭价格同步后不应改动子站价格")
	}
}

// TestPricingRejectedByChild：子站拒绝价格时报错（带上设置项名称），渠道照常同步。
func TestPricingRejectedByChild(t *testing.T) {
	cfg := testConfig(true)
	store := newFakeStore()
	api := &fakeAPI{store: store, rejectPricing: "Invalid billing expression for model gpt-6-astra"}
	w, st := newTestWorker(cfg, store, api)
	snap := masterSnap()
	snap.Pricing = masterPricing()
	err := w.syncOnce(context.Background(), snap)
	if err == nil || !strings.Contains(err.Error(), "模型价格") || !strings.Contains(err.Error(), "gpt-6-astra") {
		t.Fatalf("应报告价格写入失败，got %v", err)
	}
	if store.state.Channels.Rows[1] == nil || store.state.Options["GroupRatio"] == "" {
		t.Fatal("价格失败不应影响渠道和分组同步")
	}
	if c := st.Snapshot().Children[0]; c.State == StateOK {
		t.Fatalf("价格没同步上时状态不能是 ok：%+v", c)
	}
}
