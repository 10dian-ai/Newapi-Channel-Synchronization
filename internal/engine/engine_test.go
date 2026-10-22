package engine

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/10dian-ai/newapi-channel-synchronization/internal/config"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/dbx"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/newapidb"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/notify"
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
	cp := &snapshot.Master{Channels: snapshot.Table{Columns: chanCols, Rows: map[int64]snapshot.Row{}}, Options: map[string]string{}}
	for id, r := range f.snap.Channels.Rows {
		cp.Channels.Rows[id] = r.Clone()
	}
	for k, v := range f.snap.Options {
		cp.Options[k] = v
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
		state:  &snapshot.Child{Channels: snapshot.Table{Columns: chanCols, Rows: map[int64]snapshot.Row{}}, Options: map[string]string{}},
		schema: newapidb.Schema{Channels: chanCols, Abilities: abilityCols, Options: optionCols},
	}
}

func (f *fakeStore) Schema(context.Context) (newapidb.Schema, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.schema, nil
}

func (f *fakeStore) Load(context.Context, newapidb.Schema, target.LoadOptions) (*snapshot.Child, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := &snapshot.Child{Channels: snapshot.Table{Columns: chanCols, Rows: map[int64]snapshot.Row{}}, Options: map[string]string{}}
	for id, r := range f.state.Channels.Rows {
		cp.Channels.Rows[id] = r.Clone()
	}
	for k, v := range f.state.Options {
		cp.Options[k] = v
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
	}
	return target.Result{Inserted: len(plan.Inserts), Updated: len(plan.Updates), Deleted: len(plan.Deletes)}, nil
}

type fakeAPI struct {
	mu      sync.Mutex
	store   *fakeStore
	fixes   int
	failFix int
	options []string
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
	f.mu.Lock()
	f.options = append(f.options, k)
	f.mu.Unlock()
	f.store.mu.Lock()
	f.store.state.Options[k] = v // 真实子站会写库，这里同样写入，便于写后校验
	f.store.mu.Unlock()
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
	})
	start := time.Now()
	waitFor(t, "增量同步", func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		r1, r2 := store.state.Channels.Rows[1], store.state.Channels.Rows[2]
		return r2 != nil && r2.String("status") == "2" && r1.String("priority") == "9" && store.state.Options["GroupRatio"] == `{"default":1,"vip":2}`
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
