package pricing

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestEqualIgnoresFormatting(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"{\n  \"gpt-4o\": 1.25,\n  \"o3\": 5\n}", `{"o3":5,"gpt-4o":1.25}`, true},
		{`{"a":1}`, `{"a":1.0}`, true},
		{`{"a":1}`, `{"a":1e0}`, true},
		{"", "{}", true},
		{"null", "{}", true},
		{`{"a":1}`, `{"a":2}`, false},
		{`{"a":1}`, `{"a":1,"b":1}`, false},
		{`{"m":"tier(\"base\", p * 2)"}`, `{"m":"tier(\"base\", p * 2)"}`, true},
		{`{"m":"p >= 1 && c"}`, `{"m":"p >= 1 && c"}`, true},
		{"not json", " not json ", true},
		{"not json", `{}`, false},
	}
	for _, c := range cases {
		if got := Equal(c.a, c.b); got != c.want {
			t.Errorf("Equal(%q, %q) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

func TestDiffMirrorsMasterExactly(t *testing.T) {
	master := map[string]string{
		"ModelRatio":      "{\n  \"gpt-4o\": 1.25,\n  \"o3\": 5\n}",
		"CompletionRatio": `{"gpt-4o":4}`,
		KeyBillingMode:    `{}`,
	}
	child := map[string]string{
		"ModelRatio":      `{"gpt-4o":1.25,"o3":6,"child-only":1}`, // o3 不同，child-only 主站没有
		"CompletionRatio": `{"gpt-4o":4.0}`,                        // 内容相同，只是写法不同
		"ModelPrice":      `{"mj":0.1}`,                            // 主站库里没有这一项
		// 子站没有 billing_mode：主站的空表也要写过去
	}
	plan := Diff(master, child)

	if len(plan.Changes) != 2 {
		t.Fatalf("应有 2 项变更，got %+v", plan.Changes)
	}
	mr := plan.Changes[0]
	if mr.Key != "ModelRatio" || !mr.Exists || mr.Value != master["ModelRatio"] {
		t.Fatalf("ModelRatio 变更不对：%+v", mr)
	}
	if !reflect.DeepEqual(mr.Changed, []string{"o3"}) || !reflect.DeepEqual(mr.Removed, []string{"child-only"}) || len(mr.Added) != 0 {
		t.Fatalf("按模型的差异不对：%+v", mr)
	}
	if bm := plan.Changes[1]; bm.Key != KeyBillingMode || bm.Exists || bm.Value != "{}" {
		t.Fatalf("billing_mode 应以空表新增：%+v", bm)
	}
	if !reflect.DeepEqual(plan.NotInMaster, []string{"ModelPrice"}) {
		t.Fatalf("主站没有的设置项应记录并保持不动：%v", plan.NotInMaster)
	}
	if len(plan.Warnings) != 0 {
		t.Fatalf("不应有警告：%v", plan.Warnings)
	}

	// 写入后（子站可能重新编码）再比一次，应完全一致
	after := map[string]string{}
	for k, v := range child {
		after[k] = v
	}
	for _, c := range plan.Changes {
		after[c.Key] = reencode(t, c.Value)
	}
	if again := Diff(master, after); !again.Empty() {
		t.Fatalf("写入后应没有差异，got %+v", again.Changes)
	}
}

func TestDiffBlankAndInvalidMasterValues(t *testing.T) {
	plan := Diff(map[string]string{
		"ModelRatio": "",           // 空值：等同空表，写成 {}
		"ImageRatio": "{broken",    // 坏 JSON：跳过并警告，不能把坏值推给子站
		"AudioRatio": `{"tts":15}`, // 子站的值是坏的：整体覆盖
	}, map[string]string{
		"ModelRatio": `{"a":1}`,
		"ImageRatio": `{"img":1}`,
		"AudioRatio": `oops`,
	})
	got := map[string]string{}
	for _, c := range plan.Changes {
		got[c.Key] = c.Value
	}
	if got["ModelRatio"] != "{}" || got["AudioRatio"] != `{"tts":15}` {
		t.Fatalf("变更不对：%v", got)
	}
	if _, ok := got["ImageRatio"]; ok || len(plan.Warnings) != 1 || !strings.Contains(plan.Warnings[0], "ImageRatio") {
		t.Fatalf("主站坏值应跳过并警告：changes=%v warnings=%v", got, plan.Warnings)
	}
}

func TestQuotaPerUnitCheck(t *testing.T) {
	cases := []struct {
		master, child map[string]string
		warn          bool
	}{
		{map[string]string{}, map[string]string{}, false},
		{map[string]string{KeyQuotaPerUnit: "500000"}, map[string]string{}, false}, // 没有这一项就是默认值
		{map[string]string{KeyQuotaPerUnit: "500000"}, map[string]string{KeyQuotaPerUnit: "500000.0"}, false},
		{map[string]string{KeyQuotaPerUnit: "500000"}, map[string]string{KeyQuotaPerUnit: "1000000"}, true},
		{map[string]string{KeyQuotaPerUnit: "abc"}, map[string]string{KeyQuotaPerUnit: "1"}, false}, // 解析不了不乱报
	}
	for i, c := range cases {
		plan := Diff(c.master, c.child)
		if got := len(plan.Warnings) > 0; got != c.warn {
			t.Errorf("case %d：warn=%v，want %v（%v）", i, got, c.warn, plan.Warnings)
		}
		for _, ch := range plan.Changes {
			if ch.Key == KeyQuotaPerUnit {
				t.Fatal("QuotaPerUnit 永远不能同步")
			}
		}
	}
}

// fakeNewAPI 模拟新版 NewAPI 逐项写入价格时的校验：
// 计费模式为 tiered_expr 的模型必须有表达式，否则整次写入被拒绝。
type fakeNewAPI struct {
	values map[string]map[string]any
}

func newFakeNewAPI(t *testing.T, opts map[string]string) *fakeNewAPI {
	f := &fakeNewAPI{values: map[string]map[string]any{}}
	for _, k := range Keys {
		f.values[k] = map[string]any{}
		if v, ok := opts[k]; ok {
			obj, err := parseObject(v)
			if err != nil {
				t.Fatal(err)
			}
			f.values[k] = obj
		}
	}
	return f
}

func (f *fakeNewAPI) put(key, value string) error {
	obj, err := parseObject(value)
	if err != nil {
		return fmt.Errorf("%s must be a JSON object", key)
	}
	next := map[string]map[string]any{}
	for k, v := range f.values {
		next[k] = v
	}
	next[key] = obj
	for name, mode := range next[KeyBillingMode] {
		if mode == "tiered_expr" {
			if _, ok := next[KeyBillingExpr][name]; !ok {
				return fmt.Errorf("model %s: billing expression is required", name)
			}
		}
	}
	f.values = next
	return nil
}

func (f *fakeNewAPI) options(t *testing.T) map[string]string {
	out := map[string]string{}
	for k, v := range f.values {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		out[k] = string(b)
	}
	return out
}

// TestWriteOrderPassesNewAPIValidation：同一次同步里一个模型改成表达式计费、另一个模型改回旧倍率，
// 按 Writes 的顺序逐项写入，每一步都不会被新版 NewAPI 拒绝，最终与主站一致。
func TestWriteOrderPassesNewAPIValidation(t *testing.T) {
	child := map[string]string{
		"ModelRatio":   `{"old-to-expr":2}`,
		KeyBillingMode: `{"expr-to-old":"tiered_expr","child-only":"tiered_expr"}`,
		KeyBillingExpr: `{"expr-to-old":"tier(\"base\", p * 3)","child-only":"tier(\"base\", p)"}`,
	}
	master := map[string]string{
		"ModelRatio":   `{"expr-to-old":1.5}`,
		KeyBillingMode: `{"old-to-expr":"tiered_expr"}`,
		KeyBillingExpr: `{"old-to-expr":"tier(\"base\", p * 2 + c * 8)"}`,
	}
	api := newFakeNewAPI(t, child)
	plan := Diff(master, child)
	for i, w := range plan.Writes {
		if err := api.put(w.Key, w.Value); err != nil {
			t.Fatalf("第 %d 步写入 %s 被拒绝：%v（顺序：%v）", i+1, w.Key, err, writeKeys(plan.Writes))
		}
	}
	if again := Diff(master, api.options(t)); !again.Empty() {
		t.Fatalf("写完后应与主站一致，仍有差异：%+v", again.Changes)
	}
	if first, last := plan.Writes[0].Key, plan.Writes[len(plan.Writes)-1].Key; first != KeyBillingExpr || last != KeyBillingExpr {
		t.Fatalf("表达式应先补后删：%v", writeKeys(plan.Writes))
	}

	// 对照：直接按设置项顺序写主站的值会被拒绝，说明上面的顺序是必要的
	naive := newFakeNewAPI(t, child)
	var rejected bool
	for _, c := range plan.Changes {
		if err := naive.put(c.Key, c.Value); err != nil {
			rejected = true
			break
		}
	}
	if !rejected {
		t.Fatal("对照组应被拒绝，否则这个测试没有覆盖到校验")
	}
}

func TestWriteOrderWithoutRemovalsIsSingleStep(t *testing.T) {
	master := map[string]string{
		KeyBillingMode: `{"m":"tiered_expr"}`,
		KeyBillingExpr: `{"m":"tier(\"base\", p)"}`,
	}
	plan := Diff(master, map[string]string{})
	want := []string{KeyBillingExpr, KeyBillingMode}
	if got := writeKeys(plan.Writes); !reflect.DeepEqual(got, want) {
		t.Fatalf("只新增时表达式写一次且在计费模式之前：got %v", got)
	}
	if plan.Writes[0].Value != master[KeyBillingExpr] {
		t.Fatalf("没有要删除的表达式时直接写主站的值：%q", plan.Writes[0].Value)
	}
}

func TestDetails(t *testing.T) {
	plan := Diff(
		map[string]string{"ModelRatio": `{"a":1,"b":2,"c":3,"d":4,"e":5,"f":6,"g":7}`},
		map[string]string{"ModelRatio": `{"x":1}`},
	)
	d := plan.Details()
	if len(d) != 1 || !strings.Contains(d[0], "新增 7（a, b, c, d, e 等 7 个）") || !strings.Contains(d[0], "删除 1（x）") {
		t.Fatalf("说明不对：%v", d)
	}
	if strings.Contains(d[0], "7}") {
		t.Fatal("说明里不应出现价格数值")
	}
}

func reencode(t *testing.T, v string) string {
	t.Helper()
	obj, err := parseObject(v)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func writeKeys(ws []Write) []string {
	out := make([]string, len(ws))
	for i, w := range ws {
		out[i] = w.Key
	}
	return out
}
