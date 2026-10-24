// Package pricing 是模型 1 倍价格同步的规则：同步哪些设置项、怎么比较、按什么顺序写入。
//
// 这里只有纯函数，不碰数据库和网络；读写由 source / target / engine 负责。
// 删除本包和配置里的 pricing 段即可完整移除这个功能。
//
// NewAPI 把每个模型的价格存在 options 表的 11 个设置项里，值都是"模型名 → 价格"的 JSON 对象
// （与 NewAPI model/model_pricing_config.go 的 modelPricingOptionKeys 一致）：
//
//	表达式计费：billing_setting.billing_mode / billing_expr / plugin_billing_expr
//	旧的倍率、按次计费：ModelRatio、ModelPrice、CompletionRatio……（还没迁移到表达式的模型）
//
// 子站与主站完全一致：主站库里有的设置项整份照搬，子站多出来的模型价格会被删掉。
// 分组倍率不在这里，由 options 同步负责。
package pricing

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// 需要特殊处理的设置项。
const (
	KeyBillingMode       = "billing_setting.billing_mode"
	KeyBillingExpr       = "billing_setting.billing_expr"
	KeyPluginBillingExpr = "billing_setting.plugin_billing_expr"

	// KeyQuotaPerUnit 是每 1 美元对应的额度。只读取用来检查两边是否一致，永不同步：
	// 改它会让子站所有用户余额的价值跟着变。
	KeyQuotaPerUnit = "QuotaPerUnit"
	// DefaultQuotaPerUnit 是 NewAPI 的默认值（common.QuotaPerUnit），库里没有这一项时使用。
	DefaultQuotaPerUnit = 500000.0
)

// Keys 是需要同步的模型价格设置项，顺序也是逐项写入时的基本顺序（表达式另有安排，见 orderWrites）。
var Keys = []string{
	"ModelRatio", "ModelPrice", "CompletionRatio", "CacheRatio", "CreateCacheRatio",
	"ImageRatio", "AudioRatio", "AudioCompletionRatio",
	KeyPluginBillingExpr, KeyBillingExpr, KeyBillingMode,
}

// ReadKeys 返回需要从主站、子站读取的全部设置项：模型价格，加上只用于检查的 QuotaPerUnit。
func ReadKeys() []string {
	return append(append([]string{}, Keys...), KeyQuotaPerUnit)
}

// IsKey 报告 key 是否是需要同步的模型价格设置项。
func IsKey(key string) bool {
	for _, k := range Keys {
		if k == key {
			return true
		}
	}
	return false
}

// Change 是一个价格设置项的变更，Value 是要写入子站的值（主站的值）。
type Change struct {
	Key    string
	Value  string
	Exists bool // 子站库里是否已有这一项
	// 以下是按模型统计的差异，只用于日志和状态展示。
	Added   []string // 子站将新增价格的模型
	Changed []string // 价格将改变的模型
	Removed []string // 子站将删除价格的模型（主站没有）
}

// Write 是通过子站接口进行的一次写入。
type Write struct {
	Key   string
	Value string
}

// Plan 是让子站模型价格与主站一致所需的变更。
type Plan struct {
	// Changes 是有差异的设置项，按 Keys 的顺序排列。直接写库时在同一个事务里写入这些值即可。
	Changes []Change
	// Writes 是通过子站接口逐项写入时的安全顺序（见 orderWrites）。
	Writes []Write
	// NotInMaster 是子站有、主站库里却没有的设置项：主站从没保存过这一项，或主站版本还没有这个功能。
	// 读数据库无法知道主站此时用的值，所以子站这一项保持原样。
	NotInMaster []string
	// Warnings 是需要人关注的情况（不阻止其他项的同步）。
	Warnings []string
}

// Empty 报告是否没有需要写入的价格变更。
func (p Plan) Empty() bool { return len(p.Changes) == 0 }

// Diff 计算子站需要做的价格变更。master、child 是两边 options 表里 ReadKeys 范围内的原始值。
//
// 比较的是价格内容而不是原文：新版 NewAPI 每次写入价格都会把这些设置项重新编码后存回去，
// 原文和主站必然不同，按原文比较会导致每一轮都判定有差异、反复写入。
func Diff(master, child map[string]string) Plan {
	var plan Plan
	for _, key := range Keys {
		mv, inMaster := master[key]
		cv, inChild := child[key]
		if !inMaster {
			if inChild {
				plan.NotInMaster = append(plan.NotInMaster, key)
			}
			continue
		}
		mObj, err := parseObject(mv)
		if err != nil {
			plan.Warnings = append(plan.Warnings, fmt.Sprintf("主站的模型价格设置 %s 不是有效的 JSON 对象，已跳过这一项：%v", key, err))
			continue
		}
		cObj := map[string]any{}
		if inChild {
			if parsed, err := parseObject(cv); err == nil {
				if reflect.DeepEqual(mObj, parsed) {
					continue
				}
				cObj = parsed
			}
			// 子站的值解析不了：当作空表，用主站的值整体覆盖
		}
		added, changed, removed := diffEntries(mObj, cObj)
		plan.Changes = append(plan.Changes, Change{
			Key: key, Value: writeValue(mv), Exists: inChild,
			Added: added, Changed: changed, Removed: removed,
		})
	}
	plan.Writes = orderWrites(plan.Changes, child)
	if warn := checkQuotaPerUnit(master, child); warn != "" {
		plan.Warnings = append(plan.Warnings, warn)
	}
	return plan
}

// Equal 按价格内容比较两份设置值：忽略缩进、空白、键的顺序和数字写法（1 与 1.0）。
// 任一方不是 JSON 对象时退回到去掉首尾空白后的原文比较。
func Equal(a, b string) bool {
	ao, aerr := parseObject(a)
	bo, berr := parseObject(b)
	if aerr != nil || berr != nil {
		return strings.TrimSpace(a) == strings.TrimSpace(b)
	}
	return reflect.DeepEqual(ao, bo)
}

// parseObject 把设置值解析成 JSON 对象。空值和 null 视为空对象：NewAPI 读到它们时价格表同样为空。
func parseObject(s string) (map[string]any, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "null" {
		return map[string]any{}, nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, err
	}
	if m == nil {
		m = map[string]any{}
	}
	return m, nil
}

// writeValue 返回写入子站的值。空值和 null 写成 {}：新版 NewAPI 会拒绝不是 JSON 对象的价格设置。
func writeValue(v string) string {
	if t := strings.TrimSpace(v); t == "" || t == "null" {
		return "{}"
	}
	return v
}

// orderWrites 返回通过子站接口逐项写入时的顺序。
//
// 新版 NewAPI 每写一项都会按模型校验：计费模式为 tiered_expr 的模型必须已经有表达式。
// 逐项写入时，"模型改成表达式计费"要求先写表达式、再写计费模式；"模型不再用表达式"要求
// 先改计费模式、再删表达式。两种情况可能在同一次同步里同时出现，所以表达式分两步写：
//
//  1. 先写"子站原有 ∪ 主站"的表达式（只增不删，主站的覆盖子站的）
//  2. 写其他有变化的设置项（包括计费模式）
//  3. 再写主站的精确表达式，删掉多余的；第 1 步已经等于主站时省略
func orderWrites(changes []Change, child map[string]string) []Write {
	var exprChange *Change
	var others []Write
	for i := range changes {
		c := &changes[i]
		if c.Key == KeyBillingExpr {
			exprChange = c
			continue
		}
		others = append(others, Write{Key: c.Key, Value: c.Value})
	}
	if exprChange == nil {
		return others
	}
	union, needsSecondStep := exprUnion(exprChange.Value, child[KeyBillingExpr])
	writes := make([]Write, 0, len(others)+2)
	writes = append(writes, Write{Key: KeyBillingExpr, Value: union})
	writes = append(writes, others...)
	if needsSecondStep {
		writes = append(writes, Write{Key: KeyBillingExpr, Value: exprChange.Value})
	}
	return writes
}

// exprUnion 返回"子站原有 ∪ 主站"的表达式，以及它是否与主站不同（需要第二步删除）。
func exprUnion(masterValue, childValue string) (string, bool) {
	mObj, err := parseObject(masterValue)
	if err != nil {
		return masterValue, false
	}
	cObj, err := parseObject(childValue)
	if err != nil || len(cObj) == 0 {
		return masterValue, false
	}
	union := make(map[string]any, len(mObj)+len(cObj))
	for k, v := range cObj {
		union[k] = v
	}
	for k, v := range mObj {
		union[k] = v
	}
	if reflect.DeepEqual(union, mObj) {
		return masterValue, false
	}
	encoded, err := marshal(union)
	if err != nil {
		return masterValue, false
	}
	return encoded, true
}

// marshal 编码 JSON，不转义 < > &（表达式里常有 >= 和 &&，保持可读）。
func marshal(v any) (string, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return "", err
	}
	return strings.TrimRight(buf.String(), "\n"), nil
}

func diffEntries(master, child map[string]any) (added, changed, removed []string) {
	for name, mv := range master {
		cv, ok := child[name]
		switch {
		case !ok:
			added = append(added, name)
		case !reflect.DeepEqual(mv, cv):
			changed = append(changed, name)
		}
	}
	for name := range child {
		if _, ok := master[name]; !ok {
			removed = append(removed, name)
		}
	}
	sort.Strings(added)
	sort.Strings(changed)
	sort.Strings(removed)
	return added, changed, removed
}

// checkQuotaPerUnit 检查两边每 1 美元对应的额度是否一致，不一致时返回警告。
func checkQuotaPerUnit(master, child map[string]string) string {
	m, mok := quotaPerUnit(master)
	c, cok := quotaPerUnit(child)
	if !mok || !cok || m == c {
		return ""
	}
	return fmt.Sprintf("子站的 QuotaPerUnit（每 1 美元对应的额度）是 %s，主站是 %s：同一个倍率在两边代表的钱不一样。"+
		"同步服务不会修改这一项（改了会让子站用户余额的价值跟着变），请确认后在子站手动调整",
		formatNumber(c), formatNumber(m))
}

func quotaPerUnit(opts map[string]string) (float64, bool) {
	v, ok := opts[KeyQuotaPerUnit]
	if !ok || strings.TrimSpace(v) == "" {
		return DefaultQuotaPerUnit, true
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

func formatNumber(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }

// Detail 返回一行变更说明：设置项名称与涉及的模型名（模型名不是敏感信息，价格数值不写进日志）。
func (c Change) Detail() string {
	verb := "更新"
	if !c.Exists {
		verb = "新增"
	}
	var parts []string
	for _, p := range []struct {
		label string
		names []string
	}{{"新增", c.Added}, {"修改", c.Changed}, {"删除", c.Removed}} {
		if len(p.names) > 0 {
			parts = append(parts, fmt.Sprintf("%s %d（%s）", p.label, len(p.names), previewNames(p.names, 5)))
		}
	}
	if len(parts) == 0 {
		return fmt.Sprintf("%s模型价格 %s", verb, c.Key)
	}
	return fmt.Sprintf("%s模型价格 %s：%s", verb, c.Key, strings.Join(parts, "，"))
}

// Details 返回逐项变更说明。
func (p Plan) Details() []string {
	out := make([]string, 0, len(p.Changes))
	for _, c := range p.Changes {
		out = append(out, c.Detail())
	}
	return out
}

func previewNames(names []string, n int) string {
	if len(names) <= n {
		return strings.Join(names, ", ")
	}
	return strings.Join(names[:n], ", ") + fmt.Sprintf(" 等 %d 个", len(names))
}
