package reconcile

import (
	"fmt"
	"sort"
	"strings"

	"github.com/10dian-ai/newapi-channel-synchronization/internal/newapidb"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/pricing"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/snapshot"
)

// abilityColumns 是会影响能力表（路由）的渠道列，这些列变化时才需要重建该渠道的能力记录。
var abilityColumns = map[string]bool{
	newapidb.ColModels:   true,
	newapidb.ColGroup:    true,
	newapidb.ColStatus:   true,
	newapidb.ColPriority: true,
	newapidb.ColWeight:   true,
	newapidb.ColTag:      true,
}

// RowWrite 是对一条渠道的写入。
type RowWrite struct {
	ID int64
	// Values 是要写入的列与值。插入时包含全部要写的列，更新时只包含变化的列。
	Values snapshot.Row
}

// Columns 返回排好序的列名。
func (w RowWrite) Columns() []string {
	cols := make([]string, 0, len(w.Values))
	for c := range w.Values {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	return cols
}

// OptionChange 是一项分组设置的变更。
type OptionChange struct {
	Key    string
	Value  string
	Exists bool // 子站是否已有这一项
}

// Plan 是把某个子站同步到主站状态所需的最小变更集合。
type Plan struct {
	Inserts []RowWrite
	Updates []RowWrite
	Deletes []int64
	// Abilities 是需要重建能力记录的渠道及其写入后的完整行；值为 nil 表示渠道被删除。
	Abilities map[int64]snapshot.Row
	Options   []OptionChange
	// OrphanOptions 是子站有、主站没有的同步范围内设置项（只记录，不删除）。
	OrphanOptions []string
	// Pricing 是模型价格的变更（规则见 pricing 包）。
	Pricing pricing.Plan
	// AbilityRepairs 是渠道本身无需变更、但能力表与渠道不一致而需要修复的渠道。
	AbilityRepairs []int64
	// DeleteBlocked 是被防误删拦下的删除数量。
	DeleteBlocked int
	// Warnings 是需要人关注但不阻止同步的情况。
	Warnings []string
}

// ChannelsChanged 报告渠道是否有变更。
func (p *Plan) ChannelsChanged() bool {
	return len(p.Inserts)+len(p.Updates)+len(p.Deletes) > 0
}

// Empty 报告是否没有任何需要写入的变更。
func (p *Plan) Empty() bool {
	return !p.ChannelsChanged() && len(p.Options) == 0 && len(p.AbilityRepairs) == 0 && p.Pricing.Empty()
}

// Summary 返回一行可读摘要，不包含任何字段值（渠道 Key 等敏感信息不会出现在日志里）。
func (p *Plan) Summary() string {
	out := fmt.Sprintf("新增渠道 %d，更新渠道 %d，删除渠道 %d，重建能力 %d，分组设置 %d，模型价格 %d",
		len(p.Inserts), len(p.Updates), len(p.Deletes), len(p.Abilities), len(p.Options), len(p.Pricing.Changes))
	if len(p.AbilityRepairs) > 0 {
		out += fmt.Sprintf("（其中修复能力表 %d）", len(p.AbilityRepairs))
	}
	return out
}

// Details 返回逐项变更说明（只有列名，没有值）。
func (p *Plan) Details() []string {
	var out []string
	for _, w := range p.Inserts {
		out = append(out, fmt.Sprintf("新增渠道 #%d（%s）", w.ID, w.Values.String("name")))
	}
	for _, w := range p.Updates {
		out = append(out, fmt.Sprintf("更新渠道 #%d：%s", w.ID, strings.Join(w.Columns(), ", ")))
	}
	for _, id := range p.Deletes {
		out = append(out, fmt.Sprintf("删除渠道 #%d", id))
	}
	for _, id := range p.AbilityRepairs {
		out = append(out, fmt.Sprintf("修复渠道 #%d 的能力表", id))
	}
	for _, o := range p.Options {
		verb := "新增"
		if o.Exists {
			verb = "更新"
		}
		out = append(out, fmt.Sprintf("%s设置 %s", verb, o.Key))
	}
	return append(out, p.Pricing.Details()...)
}

// DiffOptions 控制差异计算。
type DiffOptions struct {
	StatusMode    string
	DeleteMissing bool
	SyncOptions   bool
	// SyncPricing 为 true 时让子站模型价格与主站完全一致。
	SyncPricing bool
	// ExcludeType 返回 true 的渠道类型不参与同步（子站完全自己管理）。
	ExcludeType func(t int64) bool
	// DeleteGuardRatio / DeleteGuardMin 防误删阈值，见配置说明；DeleteGuardRatio 为 0 或 1 表示不拦截。
	DeleteGuardRatio float64
	DeleteGuardMin   int
	// CheckAbilities 为 true 时校验子站能力表与渠道是否一致，不一致的渠道会被修复。
	// 只在同步服务负责能力表（未使用子站修复接口）时开启。
	CheckAbilities bool
}

func (o DiffOptions) excluded(t int64) bool { return o.ExcludeType != nil && o.ExcludeType(t) }

// Diff 计算子站需要做的变更。
func Diff(master *snapshot.Master, child *snapshot.Child, cp ColumnPlan, opt DiffOptions) *Plan {
	plan := &Plan{Abilities: map[int64]snapshot.Row{}}

	for _, id := range master.Channels.IDs() {
		mRow := master.Channels.Rows[id]
		if opt.excluded(mRow.Int(colType, -1)) {
			continue
		}
		cRow, exists := child.Channels.Rows[id]
		if exists && opt.excluded(cRow.Int(colType, -1)) {
			plan.Warnings = append(plan.Warnings, fmt.Sprintf("渠道 #%d 在子站上是不参与同步的类型（由子站自己管理），与主站的同 id 渠道冲突，已跳过", id))
			continue
		}
		desired, warns := desiredRow(id, mRow, cRow, exists, cp, opt.StatusMode)
		plan.Warnings = append(plan.Warnings, warns...)
		if !exists {
			values := desired.Clone()
			for col, v := range cp.InsertFill {
				if _, ok := values[col]; !ok {
					values[col] = v
				}
			}
			values[newapidb.ColID] = mRow[newapidb.ColID]
			plan.Inserts = append(plan.Inserts, RowWrite{ID: id, Values: values})
			plan.Abilities[id] = abilitySource(cRow, values)
			continue
		}
		changes := snapshot.Row{}
		touchesAbilities := false
		for col, v := range desired {
			if !cp.Equal(col, cRow[col], v) {
				changes[col] = v
				if abilityColumns[col] {
					touchesAbilities = true
				}
			}
		}
		if len(changes) == 0 {
			continue
		}
		plan.Updates = append(plan.Updates, RowWrite{ID: id, Values: changes})
		if touchesAbilities {
			plan.Abilities[id] = abilitySource(cRow, changes)
		}
	}

	if opt.DeleteMissing {
		var extra []int64
		managed := 0
		for _, id := range child.Channels.IDs() {
			if opt.excluded(child.Channels.Rows[id].Int(colType, -1)) {
				continue
			}
			managed++
			if _, ok := master.Channels.Rows[id]; !ok {
				extra = append(extra, id)
			}
		}
		switch {
		case len(extra) == 0:
		case len(master.Channels.Rows) == 0:
			// 防误删：主站一条渠道都没有，多半是主站连错了库或刚初始化，不执行删除。
			plan.Warnings = append(plan.Warnings, fmt.Sprintf("主站没有任何渠道，为防止误删，跳过删除子站的 %d 条渠道", len(extra)))
			plan.DeleteBlocked = len(extra)
		case opt.DeleteGuardRatio > 0 && opt.DeleteGuardRatio < 1 && len(extra) >= opt.DeleteGuardMin && float64(len(extra)) > opt.DeleteGuardRatio*float64(managed):
			// 防误删：一次要删的太多（例如主站连到了另一个 NewAPI 库），先停下来等人确认。
			plan.Warnings = append(plan.Warnings, fmt.Sprintf(
				"本次需要删除子站 %d/%d 条渠道，超过防误删阈值（%.0f%%），已跳过删除。确认无误后把 sync.delete_guard_ratio 设为 1 再同步一次",
				len(extra), managed, opt.DeleteGuardRatio*100))
			plan.DeleteBlocked = len(extra)
		default:
			for _, id := range extra {
				plan.Deletes = append(plan.Deletes, id)
				plan.Abilities[id] = nil
			}
		}
	}

	if opt.CheckAbilities && child.Abilities != nil {
		// 只修复这次没有被写入的渠道（被写入的会整体重建）
		for _, id := range CheckAbilities(child.Channels, child.Abilities) {
			if _, planned := plan.Abilities[id]; planned {
				continue
			}
			if row, ok := child.Channels.Rows[id]; ok {
				plan.Abilities[id] = abilitySource(row, nil)
			} else {
				plan.Abilities[id] = nil
			}
			plan.AbilityRepairs = append(plan.AbilityRepairs, id)
		}
	}

	if opt.SyncOptions {
		keys := make([]string, 0, len(master.Options))
		for k := range master.Options {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			v := master.Options[k]
			cv, ok := child.Options[k]
			if ok && cv == v {
				continue
			}
			plan.Options = append(plan.Options, OptionChange{Key: k, Value: v, Exists: ok})
		}
		for k := range child.Options {
			if _, ok := master.Options[k]; !ok {
				plan.OrphanOptions = append(plan.OrphanOptions, k)
			}
		}
		sort.Strings(plan.OrphanOptions)
	}

	if opt.SyncPricing {
		plan.Pricing = pricing.Diff(master.Pricing, child.Pricing)
		plan.Warnings = append(plan.Warnings, plan.Pricing.Warnings...)
	}
	return plan
}

const colType = "type"

// desiredRow 计算子站某条渠道在所有同步列上的期望值。
func desiredRow(id int64, mRow, cRow snapshot.Row, exists bool, cp ColumnPlan, statusMode string) (snapshot.Row, []string) {
	var warns []string
	out := snapshot.Row{}
	configChanged := false
	for _, col := range cp.Synced {
		v := cp.coerce(col, mRow[col])
		if col == newapidb.ColKey && exists && keepChildCredential(mRow, cRow) {
			// 子站刷新过 Codex 凭证，比主站的新：保留子站的，避免用主站已经失效的旧令牌覆盖
			v = cRow[col]
			warns = append(warns, fmt.Sprintf("渠道 #%d（Codex）子站的凭证比主站新，已保留子站的；主站的刷新令牌可能已经失效，请在主站重新授权该渠道", id))
		}
		out[col] = v
		if exists && !cp.Equal(col, cRow[col], v) {
			configChanged = true
		}
	}
	keyChanged := false
	if exists {
		keyChanged = !cp.Equal(newapidb.ColKey, cRow[newapidb.ColKey], out[newapidb.ColKey])
	}
	if cp.Status {
		var childStatus *int64
		if exists {
			s := cRow.Int(newapidb.ColStatus, newapidb.StatusEnabled)
			// 主站改了这个渠道的配置（比如换了 Key）：子站之前的自动禁用判断已经过时，按主站状态重来
			if !configChanged {
				childStatus = &s
			}
		}
		d := DesiredStatus(statusMode, mRow.Int(newapidb.ColStatus, newapidb.StatusEnabled), childStatus)
		v := fmt.Sprint(d)
		out[newapidb.ColStatus] = &v
	}
	if cp.ChannelInfo {
		var childInfo snapshot.Value
		if exists {
			childInfo = cRow[newapidb.ColChannelInfo]
		}
		out[newapidb.ColChannelInfo] = cp.coerce(newapidb.ColChannelInfo, MergeChannelInfo(statusMode, mRow[newapidb.ColChannelInfo], childInfo, keyChanged))
	}
	return out, warns
}

// abilitySource 返回写入后该渠道与能力表相关的完整行。
func abilitySource(base, changes snapshot.Row) snapshot.Row {
	out := snapshot.Row{}
	for col := range abilityColumns {
		if v, ok := changes[col]; ok {
			out[col] = v
		} else if base != nil {
			out[col] = base[col]
		}
	}
	return out
}
