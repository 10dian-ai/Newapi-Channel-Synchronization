package reconcile

import (
	"fmt"
	"sort"
	"strings"

	"github.com/10dian-ai/newapi-channel-synchronization/internal/newapidb"
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
	// Warnings 是需要人关注但不阻止同步的情况。
	Warnings []string
}

// ChannelsChanged 报告渠道是否有变更。
func (p *Plan) ChannelsChanged() bool {
	return len(p.Inserts)+len(p.Updates)+len(p.Deletes) > 0
}

// Empty 报告是否没有任何需要写入的变更。
func (p *Plan) Empty() bool {
	return !p.ChannelsChanged() && len(p.Options) == 0
}

// Summary 返回一行可读摘要，不包含任何字段值（渠道 Key 等敏感信息不会出现在日志里）。
func (p *Plan) Summary() string {
	return fmt.Sprintf("新增渠道 %d，更新渠道 %d，删除渠道 %d，重建能力 %d，分组设置 %d",
		len(p.Inserts), len(p.Updates), len(p.Deletes), len(p.Abilities), len(p.Options))
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
	for _, o := range p.Options {
		verb := "新增"
		if o.Exists {
			verb = "更新"
		}
		out = append(out, fmt.Sprintf("%s设置 %s", verb, o.Key))
	}
	return out
}

// DiffOptions 控制差异计算。
type DiffOptions struct {
	StatusMode    string
	DeleteMissing bool
	SyncOptions   bool
}

// Diff 计算子站需要做的变更。
func Diff(master *snapshot.Master, child *snapshot.Child, cp ColumnPlan, opt DiffOptions) *Plan {
	plan := &Plan{Abilities: map[int64]snapshot.Row{}}

	for _, id := range master.Channels.IDs() {
		mRow := master.Channels.Rows[id]
		cRow, exists := child.Channels.Rows[id]
		desired := desiredRow(mRow, cRow, exists, cp, opt.StatusMode)
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
		for _, id := range child.Channels.IDs() {
			if _, ok := master.Channels.Rows[id]; !ok {
				extra = append(extra, id)
			}
		}
		if len(extra) > 0 && len(master.Channels.Rows) == 0 {
			// 防误删：主站一条渠道都没有，多半是主站连错了库或刚初始化，不执行删除。
			plan.Warnings = append(plan.Warnings, fmt.Sprintf("主站没有任何渠道，为防止误删，跳过删除子站的 %d 条渠道", len(extra)))
		} else {
			for _, id := range extra {
				plan.Deletes = append(plan.Deletes, id)
				plan.Abilities[id] = nil
			}
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
	return plan
}

// desiredRow 计算子站某条渠道在所有同步列上的期望值。
func desiredRow(mRow, cRow snapshot.Row, exists bool, cp ColumnPlan, statusMode string) snapshot.Row {
	out := snapshot.Row{}
	for _, col := range cp.Synced {
		out[col] = cp.coerce(col, mRow[col])
	}
	if cp.Status {
		var childStatus *int64
		if exists {
			s := cRow.Int(newapidb.ColStatus, newapidb.StatusEnabled)
			childStatus = &s
		}
		d := DesiredStatus(statusMode, mRow.Int(newapidb.ColStatus, newapidb.StatusEnabled), childStatus)
		v := fmt.Sprint(d)
		out[newapidb.ColStatus] = &v
	}
	if cp.ChannelInfo {
		var childInfo snapshot.Value
		keyChanged := false
		if exists {
			childInfo = cRow[newapidb.ColChannelInfo]
			keyChanged = !cp.Equal(newapidb.ColKey, cRow[newapidb.ColKey], mRow[newapidb.ColKey])
		}
		out[newapidb.ColChannelInfo] = cp.coerce(newapidb.ColChannelInfo, MergeChannelInfo(statusMode, mRow[newapidb.ColChannelInfo], childInfo, keyChanged))
	}
	return out
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
