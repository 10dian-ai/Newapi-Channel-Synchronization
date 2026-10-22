// Package reconcile 是同步的核心规则，全部是纯函数，不访问数据库，方便单元测试：
//
//   - 根据主站、子站各自的表结构算出"哪些列跟随主站、哪些列归子站自己"（版本差异就在这里消化）
//   - 根据主站快照和子站现状计算期望状态，并生成最小变更计划
package reconcile

import (
	"sort"

	"github.com/10dian-ai/newapi-channel-synchronization/internal/dbx"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/newapidb"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/snapshot"
)

// ColumnPlan 描述某个子站的渠道列归属。它由两边的实际表结构算出，
// 所以子站升级加了新列、主站落后几个版本，都不需要改代码。
type ColumnPlan struct {
	// Synced 跟随主站的普通列（不含 id、status、channel_info）。
	Synced []string
	// Status 为 true 表示两边都有 status 列，按状态规则同步。
	Status bool
	// ChannelInfo 为 true 表示两边都有 channel_info 列，按多 Key 合并规则同步。
	ChannelInfo bool
	// ChildOwned 子站自己维护的列（运行时数据），永不覆盖。
	ChildOwned []string
	// ChildOnly 子站有而主站没有的列（通常是子站版本更新）。这些列归子站自己，永不覆盖。
	ChildOnly []string
	// MissingInChild 主站有而子站没有的列（通常是子站版本更旧）。这些列无法同步。
	MissingInChild []string
	// InsertFill 新增渠道时必须填写的列（子站不允许为空、又没有默认值，而主站不提供）。
	InsertFill map[string]snapshot.Value

	child  dbx.Columns
	master dbx.Columns
}

// BuildColumnPlan 计算列归属。owned 是配置的子站自有列。
func BuildColumnPlan(master, child dbx.Columns, owned []string) ColumnPlan {
	ownedSet := map[string]bool{}
	for _, c := range owned {
		ownedSet[c] = true
	}
	p := ColumnPlan{InsertFill: map[string]snapshot.Value{}, child: child, master: master}
	for _, m := range master {
		if m.Name == newapidb.ColID {
			continue
		}
		if !child.Has(m.Name) {
			p.MissingInChild = append(p.MissingInChild, m.Name)
			continue
		}
		switch {
		case ownedSet[m.Name]:
			p.ChildOwned = append(p.ChildOwned, m.Name)
		case m.Name == newapidb.ColStatus:
			p.Status = true
		case m.Name == newapidb.ColChannelInfo:
			p.ChannelInfo = true
		default:
			p.Synced = append(p.Synced, m.Name)
		}
	}
	for _, c := range child {
		if c.Name == newapidb.ColID {
			continue
		}
		inMaster := master.Has(c.Name)
		if !inMaster {
			p.ChildOnly = append(p.ChildOnly, c.Name)
		}
		// 插入时不会写入的列：子站独有列、子站自有列。如果它们不允许为空又没有默认值，就填零值。
		if (!inMaster || ownedSet[c.Name]) && !c.Nullable && !c.HasDefault {
			p.InsertFill[c.Name] = dbx.ZeroValue(c.Kind)
		}
	}
	sort.Strings(p.Synced)
	sort.Strings(p.ChildOwned)
	sort.Strings(p.ChildOnly)
	sort.Strings(p.MissingInChild)
	return p
}

// ChildColumn 返回子站的列定义。
func (p ColumnPlan) ChildColumn(name string) (dbx.Column, bool) { return p.child.Get(name) }

// Managed 返回所有由同步写入的列（按名称排序）。
func (p ColumnPlan) Managed() []string {
	out := append([]string{}, p.Synced...)
	if p.Status {
		out = append(out, newapidb.ColStatus)
	}
	if p.ChannelInfo {
		out = append(out, newapidb.ColChannelInfo)
	}
	sort.Strings(out)
	return out
}

// coerce 处理"主站为 NULL 而子站该列不允许 NULL"的情况，避免写入失败。
func (p ColumnPlan) coerce(col string, v snapshot.Value) snapshot.Value {
	if v != nil {
		return v
	}
	if c, ok := p.child.Get(col); ok && !c.Nullable {
		return dbx.ZeroValue(c.Kind)
	}
	return nil
}

// isJSON 报告该列在任一侧是否为 JSON 类型（或已知存放 JSON 的列）。
func (p ColumnPlan) isJSON(col string) bool {
	if col == newapidb.ColChannelInfo {
		return true
	}
	if c, ok := p.child.Get(col); ok && c.Kind == dbx.KindJSON {
		return true
	}
	if c, ok := p.master.Get(col); ok && c.Kind == dbx.KindJSON {
		return true
	}
	return false
}

// isNumeric 报告该列在子站是否为数值或布尔类型。
func (p ColumnPlan) isNumeric(col string) bool {
	if c, ok := p.child.Get(col); ok {
		return c.Kind == dbx.KindInt || c.Kind == dbx.KindFloat || c.Kind == dbx.KindBool
	}
	return false
}
