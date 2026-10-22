// Package newapidb 封装对 NewAPI 三张相关表的通用读取逻辑：
// channels（渠道）、abilities（渠道能力/路由表）、options（系统设置，分组设置存在这里）。
// 主站读取与子站读取共用这里的代码，保证两边的规范化方式完全一致。
package newapidb

import (
	"context"
	"fmt"
	"strings"

	"github.com/10dian-ai/newapi-channel-synchronization/internal/dbx"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/snapshot"
)

// NewAPI 表名与关键列名。
const (
	TableChannels  = "channels"
	TableAbilities = "abilities"
	TableOptions   = "options"

	ColID          = "id"
	ColKey         = "key"
	ColStatus      = "status"
	ColChannelInfo = "channel_info"
	ColModels      = "models"
	ColGroup       = "group"
	ColPriority    = "priority"
	ColWeight      = "weight"
	ColTag         = "tag"

	OptionKeyCol   = "key"
	OptionValueCol = "value"
)

// NewAPI 渠道状态值（common.ChannelStatus*）。
const (
	StatusEnabled          = 1
	StatusManuallyDisabled = 2
	StatusAutoDisabled     = 3
)

// Schema 是与同步相关的三张表结构。
type Schema struct {
	Channels  dbx.Columns
	Abilities dbx.Columns
	Options   dbx.Columns
}

// Fingerprint 返回整体结构指纹。
func (s Schema) Fingerprint() string {
	return s.Channels.Fingerprint() + "-" + s.Abilities.Fingerprint() + "-" + s.Options.Fingerprint()
}

// LoadSchema 读取三张表的结构并检查必要的列是否存在。
func LoadSchema(ctx context.Context, q dbx.Queryer, d dbx.Dialect) (Schema, error) {
	var s Schema
	var err error
	if s.Channels, err = dbx.LoadColumns(ctx, q, d, TableChannels); err != nil {
		return s, err
	}
	if s.Abilities, err = dbx.LoadColumns(ctx, q, d, TableAbilities); err != nil {
		return s, err
	}
	if s.Options, err = dbx.LoadColumns(ctx, q, d, TableOptions); err != nil {
		return s, err
	}
	if len(s.Channels) == 0 {
		return s, fmt.Errorf("没有找到 %s 表，请确认连接的是 NewAPI 数据库", TableChannels)
	}
	for _, col := range []string{ColID, ColKey, ColStatus, ColModels, ColGroup} {
		if !s.Channels.Has(col) {
			return s, fmt.Errorf("%s 表缺少 %s 列，请确认连接的是 NewAPI 数据库", TableChannels, col)
		}
	}
	if len(s.Options) > 0 && (!s.Options.Has(OptionKeyCol) || !s.Options.Has(OptionValueCol)) {
		return s, fmt.Errorf("%s 表缺少 key/value 列", TableOptions)
	}
	return s, nil
}

// ReadChannels 读取 channels 表的指定列（必须包含 id）。
func ReadChannels(ctx context.Context, q dbx.Queryer, d dbx.Dialect, cols dbx.Columns) (snapshot.Table, error) {
	table := snapshot.Table{Columns: cols, Rows: map[int64]snapshot.Row{}}
	quoted := make([]string, len(cols))
	idIndex := -1
	for i, c := range cols {
		quoted[i] = d.Quote(c.Name)
		if c.Name == ColID {
			idIndex = i
		}
	}
	if idIndex < 0 {
		return table, fmt.Errorf("读取渠道时缺少 id 列")
	}
	query := "SELECT " + strings.Join(quoted, ", ") + " FROM " + d.Quote(TableChannels) + " ORDER BY " + d.Quote(ColID)
	rows, err := q.QueryContext(ctx, query)
	if err != nil {
		return table, fmt.Errorf("读取渠道失败：%w", err)
	}
	defer rows.Close()
	values := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range values {
		ptrs[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return table, fmt.Errorf("读取渠道失败：%w", err)
		}
		row := make(snapshot.Row, len(cols))
		for i, c := range cols {
			row[c.Name] = dbx.Normalize(values[i])
		}
		id := row.Int(ColID, -1)
		if id < 0 {
			return table, fmt.Errorf("渠道 id 无法解析：%v", values[idIndex])
		}
		table.Rows[id] = row
	}
	if err := rows.Err(); err != nil {
		return table, fmt.Errorf("读取渠道失败：%w", err)
	}
	return table, nil
}

// OptionMatcher 决定哪些 option 属于同步范围。
type OptionMatcher struct {
	Keys     []string
	Prefixes []string
}

// Match 报告 key 是否在同步范围内。
func (m OptionMatcher) Match(key string) bool {
	for _, k := range m.Keys {
		if key == k {
			return true
		}
	}
	for _, p := range m.Prefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

// Empty 报告是否没有配置任何 key。
func (m OptionMatcher) Empty() bool { return len(m.Keys) == 0 && len(m.Prefixes) == 0 }

// WhereClause 生成用于预筛选的 WHERE 子句与参数（LIKE 的 _ 通配可能多匹配，结果会再用 Match 精确过滤）。
func (m OptionMatcher) WhereClause(d dbx.Dialect, startArg int) (string, []any) {
	var parts []string
	var args []any
	n := startArg
	keyCol := d.Quote(OptionKeyCol)
	if len(m.Keys) > 0 {
		ph := make([]string, len(m.Keys))
		for i, k := range m.Keys {
			ph[i] = d.Placeholder(n)
			n++
			args = append(args, k)
		}
		parts = append(parts, keyCol+" IN ("+strings.Join(ph, ", ")+")")
	}
	for _, p := range m.Prefixes {
		parts = append(parts, keyCol+" LIKE "+d.Placeholder(n))
		n++
		args = append(args, p+"%")
	}
	if len(parts) == 0 {
		return "1 = 0", nil
	}
	return "(" + strings.Join(parts, " OR ") + ")", args
}

// ReadOptions 读取同步范围内的设置项。
func ReadOptions(ctx context.Context, q dbx.Queryer, d dbx.Dialect, m OptionMatcher) (map[string]string, error) {
	out := map[string]string{}
	if m.Empty() {
		return out, nil
	}
	where, args := m.WhereClause(d, 1)
	query := "SELECT " + d.Quote(OptionKeyCol) + ", " + d.Quote(OptionValueCol) + " FROM " + d.Quote(TableOptions) + " WHERE " + where
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("读取设置失败：%w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var k, v any
		if err := rows.Scan(&k, &v); err != nil {
			return nil, fmt.Errorf("读取设置失败：%w", err)
		}
		key := dbx.Normalize(k)
		if key == nil || !m.Match(*key) {
			continue
		}
		if val := dbx.Normalize(v); val != nil {
			out[*key] = *val
		} else {
			out[*key] = ""
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("读取设置失败：%w", err)
	}
	return out, nil
}

// ReadAbilities 读取能力表中用于校验的字段。
func ReadAbilities(ctx context.Context, q dbx.Queryer, d dbx.Dialect, cols dbx.Columns) ([]snapshot.AbilityRow, error) {
	want := []string{"group", "model", "channel_id", "enabled", "priority", "weight"}
	exprs := make([]string, len(want))
	for i, c := range want {
		if cols.Has(c) {
			exprs[i] = d.Quote(c)
		} else {
			exprs[i] = "NULL"
		}
	}
	rows, err := q.QueryContext(ctx, "SELECT "+strings.Join(exprs, ", ")+" FROM "+d.Quote(TableAbilities))
	if err != nil {
		return nil, fmt.Errorf("读取能力表失败：%w", err)
	}
	defer rows.Close()
	out := []snapshot.AbilityRow{}
	values := make([]any, len(want))
	ptrs := make([]any, len(want))
	for i := range values {
		ptrs[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("读取能力表失败：%w", err)
		}
		r := snapshot.Row{}
		for i, c := range want {
			r[c] = dbx.Normalize(values[i])
		}
		out = append(out, snapshot.AbilityRow{
			Group:     r.String("group"),
			Model:     r.String("model"),
			ChannelID: r.Int("channel_id", -1),
			Enabled:   r.Int("enabled", 0) == 1,
			Priority:  r.Int("priority", 0),
			Weight:    r.Int("weight", 0),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("读取能力表失败：%w", err)
	}
	return out, nil
}
