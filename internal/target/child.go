// Package target 负责子站数据库：读取现状、在一个事务里应用变更计划。
package target

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/10dian-ai/newapi-channel-synchronization/internal/dbx"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/newapidb"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/reconcile"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/snapshot"
)

// 能力表中同步服务认识的列。
var knownAbilityColumns = map[string]bool{
	"group": true, "model": true, "channel_id": true, "enabled": true,
	"priority": true, "weight": true, "tag": true,
}

// Store 是一个子站数据库。
type Store struct {
	db      *dbx.DB
	matcher newapidb.OptionMatcher // 分组设置的范围
	pricing newapidb.OptionMatcher // 模型价格设置项的范围，为空表示不同步价格
}

// New 创建子站存储。
func New(db *dbx.DB, matcher, pricing newapidb.OptionMatcher) *Store {
	return &Store{db: db, matcher: matcher, pricing: pricing}
}

// DB 返回底层连接（用于身份检查）。
func (s *Store) DB() *dbx.DB { return s.db }

// Schema 读取子站表结构。
func (s *Store) Schema(ctx context.Context) (newapidb.Schema, error) {
	return newapidb.LoadSchema(ctx, s.db, s.db.Dialect)
}

// LoadOptions 控制读取哪些内容。
type LoadOptions struct {
	Options   bool // 读取同步范围内的设置
	Pricing   bool // 读取模型价格设置项
	Abilities bool // 读取能力表（用于校验）
}

// Load 读取子站当前的渠道、同步范围内的设置、模型价格，以及（可选的）能力表。
func (s *Store) Load(ctx context.Context, schema newapidb.Schema, opt LoadOptions) (*snapshot.Child, error) {
	channels, err := newapidb.ReadChannels(ctx, s.db, s.db.Dialect, schema.Channels)
	if err != nil {
		return nil, err
	}
	state := &snapshot.Child{Channels: channels, Options: map[string]string{}, Pricing: map[string]string{}}
	if opt.Options && len(schema.Options) > 0 {
		if state.Options, err = newapidb.ReadOptions(ctx, s.db, s.db.Dialect, s.matcher); err != nil {
			return nil, err
		}
	}
	if opt.Pricing && len(schema.Options) > 0 && !s.pricing.Empty() {
		if state.Pricing, err = newapidb.ReadOptions(ctx, s.db, s.db.Dialect, s.pricing); err != nil {
			return nil, fmt.Errorf("读取模型价格失败：%w", err)
		}
	}
	if opt.Abilities && len(schema.Abilities) > 0 {
		if state.Abilities, err = newapidb.ReadAbilities(ctx, s.db, s.db.Dialect, schema.Abilities); err != nil {
			return nil, err
		}
	}
	return state, nil
}

// AbilitiesWritable 报告同步服务能否自行重建该子站的能力表。
// 如果子站版本的能力表多了不允许为空、又没有默认值的新列，就只能交给子站自己的修复接口。
func AbilitiesWritable(cols dbx.Columns) (bool, string) {
	if len(cols) == 0 {
		return false, "子站没有 abilities 表"
	}
	for _, c := range cols {
		if !knownAbilityColumns[c.Name] && !c.Nullable && !c.HasDefault {
			return false, fmt.Sprintf("子站 abilities 表有未知的必填列 %s", c.Name)
		}
	}
	for _, need := range []string{"group", "model", "channel_id", "enabled"} {
		if !cols.Has(need) {
			return false, fmt.Sprintf("子站 abilities 表缺少 %s 列", need)
		}
	}
	return true, ""
}

// ApplyOptions 控制 Apply 的行为。
type ApplyOptions struct {
	// RebuildAbilities 是否在同一事务里重建受影响渠道的能力记录。
	RebuildAbilities bool
	// WriteOptions 是否在同一事务里写入分组设置与模型价格（options_via=db 时）。
	WriteOptions bool
}

// Result 是一次写入的结果。
type Result struct {
	Inserted, Updated, Deleted, AbilityRows, Options, Pricing int
}

// Apply 在一个事务中写入渠道、能力表和（可选的）分组设置与模型价格。任一步失败都会整体回滚。
func (s *Store) Apply(ctx context.Context, plan *reconcile.Plan, schema newapidb.Schema, opt ApplyOptions) (Result, error) {
	var res Result
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return res, fmt.Errorf("开启事务失败：%w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	d := s.db.Dialect
	for _, w := range plan.Inserts {
		if err := insertRow(ctx, tx, d, schema.Channels, w); err != nil {
			return res, fmt.Errorf("新增渠道 #%d 失败：%w", w.ID, err)
		}
		res.Inserted++
	}
	for _, w := range plan.Updates {
		if err := updateRow(ctx, tx, d, schema.Channels, w); err != nil {
			return res, fmt.Errorf("更新渠道 #%d 失败：%w", w.ID, err)
		}
		res.Updated++
	}
	if len(plan.Deletes) > 0 {
		if err := deleteByIDs(ctx, tx, d, newapidb.TableChannels, newapidb.ColID, plan.Deletes); err != nil {
			return res, fmt.Errorf("删除渠道失败：%w", err)
		}
		res.Deleted = len(plan.Deletes)
	}

	if opt.RebuildAbilities && len(plan.Abilities) > 0 {
		n, err := rebuildAbilities(ctx, tx, d, schema, plan.Abilities)
		if err != nil {
			return res, fmt.Errorf("重建能力表失败：%w", err)
		}
		res.AbilityRows = n
	}

	if opt.WriteOptions {
		for _, o := range plan.Options {
			if err := upsertOption(ctx, tx, d, o.Key, o.Value); err != nil {
				return res, fmt.Errorf("写入设置 %s 失败：%w", o.Key, err)
			}
			res.Options++
		}
		// 同一个事务里一次写完，子站不会看到写了一半的价格，所以不需要逐项写入时的特殊顺序
		for _, c := range plan.Pricing.Changes {
			if err := upsertOption(ctx, tx, d, c.Key, c.Value); err != nil {
				return res, fmt.Errorf("写入模型价格 %s 失败：%w", c.Key, err)
			}
			res.Pricing++
		}
	}

	if len(plan.Inserts) > 0 {
		if q := d.ResetSequenceSQL(newapidb.TableChannels, newapidb.ColID); q != "" {
			if _, err := tx.ExecContext(ctx, q); err != nil {
				return res, fmt.Errorf("修正渠道 id 序列失败：%w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return res, fmt.Errorf("提交事务失败：%w", err)
	}
	committed = true
	return res, nil
}

func insertRow(ctx context.Context, ex dbx.Execer, d dbx.Dialect, cols dbx.Columns, w reconcile.RowWrite) error {
	names := w.Columns()
	quoted := make([]string, 0, len(names))
	ph := make([]string, 0, len(names))
	args := make([]any, 0, len(names))
	for _, name := range names {
		col, ok := cols.Get(name)
		if !ok {
			continue
		}
		v, err := dbx.DriverValue(col, w.Values[name])
		if err != nil {
			return err
		}
		quoted = append(quoted, d.Quote(name))
		args = append(args, v)
		ph = append(ph, d.Placeholder(len(args)))
	}
	q := "INSERT INTO " + d.Quote(newapidb.TableChannels) + " (" + strings.Join(quoted, ", ") + ") VALUES (" + strings.Join(ph, ", ") + ")"
	_, err := ex.ExecContext(ctx, q, args...)
	return err
}

func updateRow(ctx context.Context, ex dbx.Execer, d dbx.Dialect, cols dbx.Columns, w reconcile.RowWrite) error {
	names := w.Columns()
	sets := make([]string, 0, len(names))
	args := make([]any, 0, len(names)+1)
	for _, name := range names {
		col, ok := cols.Get(name)
		if !ok || name == newapidb.ColID {
			continue
		}
		v, err := dbx.DriverValue(col, w.Values[name])
		if err != nil {
			return err
		}
		args = append(args, v)
		sets = append(sets, d.Quote(name)+" = "+d.Placeholder(len(args)))
	}
	if len(sets) == 0 {
		return nil
	}
	args = append(args, w.ID)
	q := "UPDATE " + d.Quote(newapidb.TableChannels) + " SET " + strings.Join(sets, ", ") + " WHERE " + d.Quote(newapidb.ColID) + " = " + d.Placeholder(len(args))
	_, err := ex.ExecContext(ctx, q, args...)
	return err
}

func deleteByIDs(ctx context.Context, ex dbx.Execer, d dbx.Dialect, table, col string, ids []int64) error {
	const chunk = 200
	for start := 0; start < len(ids); start += chunk {
		end := min(start+chunk, len(ids))
		ph := make([]string, 0, end-start)
		args := make([]any, 0, end-start)
		for _, id := range ids[start:end] {
			args = append(args, id)
			ph = append(ph, d.Placeholder(len(args)))
		}
		q := "DELETE FROM " + d.Quote(table) + " WHERE " + d.Quote(col) + " IN (" + strings.Join(ph, ", ") + ")"
		if _, err := ex.ExecContext(ctx, q, args...); err != nil {
			return err
		}
	}
	return nil
}

// rebuildAbilities 先删除受影响渠道的全部能力记录，再按渠道"此刻"的状态重新生成，
// 与 NewAPI 后台保存渠道时的做法一致。
//
// 渠道行在同一个事务里用 SELECT ... FOR UPDATE 重新读取并加锁，而不是用事务开始前读到的旧值：
// 子站 NewAPI 可能刚好在这期间自动禁用了某个渠道，用旧值会生成 enabled=1 的能力记录，
// 让流量继续打到已禁用的渠道上。
func rebuildAbilities(ctx context.Context, tx *sql.Tx, d dbx.Dialect, schema newapidb.Schema, targets map[int64]snapshot.Row) (int, error) {
	cols := schema.Abilities
	ids := make([]int64, 0, len(targets))
	var live []int64
	for id, row := range targets {
		ids = append(ids, id)
		if row != nil {
			live = append(live, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	sort.Slice(live, func(i, j int) bool { return live[i] < live[j] })

	fresh, err := lockChannels(ctx, tx, d, schema.Channels, live)
	if err != nil {
		return 0, err
	}
	if err := deleteByIDs(ctx, tx, d, newapidb.TableAbilities, "channel_id", ids); err != nil {
		return 0, err
	}

	var all []newapidb.Ability
	for _, id := range live {
		if row, ok := fresh[id]; ok {
			all = append(all, newapidb.BuildAbilities(id, row)...)
		}
	}
	if len(all) == 0 {
		return 0, nil
	}
	insertCols := make([]string, 0, 7)
	for _, c := range []string{"group", "model", "channel_id", "enabled", "priority", "weight", "tag"} {
		if cols.Has(c) {
			insertCols = append(insertCols, c)
		}
	}
	quoted := make([]string, len(insertCols))
	for i, c := range insertCols {
		quoted[i] = d.Quote(c)
	}
	written := 0
	const chunk = 100
	for start := 0; start < len(all); start += chunk {
		end := min(start+chunk, len(all))
		var values []string
		var args []any
		for _, a := range all[start:end] {
			ph := make([]string, len(insertCols))
			for i, c := range insertCols {
				col, _ := cols.Get(c)
				args = append(args, abilityValue(col, a))
				ph[i] = d.Placeholder(len(args))
			}
			values = append(values, "("+strings.Join(ph, ", ")+")")
		}
		// 与 NewAPI 一样忽略主键冲突：在大小写不敏感的排序规则下，gpt-4o 与 GPT-4o 是同一个主键
		q := "INSERT INTO " + d.Quote(newapidb.TableAbilities) + " (" + strings.Join(quoted, ", ") + ") VALUES " +
			strings.Join(values, ", ") + d.OnConflictDoNothing("channel_id")
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return written, err
		}
		written += end - start
	}
	return written, nil
}

// lockChannels 在事务内重新读取并锁定指定渠道中与能力表相关的列。
func lockChannels(ctx context.Context, tx *sql.Tx, d dbx.Dialect, chCols dbx.Columns, ids []int64) (map[int64]snapshot.Row, error) {
	out := map[int64]snapshot.Row{}
	if len(ids) == 0 {
		return out, nil
	}
	var cols dbx.Columns
	for _, name := range []string{newapidb.ColID, newapidb.ColModels, newapidb.ColGroup, newapidb.ColStatus, newapidb.ColPriority, newapidb.ColWeight, newapidb.ColTag} {
		if c, ok := chCols.Get(name); ok {
			cols = append(cols, c)
		}
	}
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = d.Quote(c.Name)
	}
	const chunk = 200
	for start := 0; start < len(ids); start += chunk {
		end := min(start+chunk, len(ids))
		ph := make([]string, 0, end-start)
		args := make([]any, 0, end-start)
		for _, id := range ids[start:end] {
			args = append(args, id)
			ph = append(ph, d.Placeholder(len(args)))
		}
		q := "SELECT " + strings.Join(quoted, ", ") + " FROM " + d.Quote(newapidb.TableChannels) +
			" WHERE " + d.Quote(newapidb.ColID) + " IN (" + strings.Join(ph, ", ") + ") FOR UPDATE"
		rows, err := tx.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		for rows.Next() {
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				return nil, err
			}
			row := snapshot.Row{}
			for i, c := range cols {
				row[c.Name] = dbx.Normalize(values[i])
			}
			out[row.Int(newapidb.ColID, -1)] = row
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	return out, nil
}

func abilityValue(col dbx.Column, a newapidb.Ability) any {
	switch col.Name {
	case "group":
		return a.Group
	case "model":
		return a.Model
	case "channel_id":
		return a.ChannelID
	case "enabled":
		if col.Kind == dbx.KindBool {
			return a.Enabled
		}
		if a.Enabled {
			return int64(1)
		}
		return int64(0)
	case "priority":
		return a.Priority
	case "weight":
		return a.Weight
	case "tag":
		if a.Tag == nil {
			return nil
		}
		return *a.Tag
	}
	return nil
}

func upsertOption(ctx context.Context, tx *sql.Tx, d dbx.Dialect, key, value string) error {
	keyCol, valCol, table := d.Quote(newapidb.OptionKeyCol), d.Quote(newapidb.OptionValueCol), d.Quote(newapidb.TableOptions)
	var count int64
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE "+keyCol+" = "+d.Placeholder(1), key).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		_, err := tx.ExecContext(ctx, "UPDATE "+table+" SET "+valCol+" = "+d.Placeholder(1)+" WHERE "+keyCol+" = "+d.Placeholder(2), value, key)
		return err
	}
	_, err := tx.ExecContext(ctx, "INSERT INTO "+table+" ("+keyCol+", "+valCol+") VALUES ("+d.Placeholder(1)+", "+d.Placeholder(2)+")", key, value)
	return err
}

// ErrSameDatabase 表示子站与主站是同一个数据库。
var ErrSameDatabase = errors.New("子站数据库与主站是同一个数据库，主库必须保持只读")
