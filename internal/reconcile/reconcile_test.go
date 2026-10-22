package reconcile

import (
	"strings"
	"testing"

	"github.com/10dian-ai/newapi-channel-synchronization/internal/config"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/dbx"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/newapidb"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/snapshot"
)

func s(v string) snapshot.Value { return &v }

func col(name string, kind dbx.Kind) dbx.Column {
	return dbx.Column{Name: name, Kind: kind, Nullable: true}
}

// 模拟主站（旧版本）与子站（新版本）的列差异
var masterCols = dbx.Columns{
	col("id", dbx.KindInt), col("type", dbx.KindInt), col("key", dbx.KindText), col("status", dbx.KindInt),
	col("name", dbx.KindText), col("weight", dbx.KindInt), col("models", dbx.KindText), col("group", dbx.KindText),
	col("priority", dbx.KindInt), col("tag", dbx.KindText), col("used_quota", dbx.KindInt),
	col("channel_info", dbx.KindJSON), col("header_override", dbx.KindText), col("other_info", dbx.KindText),
}

var childCols = dbx.Columns{
	col("id", dbx.KindInt), col("type", dbx.KindInt), col("key", dbx.KindText), col("status", dbx.KindInt),
	col("name", dbx.KindText), col("weight", dbx.KindInt), col("models", dbx.KindText), col("group", dbx.KindText),
	col("priority", dbx.KindInt), col("tag", dbx.KindText), col("used_quota", dbx.KindInt),
	col("channel_info", dbx.KindJSON), col("other_info", dbx.KindText),
	// 子站新版本多出的列：一个可空，一个必填且无默认值
	col("new_optional", dbx.KindText),
	{Name: "new_required", Kind: dbx.KindInt, Nullable: false, HasDefault: false},
}

func TestBuildColumnPlanHandlesVersionSkew(t *testing.T) {
	cp := BuildColumnPlan(masterCols, childCols, config.DefaultChildOwnedColumns)
	if !cp.Status || !cp.ChannelInfo {
		t.Fatalf("status/channel_info 应按特殊规则同步：%+v", cp)
	}
	if got := strings.Join(cp.MissingInChild, ","); got != "header_override" {
		t.Errorf("MissingInChild = %q", got)
	}
	if got := strings.Join(cp.ChildOnly, ","); got != "new_optional,new_required" {
		t.Errorf("ChildOnly = %q", got)
	}
	if got := strings.Join(cp.ChildOwned, ","); got != "other_info,used_quota" {
		t.Errorf("ChildOwned = %q", got)
	}
	for _, c := range cp.Synced {
		switch c {
		case "id", "status", "channel_info", "used_quota", "other_info", "header_override", "new_optional", "new_required":
			t.Errorf("不应出现在 Synced 中：%s", c)
		}
	}
	if v, ok := cp.InsertFill["new_required"]; !ok || *v != "0" {
		t.Errorf("new_required 应在插入时填零值，got %v", cp.InsertFill)
	}
	if _, ok := cp.InsertFill["new_optional"]; ok {
		t.Errorf("可空列不需要填充")
	}
}

func TestDesiredStatus(t *testing.T) {
	i := func(v int64) *int64 { return &v }
	cases := []struct {
		mode   string
		master int64
		child  *int64
		want   int64
	}{
		{config.StatusModeSmart, 1, nil, 1},
		{config.StatusModeSmart, 2, nil, 2},
		{config.StatusModeSmart, 3, nil, 1},   // 主站自动禁用不传播
		{config.StatusModeSmart, 1, i(3), 3},  // 子站自己的自动禁用保留
		{config.StatusModeSmart, 3, i(3), 3},  // 两边都自动禁用：保留子站的
		{config.StatusModeSmart, 1, i(2), 1},  // 主站重新启用 → 子站启用
		{config.StatusModeSmart, 2, i(3), 2},  // 主站手动禁用优先
		{config.StatusModeSmart, 9, i(1), 9},  // 未知状态透传
		{config.StatusModeMirror, 3, i(1), 3}, // mirror 完全一致
		{config.StatusModeMirror, 1, i(3), 1},
	}
	for _, c := range cases {
		if got := DesiredStatus(c.mode, c.master, c.child); got != c.want {
			t.Errorf("DesiredStatus(%s, %d, %v) = %d, want %d", c.mode, c.master, c.child, got, c.want)
		}
	}
}

func TestMergeChannelInfo(t *testing.T) {
	master := s(`{"is_multi_key":true,"multi_key_size":3,"multi_key_mode":"polling","multi_key_status_list":{"0":2,"1":3},"multi_key_disabled_reason":{"0":"manual","1":"auto-master"},"multi_key_polling_index":0}`)
	child := s(`{"is_multi_key":true,"multi_key_size":3,"multi_key_mode":"polling","multi_key_status_list":{"2":3},"multi_key_disabled_reason":{"2":"auto-child"},"multi_key_disabled_time":{"2":123},"multi_key_polling_index":2}`)

	got := MergeChannelInfo(config.StatusModeSmart, master, child, false)
	want := `{"is_multi_key":true,"multi_key_disabled_reason":{"0":"manual","2":"auto-child"},"multi_key_disabled_time":{"2":123},"multi_key_mode":"polling","multi_key_polling_index":2,"multi_key_size":3,"multi_key_status_list":{"0":2,"2":3}}`
	if got == nil || *got != want {
		t.Fatalf("smart 合并结果\n got  %v\n want %s", deref(got), want)
	}

	// 幂等：把结果当作子站现状再算一次，结果不变
	again := MergeChannelInfo(config.StatusModeSmart, master, got, false)
	if *again != *got {
		t.Fatalf("合并不是幂等的\n first  %s\n second %s", *got, *again)
	}

	// Key 变了：以主站为基础，丢弃主站自己的自动禁用，丢弃子站的旧下标
	fresh := MergeChannelInfo(config.StatusModeSmart, master, child, true)
	wantFresh := `{"is_multi_key":true,"multi_key_disabled_reason":{"0":"manual"},"multi_key_mode":"polling","multi_key_polling_index":0,"multi_key_size":3,"multi_key_status_list":{"0":2}}`
	if *fresh != wantFresh {
		t.Fatalf("Key 变化后的结果\n got  %s\n want %s", *fresh, wantFresh)
	}
	if again := MergeChannelInfo(config.StatusModeSmart, master, fresh, false); *again != *fresh {
		t.Fatalf("Key 变化后的结果不是幂等的：%s vs %s", *fresh, *again)
	}

	// mirror：原样使用主站
	if m := MergeChannelInfo(config.StatusModeMirror, master, child, false); *m != *master {
		t.Fatalf("mirror 应原样返回主站")
	}
	// 主站为 NULL
	if m := MergeChannelInfo(config.StatusModeSmart, nil, child, false); m != nil {
		t.Fatalf("主站 NULL 应得到 NULL")
	}
}

func TestMergeChannelInfoKeepsEmptyRepresentation(t *testing.T) {
	master := s(`{"is_multi_key":false,"multi_key_size":0,"multi_key_status_list":null,"multi_key_polling_index":0,"multi_key_mode":""}`)
	got := MergeChannelInfo(config.StatusModeSmart, master, nil, false)
	if !jsonEqual(*got, *master) {
		t.Fatalf("单 Key 渠道不应产生无意义的变化：%s", *got)
	}
}

func baseMaster() *snapshot.Master {
	return &snapshot.Master{
		Channels: snapshot.Table{Columns: masterCols, Rows: map[int64]snapshot.Row{
			1: {"id": s("1"), "type": s("1"), "key": s("sk-1"), "status": s("1"), "name": s("a"), "weight": s("0"),
				"models": s("gpt-4o,gpt-4o-mini"), "group": s("default,vip"), "priority": s("10"), "tag": nil,
				"used_quota": s("100"), "channel_info": s(`{"is_multi_key":false}`), "header_override": nil, "other_info": s("")},
			2: {"id": s("2"), "type": s("14"), "key": s("sk-2"), "status": s("2"), "name": s("b"), "weight": s("5"),
				"models": s("claude"), "group": s("default"), "priority": s("0"), "tag": s("t1"),
				"used_quota": s("5"), "channel_info": s(`{"is_multi_key":false}`), "header_override": nil, "other_info": s("")},
		}},
		Options: map[string]string{"GroupRatio": `{"default":1,"vip":0.8}`, "UserUsableGroups": `{"default":"默认"}`},
	}
}

func TestDiffFromEmptyChild(t *testing.T) {
	cp := BuildColumnPlan(masterCols, childCols, config.DefaultChildOwnedColumns)
	child := &snapshot.Child{Channels: snapshot.Table{Columns: childCols, Rows: map[int64]snapshot.Row{}}, Options: map[string]string{}}
	plan := Diff(baseMaster(), child, cp, DiffOptions{StatusMode: config.StatusModeSmart, DeleteMissing: true, SyncOptions: true})
	if len(plan.Inserts) != 2 || len(plan.Updates) != 0 || len(plan.Deletes) != 0 {
		t.Fatalf("plan = %s", plan.Summary())
	}
	ins := plan.Inserts[0].Values
	if ins.String("id") != "1" || ins.String("key") != "sk-1" || ins.String("status") != "1" {
		t.Errorf("插入值不对：%v", ins)
	}
	if _, ok := ins["used_quota"]; ok {
		t.Errorf("子站自有列 used_quota 不应写入")
	}
	if ins.String("new_required") != "0" {
		t.Errorf("必填新列应填零值")
	}
	if len(plan.Abilities) != 2 || plan.Abilities[1].String("models") != "gpt-4o,gpt-4o-mini" {
		t.Errorf("新增渠道应重建能力表：%v", plan.Abilities)
	}
	if len(plan.Options) != 2 {
		t.Errorf("应同步两项设置：%v", plan.Options)
	}
	if plan.Inserts[1].Values.String("status") != "2" {
		t.Errorf("手动禁用的渠道应保持禁用")
	}
}

// applyPlan 在内存中模拟写入，用来验证"写完后再算一次差异为空"。
func applyPlan(child *snapshot.Child, plan *Plan) {
	for _, w := range plan.Inserts {
		child.Channels.Rows[w.ID] = w.Values.Clone()
	}
	for _, w := range plan.Updates {
		for c, v := range w.Values {
			child.Channels.Rows[w.ID][c] = v
		}
	}
	for _, id := range plan.Deletes {
		delete(child.Channels.Rows, id)
	}
	for _, o := range plan.Options {
		child.Options[o.Key] = o.Value
	}
}

func TestDiffConvergesAndRespectsOwnership(t *testing.T) {
	cp := BuildColumnPlan(masterCols, childCols, config.DefaultChildOwnedColumns)
	opt := DiffOptions{StatusMode: config.StatusModeSmart, DeleteMissing: true, SyncOptions: true}
	master := baseMaster()
	child := &snapshot.Child{Channels: snapshot.Table{Columns: childCols, Rows: map[int64]snapshot.Row{}}, Options: map[string]string{}}
	applyPlan(child, Diff(master, child, cp, opt))
	if p := Diff(master, child, cp, opt); !p.Empty() {
		t.Fatalf("写入后应收敛，仍有：%v", p.Details())
	}

	// 子站运行时数据变化、子站自动禁用渠道 1：都不应触发同步
	child.Channels.Rows[1]["used_quota"] = s("99999")
	child.Channels.Rows[1]["status"] = s("3")
	child.Channels.Rows[1]["other_info"] = s(`{"status_reason":"401"}`)
	child.Channels.Rows[1]["new_optional"] = s("child-only")
	if p := Diff(master, child, cp, opt); !p.Empty() {
		t.Fatalf("子站自有数据不应被覆盖：%v", p.Details())
	}

	// 主站改优先级和模型：只更新这两列，并重建能力
	master.Channels.Rows[1]["priority"] = s("20")
	master.Channels.Rows[1]["models"] = s("gpt-4o")
	master.Channels.Rows[1]["used_quota"] = s("123456") // 主站运行时变化不影响
	p := Diff(master, child, cp, opt)
	if len(p.Updates) != 1 || strings.Join(p.Updates[0].Columns(), ",") != "models,priority" {
		t.Fatalf("应只更新 models,priority：%v", p.Details())
	}
	if ab := p.Abilities[1]; ab == nil || ab.String("status") != "3" || ab.String("priority") != "20" {
		t.Fatalf("能力表应按写入后的状态重建（保持子站自动禁用）：%v", ab)
	}
	applyPlan(child, p)

	// 主站删除渠道 2
	delete(master.Channels.Rows, 2)
	p = Diff(master, child, cp, opt)
	if len(p.Deletes) != 1 || p.Deletes[0] != 2 {
		t.Fatalf("应删除渠道 2：%v", p.Details())
	}
	if ab, ok := p.Abilities[2]; !ok || ab != nil {
		t.Fatalf("删除的渠道应清理能力")
	}
}

func TestDiffRefusesToWipeChildWhenMasterEmpty(t *testing.T) {
	cp := BuildColumnPlan(masterCols, childCols, nil)
	master := &snapshot.Master{Channels: snapshot.Table{Columns: masterCols, Rows: map[int64]snapshot.Row{}}}
	child := &snapshot.Child{Channels: snapshot.Table{Columns: childCols, Rows: map[int64]snapshot.Row{1: {"id": s("1")}}}}
	p := Diff(master, child, cp, DiffOptions{StatusMode: config.StatusModeSmart, DeleteMissing: true})
	if len(p.Deletes) != 0 || len(p.Warnings) != 1 {
		t.Fatalf("主站为空时不应删除子站渠道：%+v", p)
	}
}

func TestEqualAcrossDatabases(t *testing.T) {
	cp := BuildColumnPlan(masterCols, childCols, nil)
	if !cp.Equal("channel_info", s(`{"b":1, "a":[1,2]}`), s(`{"a":[1,2],"b":1.0}`)) {
		t.Error("JSON 应按语义比较")
	}
	if !cp.Equal("priority", s("10"), s("10.0")) {
		t.Error("数值应按数值比较")
	}
	if cp.Equal("name", s("a"), s("A")) || cp.Equal("name", nil, s("")) {
		t.Error("文本应精确比较，NULL 与空串不同")
	}
}

func TestBuildAbilitiesMatchesNewAPI(t *testing.T) {
	row := snapshot.Row{"models": s(",gpt-4o,gpt-4o,o3,"), "group": s("default,vip,default"), "status": s("1"),
		"priority": nil, "weight": s("7"), "tag": s("x")}
	abs := newapidb.BuildAbilities(5, row)
	if len(abs) != 4 {
		t.Fatalf("应生成 2 模型 × 2 分组 = 4 条（去重、去首尾逗号），got %d：%+v", len(abs), abs)
	}
	for _, a := range abs {
		if !a.Enabled || a.Priority != 0 || a.Weight != 7 || a.Tag == nil || *a.Tag != "x" || a.ChannelID != 5 {
			t.Errorf("字段不对：%+v", a)
		}
	}
	if len(newapidb.BuildAbilities(1, snapshot.Row{"models": s(""), "group": s("default")})) != 0 {
		t.Error("没有模型时不应有能力记录")
	}
	disabled := newapidb.BuildAbilities(1, snapshot.Row{"models": s("m"), "group": s("g"), "status": s("3")})
	if len(disabled) != 1 || disabled[0].Enabled {
		t.Error("禁用渠道的能力应为 enabled=false")
	}
}

func deref(v snapshot.Value) string {
	if v == nil {
		return "<nil>"
	}
	return *v
}
