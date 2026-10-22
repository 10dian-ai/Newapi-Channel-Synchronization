package reconcile

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/10dian-ai/newapi-channel-synchronization/internal/newapidb"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/snapshot"
)

// ChannelTypeCodex 是 NewAPI 的 Codex（ChatGPT OAuth）渠道类型。
// 这类渠道的 Key 是一段 JSON，包含会轮换的刷新令牌，每个 NewAPI 主节点都会定期自动刷新。
const ChannelTypeCodex = 57

// keepChildCredential 判断是否应保留子站的 Codex 凭证而不用主站的覆盖。
//
// 刷新令牌是一次性的：如果子站先刷新了，主站手里的旧刷新令牌就作废了。此时再用主站的旧凭证
// 覆盖子站，两边都会在访问令牌过期后失效。所以只要子站的凭证明确比主站新，就保留子站的。
func keepChildCredential(mRow, cRow snapshot.Row) bool {
	if mRow.Int(colType, -1) != ChannelTypeCodex || cRow == nil {
		return false
	}
	mt, ok1 := credentialTime(mRow.String(newapidb.ColKey))
	ct, ok2 := credentialTime(cRow.String(newapidb.ColKey))
	return ok1 && ok2 && ct.After(mt)
}

// credentialTime 取 Codex 凭证的刷新时间（没有时用过期时间）。
func credentialTime(raw string) (time.Time, bool) {
	var key struct {
		LastRefresh string `json:"last_refresh"`
		Expired     string `json:"expired"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &key); err != nil {
		return time.Time{}, false
	}
	for _, s := range []string{key.LastRefresh, key.Expired} {
		if t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(s)); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// CheckAbilities 对比子站渠道与能力表，返回能力记录不一致的渠道 id（含已不存在渠道的遗留记录）。
//
// 比较时忽略大小写和末尾空格：MySQL 常用的排序规则下 "gpt-4o" 与 "GPT-4o" 是同一个主键，
// 数据库里只会有一条，按字面比较会永远"不一致"。
func CheckAbilities(channels snapshot.Table, abilities []snapshot.AbilityRow) []int64 {
	type state struct {
		enabled          bool
		priority, weight int64
	}
	norm := func(s string) string { return strings.ToLower(strings.TrimRight(s, " ")) }
	actual := map[int64]map[string]state{}
	for _, a := range abilities {
		m := actual[a.ChannelID]
		if m == nil {
			m = map[string]state{}
			actual[a.ChannelID] = m
		}
		m[norm(a.Group)+"|"+norm(a.Model)] = state{a.Enabled, a.Priority, a.Weight}
	}
	var bad []int64
	for _, id := range channels.IDs() {
		want := map[string]state{}
		for _, a := range newapidb.BuildAbilities(id, channels.Rows[id]) {
			k := norm(a.Group) + "|" + norm(a.Model)
			if _, dup := want[k]; !dup {
				want[k] = state{a.Enabled, a.Priority, a.Weight}
			}
		}
		got := actual[id]
		delete(actual, id)
		if len(got) != len(want) {
			bad = append(bad, id)
			continue
		}
		for k, w := range want {
			if g, ok := got[k]; !ok || g != w {
				bad = append(bad, id)
				break
			}
		}
	}
	for id := range actual {
		bad = append(bad, id) // 渠道已不存在
	}
	return bad
}
