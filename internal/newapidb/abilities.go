package newapidb

import (
	"strconv"
	"strings"

	"github.com/10dian-ai/newapi-channel-synchronization/internal/snapshot"
)

// Ability 对应 NewAPI abilities 表的一行：分组 × 模型 × 渠道。
type Ability struct {
	Group     string
	Model     string
	ChannelID int64
	Enabled   bool
	Priority  int64
	Weight    int64
	Tag       *string
}

// BuildAbilities 按 NewAPI model.Channel.UpdateAbilities 的规则，从一条渠道生成能力表记录：
//   - 模型：去掉首尾逗号后按逗号拆分；models 为空时没有能力记录
//   - 分组：按逗号拆分（与 NewAPI 一致，不去空格）
//   - 同一 分组|模型 只保留一条
//   - enabled = 渠道状态是否为启用；priority/weight 为空时取 0；tag 原样复制
//
// 子站配置了管理接口时，写库后还会调用子站自己的修复接口，由子站按它所在版本的规则再重建一次，
// 所以这里的规则即使与将来的版本有细微差别，也会被子站自己纠正。
func BuildAbilities(id int64, row snapshot.Row) []Ability {
	models := row.String(ColModels)
	if models == "" {
		return nil
	}
	modelList := strings.Split(strings.Trim(models, ","), ",")
	groupList := strings.Split(row.String(ColGroup), ",")
	status := row.Int(ColStatus, 0)
	priority := row.Int(ColPriority, 0)
	weight := row.Int(ColWeight, 0)
	if weight < 0 {
		weight = 0
	}
	var tag *string
	if v := row.Get(ColTag); v != nil {
		t := *v
		tag = &t
	}
	seen := map[string]bool{}
	out := make([]Ability, 0, len(modelList)*len(groupList))
	for _, model := range modelList {
		for _, group := range groupList {
			key := group + "|" + model
			if seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, Ability{
				Group:     group,
				Model:     model,
				ChannelID: id,
				Enabled:   status == StatusEnabled,
				Priority:  priority,
				Weight:    weight,
				Tag:       tag,
			})
		}
	}
	return out
}

// Key 返回能力记录的主键字符串。
func (a Ability) Key() string {
	return a.Group + "|" + a.Model + "|" + strconv.FormatInt(a.ChannelID, 10)
}
