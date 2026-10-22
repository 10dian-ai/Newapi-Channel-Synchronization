package reconcile

import (
	"encoding/json"
	"sort"
	"strconv"

	"github.com/10dian-ai/newapi-channel-synchronization/internal/config"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/newapidb"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/snapshot"
)

// DesiredStatus 计算子站渠道应有的状态。
//
// smart 模式（默认）：
//   - 主站手动禁用（2）→ 子站禁用（2）
//   - 子站自己自动禁用（3）→ 保持 3，交给子站自己的健康检查恢复
//   - 其余情况（主站启用 1，或主站自动禁用 3）→ 子站启用（1）。
//     主站的自动禁用是主站自己的运行时判断，不传给子站；同步服务自己从不把渠道设为 3，
//     因此子站上的每一个 3 都来自子站自己，不会出现"被同步卡死在禁用状态"的情况。
//   - 未知状态值原样透传，兼容将来的版本。
//
// mirror 模式：与主站完全一致。
func DesiredStatus(mode string, master int64, child *int64) int64 {
	if mode == config.StatusModeMirror {
		return master
	}
	switch master {
	case newapidb.StatusManuallyDisabled:
		return newapidb.StatusManuallyDisabled
	case newapidb.StatusEnabled, newapidb.StatusAutoDisabled:
		if child != nil && *child == newapidb.StatusAutoDisabled {
			return newapidb.StatusAutoDisabled
		}
		return newapidb.StatusEnabled
	default:
		return master
	}
}

// channel_info 中的键（NewAPI model.ChannelInfo）。
const (
	ciIsMultiKey     = "is_multi_key"
	ciMultiKeySize   = "multi_key_size"
	ciMultiKeyMode   = "multi_key_mode"
	ciStatusList     = "multi_key_status_list"
	ciDisabledReason = "multi_key_disabled_reason"
	ciDisabledTime   = "multi_key_disabled_time"
	ciPollingIndex   = "multi_key_polling_index"
)

// MergeChannelInfo 计算子站 channel_info 的期望值。
//
// channel_info 里既有配置（是否多 Key、Key 数量、轮询方式），也有运行时状态（每个 Key 的启用状态、
// 禁用原因与时间、轮询下标）。规则：
//   - 配置跟随主站；只在子站版本才有的键（主站版本不认识）保留子站自己的
//   - 每个 Key 的状态按与渠道状态相同的规则合并（mirror 模式直接用主站的）
//   - 轮询下标始终保留子站自己的
//   - Key 内容或多 Key 配置变了时，旧的下标与状态已经没有意义，以主站为基础
//
// 缺省与零值视为相同（例如旧版本没有 multi_key_mode，新版本是 ""），避免版本差导致反复改写。
func MergeChannelInfo(mode string, master, child snapshot.Value, keyChanged bool) snapshot.Value {
	if master == nil {
		return nil
	}
	mv, ok := decodeJSON(*master)
	mInfo, isObj := mv.(map[string]any)
	if !ok || !isObj {
		return master
	}
	var cInfo map[string]any
	if child != nil {
		if cv, ok := decodeJSON(*child); ok {
			cInfo, _ = cv.(map[string]any)
		}
	}
	fresh := cInfo == nil || keyChanged ||
		!sameConfig(mInfo[ciIsMultiKey], cInfo[ciIsMultiKey]) ||
		!sameConfig(mInfo[ciMultiKeySize], cInfo[ciMultiKeySize]) ||
		!sameConfig(mInfo[ciMultiKeyMode], cInfo[ciMultiKeyMode])

	out := make(map[string]any, len(mInfo)+len(cInfo))
	// 子站版本独有的键：主站不认识，保留子站的
	for k, v := range cInfo {
		if _, known := mInfo[k]; !known && !isKnownInfoKey(k) {
			out[k] = v
		}
	}
	for k, v := range mInfo {
		out[k] = v
	}
	// 主站缺省、子站有零值的配置键：保持子站的表示，避免无意义的改写
	for _, k := range []string{ciIsMultiKey, ciMultiKeySize, ciMultiKeyMode} {
		if _, ok := mInfo[k]; !ok {
			if cv, ok := cInfo[k]; ok && isZeroJSON(cv) {
				out[k] = cv
			}
		}
	}

	mStatus := intMap(mInfo[ciStatusList])
	var cStatus map[int]int64
	if !fresh {
		cStatus = intMap(cInfo[ciStatusList])
	}
	indexes := map[int]bool{}
	for i := range mStatus {
		indexes[i] = true
	}
	for i := range cStatus {
		indexes[i] = true
	}
	desired := map[int]int64{}
	fromMaster := map[int]bool{}
	for i := range indexes {
		m, ok := mStatus[i]
		if !ok {
			m = newapidb.StatusEnabled
		}
		c, ok := cStatus[i]
		if !ok {
			c = newapidb.StatusEnabled
		}
		d := DesiredStatus(mode, m, &c)
		if d == newapidb.StatusEnabled {
			continue
		}
		desired[i] = d
		fromMaster[i] = d != newapidb.StatusAutoDisabled || c != newapidb.StatusAutoDisabled || mode == config.StatusModeMirror
	}
	setIntMap(out, mInfo, ciStatusList, desired)

	reasons := map[int]any{}
	times := map[int]any{}
	mReason, mTime := anyMap(mInfo[ciDisabledReason]), anyMap(mInfo[ciDisabledTime])
	var cReason, cTime map[int]any
	if !fresh {
		cReason, cTime = anyMap(cInfo[ciDisabledReason]), anyMap(cInfo[ciDisabledTime])
	}
	for i := range desired {
		srcReason, srcTime := mReason, mTime
		if !fromMaster[i] {
			srcReason, srcTime = cReason, cTime
		}
		if v, ok := srcReason[i]; ok {
			reasons[i] = v
		}
		if v, ok := srcTime[i]; ok {
			times[i] = v
		}
	}
	setAnyMap(out, ciDisabledReason, reasons)
	setAnyMap(out, ciDisabledTime, times)

	// 轮询下标是子站自己的运行时状态（Key 或多 Key 配置变化时用主站的起点）
	if !fresh {
		if v, ok := cInfo[ciPollingIndex]; ok {
			out[ciPollingIndex] = v
		} else {
			delete(out, ciPollingIndex)
		}
	}

	s, err := marshalCanonical(out)
	if err != nil {
		return master
	}
	return &s
}

func isKnownInfoKey(k string) bool {
	switch k {
	case ciIsMultiKey, ciMultiKeySize, ciMultiKeyMode, ciStatusList, ciDisabledReason, ciDisabledTime, ciPollingIndex:
		return true
	}
	return false
}

// sameConfig 比较配置值，缺省（nil）与零值视为相同。
func sameConfig(a, b any) bool {
	if sameValue(a, b) {
		return true
	}
	return isZeroJSON(a) && isZeroJSON(b)
}

func isZeroJSON(v any) bool {
	switch x := v.(type) {
	case nil:
		return true
	case bool:
		return !x
	case string:
		return x == ""
	case json.Number:
		f, err := x.Float64()
		return err == nil && f == 0
	case float64:
		return x == 0
	case map[string]any:
		return len(x) == 0
	case []any:
		return len(x) == 0
	}
	return false
}

func sameValue(a, b any) bool {
	ca, err1 := marshalCanonical(a)
	cb, err2 := marshalCanonical(b)
	return err1 == nil && err2 == nil && ca == cb
}

func intMap(v any) map[int]int64 {
	out := map[int]int64{}
	m, ok := v.(map[string]any)
	if !ok {
		return out
	}
	for k, val := range m {
		i, err := strconv.Atoi(k)
		if err != nil {
			continue
		}
		switch n := val.(type) {
		case json.Number:
			if x, err := n.Int64(); err == nil {
				out[i] = x
			}
		case float64:
			out[i] = int64(n)
		}
	}
	return out
}

func anyMap(v any) map[int]any {
	out := map[int]any{}
	m, ok := v.(map[string]any)
	if !ok {
		return out
	}
	for k, val := range m {
		if i, err := strconv.Atoi(k); err == nil {
			out[i] = val
		}
	}
	return out
}

// setIntMap 写入状态表；为空时尽量保持主站原来的表示（缺省 / null / {}），避免无意义的改动。
func setIntMap(out, master map[string]any, key string, m map[int]int64) {
	if len(m) == 0 {
		orig, present := master[key]
		switch {
		case !present:
			delete(out, key)
		case orig == nil:
			out[key] = nil
		default:
			out[key] = map[string]any{}
		}
		return
	}
	obj := make(map[string]any, len(m))
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	for _, k := range keys {
		obj[strconv.Itoa(k)] = json.Number(strconv.FormatInt(m[k], 10))
	}
	out[key] = obj
}

// setAnyMap 写入原因/时间表；这两个字段在 NewAPI 中是 omitempty，为空时直接去掉。
func setAnyMap(out map[string]any, key string, m map[int]any) {
	if len(m) == 0 {
		delete(out, key)
		return
	}
	obj := make(map[string]any, len(m))
	for k, v := range m {
		obj[strconv.Itoa(k)] = v
	}
	out[key] = obj
}
