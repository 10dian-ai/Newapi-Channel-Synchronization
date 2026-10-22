// Package snapshot 定义主站快照与子站状态的数据结构。
//
// 行以"列名 -> 规范化字符串值"的形式保存（nil 表示 NULL），这样不同 NewAPI
// 版本多出来或少掉的列都能自然处理，不需要为每个版本写一套结构体。
package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"time"

	"github.com/10dian-ai/newapi-channel-synchronization/internal/dbx"
)

// Value 是规范化后的列值，nil 表示 SQL NULL。
type Value = *string

// Row 是一行数据。
type Row map[string]Value

// Get 返回某列的值（不存在或 NULL 都返回 nil）。
func (r Row) Get(col string) Value { return r[col] }

// String 返回某列的字符串值，NULL 返回空串。
func (r Row) String(col string) string {
	if v := r[col]; v != nil {
		return *v
	}
	return ""
}

// Int 返回某列的整数值，无法解析时返回 def。
func (r Row) Int(col string, def int64) int64 {
	v := r[col]
	if v == nil {
		return def
	}
	n, err := strconv.ParseInt(*v, 10, 64)
	if err != nil {
		f, ferr := strconv.ParseFloat(*v, 64)
		if ferr != nil {
			return def
		}
		return int64(f)
	}
	return n
}

// Clone 复制一行。
func (r Row) Clone() Row {
	out := make(Row, len(r))
	for k, v := range r {
		out[k] = v
	}
	return out
}

// Table 是一张按主键 id 索引的表。
type Table struct {
	Columns dbx.Columns
	Rows    map[int64]Row
}

// IDs 返回排好序的主键列表。
func (t *Table) IDs() []int64 {
	ids := make([]int64, 0, len(t.Rows))
	for id := range t.Rows {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// Master 是一次主站快照。
type Master struct {
	Channels Table
	// Options 是白名单内的分组设置。
	Options map[string]string
	// Checksum 是探测得到的校验和（可能为空）。
	Checksum string
	// DetectedAt 是发现这次变更的时间，用来计算端到端同步延迟。
	DetectedAt time.Time
	// LoadedAt 是读取完成的时间。
	LoadedAt time.Time
	// Full 表示这是一次定期全量对账触发的快照。
	Full bool
}

// Fingerprint 返回快照内容指纹（用于日志与状态页）。
func (m *Master) Fingerprint() string {
	h := sha256.New()
	for _, id := range m.Channels.IDs() {
		row := m.Channels.Rows[id]
		cols := make([]string, 0, len(row))
		for c := range row {
			cols = append(cols, c)
		}
		sort.Strings(cols)
		h.Write([]byte(strconv.FormatInt(id, 10)))
		for _, c := range cols {
			h.Write([]byte{0x1e})
			h.Write([]byte(c))
			h.Write([]byte{0x1f})
			if v := row[c]; v != nil {
				h.Write([]byte(*v))
			} else {
				h.Write([]byte{0})
			}
		}
		h.Write([]byte{0x1d})
	}
	keys := make([]string, 0, len(m.Options))
	for k := range m.Options {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{0x1f})
		h.Write([]byte(m.Options[k]))
		h.Write([]byte{0x1d})
	}
	return hex.EncodeToString(h.Sum(nil)[:8])
}

// Child 是子站当前状态。
type Child struct {
	Channels Table
	Options  map[string]string
}
