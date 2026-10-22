package dbx

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Kind 是列的大类，用于跨数据库比较值和生成写入参数。
type Kind int

const (
	KindText Kind = iota
	KindInt
	KindFloat
	KindBool
	KindJSON
)

func (k Kind) String() string {
	switch k {
	case KindInt:
		return "int"
	case KindFloat:
		return "float"
	case KindBool:
		return "bool"
	case KindJSON:
		return "json"
	default:
		return "text"
	}
}

// Column 描述表中的一列。
type Column struct {
	Name       string
	DataType   string // 数据库原始类型名，小写
	Kind       Kind
	Nullable   bool
	HasDefault bool
}

// Columns 是有序的列集合。
type Columns []Column

// Names 返回列名列表。
func (cs Columns) Names() []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.Name
	}
	return out
}

// Get 按名称查找列。
func (cs Columns) Get(name string) (Column, bool) {
	for _, c := range cs {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}

// Has 报告是否存在某列。
func (cs Columns) Has(name string) bool {
	_, ok := cs.Get(name)
	return ok
}

// Fingerprint 返回表结构指纹，用于发现子站升级导致的结构变化。
func (cs Columns) Fingerprint() string {
	parts := make([]string, len(cs))
	for i, c := range cs {
		parts[i] = fmt.Sprintf("%s:%s:%t:%t", c.Name, c.DataType, c.Nullable, c.HasDefault)
	}
	sort.Strings(parts)
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(sum[:8])
}

// KindOf 把数据库类型名映射为大类。
func KindOf(dataType, fullType string) Kind {
	t := strings.ToLower(strings.TrimSpace(dataType))
	full := strings.ToLower(fullType)
	switch t {
	case "tinyint":
		if strings.HasPrefix(full, "tinyint(1)") {
			return KindBool
		}
		return KindInt
	case "smallint", "mediumint", "int", "integer", "bigint", "int2", "int4", "int8", "serial", "bigserial":
		return KindInt
	case "float", "double", "decimal", "numeric", "real", "double precision", "float4", "float8":
		return KindFloat
	case "boolean", "bool", "bit":
		return KindBool
	case "json", "jsonb":
		return KindJSON
	default:
		return KindText
	}
}

// LoadColumns 读取表结构。表不存在时返回空集合。
func LoadColumns(ctx context.Context, q Queryer, d Dialect, table string) (Columns, error) {
	query, args := d.ColumnsQuery(table)
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("读取表 %s 结构失败：%w", table, err)
	}
	defer rows.Close()
	var out Columns
	for rows.Next() {
		var name, dataType, nullable string
		var def, full sql.NullString
		if err := rows.Scan(&name, &dataType, &nullable, &def, &full); err != nil {
			return nil, fmt.Errorf("读取表 %s 结构失败：%w", table, err)
		}
		out = append(out, Column{
			Name:       strings.ToLower(name),
			DataType:   strings.ToLower(dataType),
			Kind:       KindOf(dataType, full.String),
			Nullable:   strings.EqualFold(nullable, "YES"),
			HasDefault: def.Valid,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("读取表 %s 结构失败：%w", table, err)
	}
	return out, nil
}

// Queryer 是 *sql.DB 与 *sql.Tx 的公共查询接口。
type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Execer 是 *sql.DB 与 *sql.Tx 的公共执行接口。
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Normalize 把驱动返回的值统一转成可比较的字符串形式；nil 表示 SQL NULL。
func Normalize(v any) *string {
	var s string
	switch x := v.(type) {
	case nil:
		return nil
	case []byte:
		s = string(x)
	case string:
		s = x
	case int64:
		s = strconv.FormatInt(x, 10)
	case int32:
		s = strconv.FormatInt(int64(x), 10)
	case int:
		s = strconv.Itoa(x)
	case uint64:
		s = strconv.FormatUint(x, 10)
	case float64:
		s = strconv.FormatFloat(x, 'f', -1, 64)
	case float32:
		s = strconv.FormatFloat(float64(x), 'f', -1, 32)
	case bool:
		if x {
			s = "1"
		} else {
			s = "0"
		}
	case time.Time:
		s = x.UTC().Format(time.RFC3339Nano)
	default:
		s = fmt.Sprint(x)
	}
	return &s
}

// DriverValue 把规范化后的值按目标列的类型转换为写入参数。
// 无法无损转换时返回错误，绝不静默截断（例如 1.5 写入整数列、超出范围的无符号数）。
func DriverValue(col Column, v *string) (any, error) {
	if v == nil {
		return nil, nil
	}
	s := strings.TrimSpace(*v)
	switch col.Kind {
	case KindInt:
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return n, nil
		}
		if n, err := strconv.ParseUint(s, 10, 64); err == nil {
			return n, nil // bigint unsigned 的大值
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil && f == math.Trunc(f) && math.Abs(f) < 1<<53 {
			return int64(f), nil
		}
		if b, ok := parseBool(s); ok {
			if b {
				return int64(1), nil
			}
			return int64(0), nil
		}
		return nil, fmt.Errorf("列 %s 是整数类型，无法写入值 %q", col.Name, truncateForError(*v))
	case KindFloat:
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f, nil
		}
		return nil, fmt.Errorf("列 %s 是数值类型，无法写入值 %q", col.Name, truncateForError(*v))
	case KindBool:
		if b, ok := parseBool(s); ok {
			return b, nil
		}
		return nil, fmt.Errorf("列 %s 是布尔类型，无法写入值 %q", col.Name, truncateForError(*v))
	default:
		return *v, nil
	}
}

// truncateForError 截断错误信息里的值，避免把长字段（可能含密钥）整段写进日志。
func truncateForError(s string) string {
	if len(s) > 16 {
		return s[:8] + "…"
	}
	return s
}

func parseBool(s string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "t", "true", "y", "yes", "on":
		return true, true
	case "0", "f", "false", "n", "no", "off":
		return false, true
	}
	return false, false
}

// ZeroValue 返回某类列的零值（用于子站存在而主站没有、又不允许为空的列）。
func ZeroValue(kind Kind) *string {
	var s string
	switch kind {
	case KindInt, KindFloat, KindBool:
		s = "0"
	case KindJSON:
		s = "{}"
	default:
		s = ""
	}
	return &s
}
