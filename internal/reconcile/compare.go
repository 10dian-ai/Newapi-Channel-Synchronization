package reconcile

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/10dian-ai/newapi-channel-synchronization/internal/snapshot"
)

// Equal 比较两个值是否等价。不同数据库对同一个值的文本表示可能不同
// （JSON 的键顺序和空格、数字 1 与 1.0、布尔 true 与 1），这里统一按语义比较。
func (p ColumnPlan) Equal(col string, a, b snapshot.Value) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if *a == *b {
		return true
	}
	if p.isJSON(col) {
		return jsonEqual(*a, *b)
	}
	if p.isNumeric(col) {
		return numericEqual(*a, *b)
	}
	return false
}

func numericEqual(a, b string) bool {
	fa, ok1 := toNumber(a)
	fb, ok2 := toNumber(b)
	return ok1 && ok2 && fa == fb
}

func toNumber(s string) (float64, bool) {
	s = strings.TrimSpace(s)
	switch strings.ToLower(s) {
	case "true", "t":
		return 1, true
	case "false", "f":
		return 0, true
	}
	f, err := strconv.ParseFloat(s, 64)
	return f, err == nil
}

// CanonicalJSON 把 JSON 文本规范化（键排序、去空白、数字保持原样）。不是合法 JSON 时返回原文。
func CanonicalJSON(s string) string {
	v, ok := decodeJSON(s)
	if !ok {
		return s
	}
	out, err := marshalCanonical(v)
	if err != nil {
		return s
	}
	return out
}

func jsonEqual(a, b string) bool {
	va, ok1 := decodeJSON(a)
	vb, ok2 := decodeJSON(b)
	if !ok1 || !ok2 {
		return false
	}
	ca, err1 := marshalCanonical(va)
	cb, err2 := marshalCanonical(vb)
	return err1 == nil && err2 == nil && ca == cb
}

func decodeJSON(s string) (any, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, false
	}
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, false
	}
	if dec.More() {
		return nil, false
	}
	return normalizeNumbers(v), true
}

// normalizeNumbers 把等值的数字统一表示（1 与 1.0 视为相同）。
func normalizeNumbers(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, val := range x {
			x[k] = normalizeNumbers(val)
		}
		return x
	case []any:
		for i, val := range x {
			x[i] = normalizeNumbers(val)
		}
		return x
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return json.Number(strconv.FormatInt(n, 10))
		}
		if f, err := x.Float64(); err == nil {
			if f == float64(int64(f)) && f < 1e15 && f > -1e15 {
				return json.Number(strconv.FormatInt(int64(f), 10))
			}
			return json.Number(strconv.FormatFloat(f, 'g', -1, 64))
		}
		return x
	default:
		return v
	}
}

func marshalCanonical(v any) (string, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return "", err
	}
	return strings.TrimRight(buf.String(), "\n"), nil
}
