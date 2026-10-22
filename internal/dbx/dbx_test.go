package dbx

import "testing"

func TestDetectType(t *testing.T) {
	cases := map[string]string{
		"root:123456@tcp(localhost:3306)/new-api":         MySQL,
		"postgresql://root:123456@postgres:5432/new-api":  Postgres,
		"postgres://u:p@h/db?sslmode=disable":             Postgres,
		"host=localhost user=u password=p dbname=new-api": Postgres,
	}
	for dsn, want := range cases {
		got, err := DetectType("", dsn)
		if err != nil || got != want {
			t.Errorf("DetectType(%q) = %q, %v; want %q", dsn, got, err, want)
		}
	}
	for _, dsn := range []string{"/data/one-api.db", "local"} {
		if _, err := DetectType("", dsn); err == nil {
			t.Errorf("SQLite DSN %q 应报不支持", dsn)
		}
	}
	if got, _ := DetectType("postgresql", "whatever"); got != Postgres {
		t.Error("显式类型应优先")
	}
}

func TestQuoteAndPlaceholder(t *testing.T) {
	m, _ := DialectFor(MySQL)
	p, _ := DialectFor(Postgres)
	if m.Quote("group") != "`group`" || p.Quote("key") != `"key"` {
		t.Error("标识符引用不对")
	}
	if m.Quote("a`b") != "`a``b`" || p.Quote(`a"b`) != `"a""b"` {
		t.Error("引号应转义")
	}
	if m.Placeholder(3) != "?" || p.Placeholder(3) != "$3" {
		t.Error("占位符不对")
	}
}

func TestKindAndValues(t *testing.T) {
	if KindOf("tinyint", "tinyint(1)") != KindBool || KindOf("tinyint", "tinyint(4)") != KindInt {
		t.Error("tinyint(1) 应视为布尔")
	}
	if KindOf("json", "") != KindJSON || KindOf("double precision", "") != KindFloat || KindOf("longtext", "") != KindText {
		t.Error("类型映射不对")
	}
	if v := Normalize([]byte("abc")); *v != "abc" {
		t.Error("[]byte 应转字符串")
	}
	if v := Normalize(true); *v != "1" {
		t.Error("布尔应规范为 1/0")
	}
	if v := Normalize(0.5); *v != "0.5" {
		t.Error("浮点格式不对")
	}
	if Normalize(nil) != nil {
		t.Error("NULL 应为 nil")
	}
	dv := func(kind Kind, v string) any {
		t.Helper()
		out, err := DriverValue(Column{Name: "c", Kind: kind}, &v)
		if err != nil {
			t.Fatalf("DriverValue(%v, %q) 出错：%v", kind, v, err)
		}
		return out
	}
	if dv(KindBool, "1") != true || dv(KindInt, "1") != int64(1) || dv(KindInt, "true") != int64(1) {
		t.Error("基本类型转换不对")
	}
	if dv(KindInt, "18446744073709551615") != uint64(18446744073709551615) {
		t.Error("bigint unsigned 的大值应原样写入")
	}
	if dv(KindInt, "10.0") != int64(10) {
		t.Error("整数值的浮点表示应转为整数")
	}
	for _, c := range []struct {
		kind Kind
		v    string
	}{{KindInt, "1.5"}, {KindInt, "abc"}, {KindFloat, "x"}, {KindBool, "maybe"}} {
		v := c.v
		if _, err := DriverValue(Column{Name: "c", Kind: c.kind}, &v); err == nil {
			t.Errorf("%q 写入 %v 列应报错而不是截断", c.v, c.kind)
		}
	}
}

func TestSameDatabase(t *testing.T) {
	same, err := SameDatabase(map[int]string{0: "uuid-a/db", 1: "h:3306/db"}, map[int]string{0: "uuid-a/db", 1: "other:3306/db"})
	if err != nil || !same {
		t.Error("实例 ID 相同应判定为同一个库")
	}
	same, err = SameDatabase(map[int]string{0: "uuid-a/db", 1: "h:3306/db"}, map[int]string{0: "uuid-b/db", 1: "h:3306/db"})
	if err != nil || same {
		t.Error("应以优先级最高的共同识别方式为准")
	}
	if _, err := SameDatabase(map[int]string{0: "a"}, map[int]string{1: "b"}); err == nil {
		t.Error("没有共同识别方式时应报错，不能默认放行")
	}
}
