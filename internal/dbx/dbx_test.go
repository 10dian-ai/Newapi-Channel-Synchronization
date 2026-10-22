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
	one := "1"
	if DriverValue(Column{Kind: KindBool}, &one) != true {
		t.Error("布尔列应写入 bool")
	}
	if DriverValue(Column{Kind: KindInt}, &one) != int64(1) {
		t.Error("整数列应写入 int64")
	}
	tr := "true"
	if DriverValue(Column{Kind: KindInt}, &tr) != int64(1) {
		t.Error("跨库布尔写入整数列应转为 1")
	}
}
