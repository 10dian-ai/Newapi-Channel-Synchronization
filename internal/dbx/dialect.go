// Package dbx 封装与 NewAPI 数据库打交道的方言差异：标识符引用、占位符、
// 表结构探测、校验和探测查询、数据库身份识别等。
package dbx

import (
	"fmt"
	"strings"
)

// 支持的数据库类型。
const (
	MySQL    = "mysql"
	Postgres = "postgres"
)

// Dialect 抽象 MySQL 与 PostgreSQL 的 SQL 差异。
type Dialect interface {
	Name() string
	// Quote 引用标识符（group、key 在两种数据库里都是保留字，必须引用）。
	Quote(ident string) string
	// Placeholder 返回第 n 个参数的占位符（从 1 开始）。
	Placeholder(n int) string
	// ColumnsQuery 返回查询某张表列信息的 SQL 与参数。
	ColumnsQuery(table string) (string, []any)
	// ChecksumQuery 返回把若干列拼接后计算行校验和、再在整表聚合的查询，
	// 结果是一行一列的字符串。用于低成本地判断数据是否变化。
	// where 可包含占位符，参数由调用方传入。
	ChecksumQuery(table string, columns []string, where string) string
	// IdentityQuery 返回用于识别"这是不是同一个数据库"的查询。
	IdentityQuery() string
	// ResetSequenceSQL 返回显式插入主键后修正自增序列的 SQL；不需要时返回空串。
	ResetSequenceSQL(table, column string) string
}

// DialectFor 根据类型返回方言。
func DialectFor(kind string) (Dialect, error) {
	switch kind {
	case MySQL:
		return mysqlDialect{}, nil
	case Postgres:
		return postgresDialect{}, nil
	default:
		return nil, fmt.Errorf("不支持的数据库类型 %q", kind)
	}
}

// DetectType 根据配置的 type 与 DSN 判断数据库类型，规则与 NewAPI 的 SQL_DSN 一致。
func DetectType(configured, dsn string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(configured)) {
	case "mysql":
		return MySQL, nil
	case "postgres", "postgresql":
		return Postgres, nil
	case "":
	default:
		return "", fmt.Errorf("不支持的数据库类型 %q", configured)
	}
	d := strings.TrimSpace(dsn)
	lower := strings.ToLower(d)
	switch {
	case strings.HasPrefix(lower, "postgres://"), strings.HasPrefix(lower, "postgresql://"):
		return Postgres, nil
	case strings.Contains(lower, "host=") && strings.Contains(lower, "dbname="):
		return Postgres, nil
	case strings.HasPrefix(lower, "local"), strings.HasSuffix(lower, ".db"), strings.HasPrefix(lower, "file:"), strings.HasPrefix(lower, "sqlite"):
		return "", fmt.Errorf("第一版暂不支持 SQLite 数据库")
	case strings.Contains(d, "@") || strings.Contains(d, "/"):
		return MySQL, nil
	default:
		return "", fmt.Errorf("无法从 DSN 判断数据库类型，请设置 database.type")
	}
}

type mysqlDialect struct{}

func (mysqlDialect) Name() string { return MySQL }

func (mysqlDialect) Quote(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}

func (mysqlDialect) Placeholder(int) string { return "?" }

func (mysqlDialect) ColumnsQuery(table string) (string, []any) {
	return `SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_TYPE
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
ORDER BY ORDINAL_POSITION`, []any{table}
}

func (d mysqlDialect) ChecksumQuery(table string, columns []string, where string) string {
	parts := make([]string, len(columns))
	for i, c := range columns {
		// IFNULL 区分 NULL 与空字符串，CHAR(31) 作为不可见分隔符
		parts[i] = "IFNULL(CAST(" + d.Quote(c) + " AS CHAR), CHAR(0))"
	}
	q := "SELECT CONCAT(COUNT(*), ':', COALESCE(SUM(CRC32(CONCAT_WS(CHAR(31), " + strings.Join(parts, ", ") + "))), 0)) FROM " + d.Quote(table)
	if where != "" {
		q += " WHERE " + where
	}
	return q
}

func (mysqlDialect) IdentityQuery() string {
	return "SELECT CONCAT(@@server_uuid, '/', DATABASE())"
}

func (mysqlDialect) ResetSequenceSQL(string, string) string { return "" }

type postgresDialect struct{}

func (postgresDialect) Name() string { return Postgres }

func (postgresDialect) Quote(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func (postgresDialect) Placeholder(n int) string { return fmt.Sprintf("$%d", n) }

func (postgresDialect) ColumnsQuery(table string) (string, []any) {
	return `SELECT column_name, data_type, is_nullable, column_default, udt_name
FROM information_schema.columns
WHERE table_schema = current_schema() AND table_name = $1
ORDER BY ordinal_position`, []any{table}
}

func (d postgresDialect) ChecksumQuery(table string, columns []string, where string) string {
	parts := make([]string, len(columns))
	for i, c := range columns {
		parts[i] = "COALESCE(" + d.Quote(c) + "::text, chr(1))"
	}
	inner := "SELECT md5(concat_ws(chr(31), " + strings.Join(parts, ", ") + ")) AS h FROM " + d.Quote(table)
	if where != "" {
		inner += " WHERE " + where
	}
	// 按行哈希值排序后再聚合，结果与物理存储顺序无关
	return "SELECT COUNT(*)::text || ':' || COALESCE(md5(string_agg(t.h, '' ORDER BY t.h)), '') FROM (" + inner + ") t"
}

func (postgresDialect) IdentityQuery() string {
	return "SELECT current_database() || '/' || COALESCE(inet_server_addr()::text, 'local') || ':' || COALESCE(inet_server_port()::text, '') || '/' || current_setting('data_directory', true)"
}

func (d postgresDialect) ResetSequenceSQL(table, column string) string {
	return fmt.Sprintf(
		"SELECT setval(pg_get_serial_sequence('%s', '%s'), GREATEST((SELECT COALESCE(MAX(%s), 0) FROM %s), 1))",
		table, column, d.Quote(column), d.Quote(table),
	)
}
