package dbx

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// DB 是带方言信息的数据库连接。
type DB struct {
	*sql.DB
	Dialect  Dialect
	ReadOnly bool
}

// Options 是打开数据库的参数。
type Options struct {
	Type         string
	DSN          string
	MaxOpenConns int
	// ReadOnly 为 true 时，会话被设置为只读（PostgreSQL 使用 default_transaction_read_only，
	// MySQL 的所有读取都在只读事务中执行），并且调用方只能拿到只读的封装。
	ReadOnly bool
}

// Open 打开数据库并测试连通性。
func Open(ctx context.Context, opt Options) (*DB, error) {
	kind, err := DetectType(opt.Type, opt.DSN)
	if err != nil {
		return nil, err
	}
	dialect, err := DialectFor(kind)
	if err != nil {
		return nil, err
	}
	var db *sql.DB
	switch kind {
	case MySQL:
		db, err = openMySQLWithFallback(ctx, opt)
	case Postgres:
		db, err = openPostgres(opt)
		if err == nil {
			err = ping(ctx, db)
		}
	}
	if err != nil {
		if db != nil {
			_ = db.Close()
		}
		return nil, err
	}
	maxOpen := opt.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = 4
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxOpen)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)
	return &DB{DB: db, Dialect: dialect, ReadOnly: opt.ReadOnly}, nil
}

func ping(ctx context.Context, db *sql.DB) error {
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		return fmt.Errorf("连接数据库失败：%w", err)
	}
	return nil
}

// openMySQLWithFallback 打开 MySQL。只读连接会把会话设为只读：
// MySQL 5.7.20+ / 8.x 用 transaction_read_only，更老的版本与 MariaDB 用 tx_read_only。
func openMySQLWithFallback(ctx context.Context, opt Options) (*sql.DB, error) {
	vars := []string{""}
	if opt.ReadOnly {
		vars = []string{"transaction_read_only", "tx_read_only"}
	}
	var lastErr error
	for _, v := range vars {
		db, err := openMySQL(opt, v)
		if err != nil {
			return nil, err
		}
		if err = ping(ctx, db); err == nil {
			return db, nil
		}
		_ = db.Close()
		lastErr = err
		if !strings.Contains(err.Error(), "Unknown system variable") {
			break
		}
	}
	return nil, lastErr
}

func openMySQL(opt Options, readOnlyVar string) (*sql.DB, error) {
	cfg, err := mysql.ParseDSN(strings.TrimSpace(opt.DSN))
	if err != nil {
		return nil, fmt.Errorf("MySQL DSN 格式错误：%w", err)
	}
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	if readOnlyVar != "" {
		// 会话级只读：即使账号有写权限、即使代码出错执行了写语句，MySQL 也会拒绝（错误 1792）。
		cfg.Params[readOnlyVar] = "1"
	} else if _, ok := cfg.Params["innodb_lock_wait_timeout"]; !ok {
		// 子站写入时最多等锁 10 秒，避免长时间阻塞子站 NewAPI 自己的写操作。
		cfg.Params["innodb_lock_wait_timeout"] = "10"
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 60 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 60 * time.Second
	}
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("MySQL 配置错误：%w", err)
	}
	return sql.OpenDB(connector), nil
}

func openPostgres(opt Options) (*sql.DB, error) {
	cfg, err := pgx.ParseConfig(strings.TrimSpace(opt.DSN))
	if err != nil {
		return nil, fmt.Errorf("PostgreSQL DSN 格式错误：%w", err)
	}
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	if _, ok := cfg.RuntimeParams["application_name"]; !ok {
		cfg.RuntimeParams["application_name"] = "newapi-sync"
	}
	setDefault := func(k, v string) {
		if _, ok := cfg.RuntimeParams[k]; !ok {
			cfg.RuntimeParams[k] = v
		}
	}
	if opt.ReadOnly {
		// 会话级只读：即使代码出错执行了写语句，数据库也会拒绝。
		cfg.RuntimeParams["default_transaction_read_only"] = "on"
		setDefault("statement_timeout", "30000")
	} else {
		// 子站写入时最多等锁 10 秒、单条语句最多 60 秒
		setDefault("lock_timeout", "10000")
		setDefault("statement_timeout", "60000")
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 10 * time.Second
	}
	return stdlib.OpenDB(*cfg), nil
}

// BeginRead 开启只读事务，保证一次快照内读到的渠道与设置是一致的。
func (db *DB) BeginRead(ctx context.Context) (*sql.Tx, error) {
	return db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
}

// Identities 返回能区分不同数据库实例的标识（实例 ID + 库名），键是识别方式的序号。
// 所有识别方式都失败时返回错误：无法确认身份时，调用方必须拒绝继续，而不是默认放行。
func (db *DB) Identities(ctx context.Context) (map[int]string, error) {
	out := map[int]string{}
	var lastErr error
	for i, q := range db.Dialect.IdentityQueries() {
		var id sql.NullString
		err := db.QueryRowContext(ctx, q).Scan(&id)
		if err == nil && id.Valid && strings.TrimSpace(id.String) != "" {
			out[i] = id.String
			continue
		}
		if err == nil {
			err = fmt.Errorf("识别查询返回空值")
		}
		lastErr = err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("无法识别数据库实例：%w", lastErr)
	}
	return out, nil
}

// SameDatabase 判断两组标识是否指向同一个数据库。按识别方式的优先级，
// 取两边都可用的第一种来判断（实例唯一 ID 比主机名可靠）。
// 两边没有任何一种共同可用的识别方式时返回错误（无法判断就不能放行）。
func SameDatabase(a, b map[int]string) (bool, error) {
	keys := make([]int, 0, len(a))
	for k := range a {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	for _, k := range keys {
		if vb, ok := b[k]; ok {
			return a[k] == vb, nil
		}
	}
	return false, fmt.Errorf("主站与子站没有共同可用的数据库识别方式，无法确认它们不是同一个库")
}
