package dbx

import (
	"context"
	"database/sql"
	"fmt"
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
		db, err = openMySQL(opt)
	case Postgres:
		db, err = openPostgres(opt)
	}
	if err != nil {
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

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("连接数据库失败：%w", err)
	}
	return &DB{DB: db, Dialect: dialect, ReadOnly: opt.ReadOnly}, nil
}

func openMySQL(opt Options) (*sql.DB, error) {
	cfg, err := mysql.ParseDSN(strings.TrimSpace(opt.DSN))
	if err != nil {
		return nil, fmt.Errorf("MySQL DSN 格式错误：%w", err)
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
	if opt.ReadOnly {
		// 会话级只读：即使代码出错执行了写语句，数据库也会拒绝。
		cfg.RuntimeParams["default_transaction_read_only"] = "on"
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

// Identity 返回能区分不同数据库实例的标识；查询失败时返回空串。
func (db *DB) Identity(ctx context.Context) string {
	var id sql.NullString
	if err := db.QueryRowContext(ctx, db.Dialect.IdentityQuery()).Scan(&id); err != nil {
		return ""
	}
	return id.String
}
