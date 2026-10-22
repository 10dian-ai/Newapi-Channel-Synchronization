// Package source 负责读取主站数据库。
//
// 这个包只提供读取能力：连接以只读模式打开（PostgreSQL 会话级只读），
// 所有查询都在只读事务里执行，代码里也没有任何写主库的路径。
package source

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/10dian-ai/newapi-channel-synchronization/internal/dbx"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/newapidb"
	"github.com/10dian-ai/newapi-channel-synchronization/internal/snapshot"
)

// Master 是只读的主站数据源。
type Master struct {
	db      *dbx.DB
	matcher newapidb.OptionMatcher
	// probeExclude 是探测时忽略的列（主站自己的运行时数据，例如已用额度），
	// 它们每次请求都可能变化，但与同步无关，不应触发同步。
	probeExclude map[string]bool

	mu           sync.Mutex
	schema       newapidb.Schema
	probeQueries []probeQuery
	probeBroken  time.Time // 探测查询不可用的时间，过一段时间后再试
}

// probeRetryAfter 是探测查询不可用后多久再尝试一次。
const probeRetryAfter = time.Minute

type probeQuery struct {
	sql  string
	args []any
}

// New 创建主站数据源。db 必须以只读方式打开。
func New(db *dbx.DB, matcher newapidb.OptionMatcher, probeExclude []string) (*Master, error) {
	if !db.ReadOnly {
		return nil, fmt.Errorf("主站连接必须以只读方式打开")
	}
	ex := map[string]bool{}
	for _, c := range probeExclude {
		ex[c] = true
	}
	return &Master{db: db, matcher: matcher, probeExclude: ex}, nil
}

// RefreshSchema 重新读取主站表结构并重建探测查询。
func (m *Master) RefreshSchema(ctx context.Context) (newapidb.Schema, error) {
	tx, err := m.db.BeginRead(ctx)
	if err != nil {
		return newapidb.Schema{}, fmt.Errorf("开启只读事务失败：%w", err)
	}
	defer func() { _ = tx.Rollback() }()
	schema, err := newapidb.LoadSchema(ctx, tx, m.db.Dialect)
	if err != nil {
		return schema, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.schema = schema
	m.probeQueries = m.buildProbes(schema)
	m.probeBroken = time.Time{}
	return schema, nil
}

// Schema 返回最近一次读取的表结构。
func (m *Master) Schema() newapidb.Schema {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.schema
}

func (m *Master) buildProbes(schema newapidb.Schema) []probeQuery {
	d := m.db.Dialect
	var exprs []string
	for _, c := range schema.Channels {
		if m.probeExclude[c.Name] {
			continue
		}
		e := d.Quote(c.Name)
		if c.Name == newapidb.ColChannelInfo {
			// 多 Key 轮询下标在未开启内存缓存时每次请求都会写回，属于运行时状态
			e = d.StripJSONKeyExpr(e, "multi_key_polling_index")
		}
		exprs = append(exprs, e)
	}
	probes := []probeQuery{{sql: d.ChecksumQuery(newapidb.TableChannels, exprs, "")}}
	if len(schema.Options) > 0 && !m.matcher.Empty() {
		where, args := m.matcher.WhereClause(d, 1)
		probes = append(probes, probeQuery{
			sql:  d.ChecksumQuery(newapidb.TableOptions, []string{d.Quote(newapidb.OptionKeyCol), d.Quote(newapidb.OptionValueCol)}, where),
			args: args,
		})
	}
	return probes
}

// Probe 用一两条聚合查询计算主站数据的校验和。数据量再大也只返回一行，开销很小。
//
// 返回值 supported=false 表示探测查询本身不可用（例如数据库函数受限），调用方应改用完整读取；
// supported=true 且 err 不为空表示连接类错误，稍后重试即可。
func (m *Master) Probe(ctx context.Context) (checksum string, supported bool, err error) {
	m.mu.Lock()
	probes, broken := m.probeQueries, m.probeBroken
	m.mu.Unlock()
	if len(probes) == 0 || (!broken.IsZero() && time.Since(broken) < probeRetryAfter) {
		return "", false, nil
	}
	tx, err := m.db.BeginRead(ctx)
	if err != nil {
		return "", true, fmt.Errorf("开启只读事务失败：%w", err)
	}
	defer func() { _ = tx.Rollback() }()
	parts := make([]string, 0, len(probes))
	for _, p := range probes {
		var v any
		if err := tx.QueryRowContext(ctx, p.sql, p.args...).Scan(&v); err != nil {
			if ctx.Err() != nil {
				return "", true, ctx.Err()
			}
			// 区分"连不上"和"这条查询不能用"：连接正常而查询失败，才判定探测不可用。
			if pingErr := m.db.PingContext(ctx); pingErr != nil {
				return "", true, fmt.Errorf("主站连接异常：%w", pingErr)
			}
			m.mu.Lock()
			m.probeBroken = time.Now()
			m.mu.Unlock()
			return "", false, fmt.Errorf("校验和探测不可用，改为完整读取：%w", err)
		}
		if s := dbx.Normalize(v); s != nil {
			parts = append(parts, *s)
		}
	}
	return strings.Join(parts, "|"), true, nil
}

// Load 在一个只读事务里读取完整快照，保证渠道与设置来自同一时刻。
func (m *Master) Load(ctx context.Context, withOptions bool) (*snapshot.Master, error) {
	m.mu.Lock()
	schema := m.schema
	m.mu.Unlock()
	if len(schema.Channels) == 0 {
		var err error
		if schema, err = m.RefreshSchema(ctx); err != nil {
			return nil, err
		}
	}
	tx, err := m.db.BeginRead(ctx)
	if err != nil {
		return nil, fmt.Errorf("开启只读事务失败：%w", err)
	}
	defer func() { _ = tx.Rollback() }()
	channels, err := newapidb.ReadChannels(ctx, tx, m.db.Dialect, schema.Channels)
	if err != nil {
		return nil, err
	}
	snap := &snapshot.Master{Channels: channels, Options: map[string]string{}, LoadedAt: time.Now()}
	if withOptions && len(schema.Options) > 0 {
		if snap.Options, err = newapidb.ReadOptions(ctx, tx, m.db.Dialect, m.matcher); err != nil {
			return nil, err
		}
	}
	return snap, nil
}
