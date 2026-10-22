package engine

import (
	"sort"
	"sync"
	"time"
)

// 子站同步状态。
const (
	StateStarting = "starting"
	StateOK       = "ok"
	StateSyncing  = "syncing"
	StateError    = "error"
	StatePaused   = "paused"
	StateDryRun   = "dry_run"
)

// MasterStatus 是主站侧的运行状态。
type MasterStatus struct {
	Name          string     `json:"name"`
	ProbeMode     string     `json:"probe_mode"` // checksum / full
	Checksum      string     `json:"checksum,omitempty"`
	Fingerprint   string     `json:"fingerprint,omitempty"`
	Channels      int        `json:"channels"`
	Options       int        `json:"options"`
	LastProbeAt   *time.Time `json:"last_probe_at,omitempty"`
	LastOKAt      *time.Time `json:"last_ok_at,omitempty"`
	LastChangeAt  *time.Time `json:"last_change_at,omitempty"`
	LastError     string     `json:"last_error,omitempty"`
	Probes        int64      `json:"probes"`
	Changes       int64      `json:"changes"`
	Reconciles    int64      `json:"reconciles"`
	ProbeFailures int64      `json:"probe_failures"`
}

// ChildStatus 是一个子站的运行状态。
type ChildStatus struct {
	Name                string     `json:"name"`
	State               string     `json:"state"`
	DryRun              bool       `json:"dry_run"`
	RefreshChannels     string     `json:"refresh_channels"`
	OptionsVia          string     `json:"options_via"`
	Version             string     `json:"version,omitempty"`
	SchemaFingerprint   string     `json:"schema_fingerprint,omitempty"`
	MissingInChild      []string   `json:"missing_in_child,omitempty"`
	ChildOnlyColumns    []string   `json:"child_only_columns,omitempty"`
	Channels            int        `json:"channels"`
	LastAttemptAt       *time.Time `json:"last_attempt_at,omitempty"`
	LastSuccessAt       *time.Time `json:"last_success_at,omitempty"`
	LastChangeAppliedAt *time.Time `json:"last_change_applied_at,omitempty"`
	LastLagMillis       int64      `json:"last_lag_ms"`
	LastPlan            string     `json:"last_plan,omitempty"`
	LastDetails         []string   `json:"last_details,omitempty"`
	LastWarnings        []string   `json:"last_warnings,omitempty"`
	LastError           string     `json:"last_error,omitempty"`
	ConsecutiveFailures int        `json:"consecutive_failures"`
	Syncs               int64      `json:"syncs"`
	Failures            int64      `json:"failures"`
	Inserted            int64      `json:"inserted"`
	Updated             int64      `json:"updated"`
	Deleted             int64      `json:"deleted"`
	OptionWrites        int64      `json:"option_writes"`
}

// Status 是线程安全的状态登记处，供状态接口与指标使用。
type Status struct {
	mu        sync.RWMutex
	startedAt time.Time
	master    MasterStatus
	children  map[string]*ChildStatus
}

// NewStatus 创建状态登记处。
func NewStatus(masterName string) *Status {
	return &Status{
		startedAt: time.Now(),
		master:    MasterStatus{Name: masterName, ProbeMode: "checksum"},
		children:  map[string]*ChildStatus{},
	}
}

func (s *Status) updateMaster(fn func(m *MasterStatus)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(&s.master)
}

func (s *Status) updateChild(name string, fn func(c *ChildStatus)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.children[name]
	if !ok {
		c = &ChildStatus{Name: name, State: StateStarting}
		s.children[name] = c
	}
	fn(c)
}

// Snapshot 是某一时刻状态的副本。
type Snapshot struct {
	StartedAt time.Time     `json:"started_at"`
	Master    MasterStatus  `json:"master"`
	Children  []ChildStatus `json:"children"`
}

// Snapshot 返回当前状态的副本。
func (s *Status) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := Snapshot{StartedAt: s.startedAt, Master: s.master}
	for _, c := range s.children {
		cp := *c
		cp.LastDetails = append([]string(nil), c.LastDetails...)
		cp.LastWarnings = append([]string(nil), c.LastWarnings...)
		cp.MissingInChild = append([]string(nil), c.MissingInChild...)
		cp.ChildOnlyColumns = append([]string(nil), c.ChildOnlyColumns...)
		out.Children = append(out.Children, cp)
	}
	sort.Slice(out.Children, func(i, j int) bool { return out.Children[i].Name < out.Children[j].Name })
	return out
}

// MasterHealthy 报告主站最近是否可以正常读取。
func (s *Status) MasterHealthy(maxAge time.Duration) (bool, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.master.LastOKAt == nil {
		if time.Since(s.startedAt) < maxAge {
			return true, "starting"
		}
		return false, "主站从未读取成功：" + s.master.LastError
	}
	if age := time.Since(*s.master.LastOKAt); age > maxAge {
		return false, "主站已 " + age.Truncate(time.Second).String() + " 未读取成功：" + s.master.LastError
	}
	return true, "ok"
}

func ptrTime(t time.Time) *time.Time { return &t }
