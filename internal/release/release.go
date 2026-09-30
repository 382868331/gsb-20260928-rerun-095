package release

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"gateway/internal/config"
)

type Stage string

const (
	StageCandidate  Stage = "candidate"
	StageCanary     Stage = "canary"
	StagePromoted   Stage = "promoted"
	StageRejected   Stage = "rejected"
	StageRolledBack Stage = "rolled_back"
)

type CanaryReport struct {
	Requests    int64     `json:"requests"`
	Failures    int64     `json:"failures"`
	MaxRequests int64     `json:"max_requests"`
	Statuses    []int     `json:"statuses"`
	StartedAt   time.Time `json:"started_at"`
	EndedAt     time.Time `json:"ended_at,omitempty"`
	maxRequests int64     `json:"-"`
	remaining   int64     `json:"-"`
}

type Release struct {
	ID           string           `json:"id"`
	Revision     string           `json:"revision"`
	ParentID     string           `json:"parent_id,omitempty"`
	Stage        Stage            `json:"stage"`
	Config       *config.Config   `json:"config"`
	Snapshot     *config.Snapshot `json:"snapshot"`
	Reason       string           `json:"reason,omitempty"`
	CreatedAt    time.Time        `json:"created_at"`
	PromotedAt   time.Time        `json:"promoted_at,omitempty"`
	Canary       *CanaryReport    `json:"canary,omitempty"`
	canaryPassed bool
}

type Manager struct {
	mu       sync.Mutex
	current  atomic.Pointer[Release]
	history  []*Release
	active   *Release
	sequence atomic.Int64
	// OnSwap is invoked outside the manager lock whenever the serving
	// snapshot changes (promotion or rollback).
	OnSwap func(previous, next *Release)
}

func NewManager(initial *config.Config) (*Manager, error) {
	snapshot, err := initial.Snapshot()
	if err != nil {
		return nil, err
	}
	manager := &Manager{}
	first := &Release{
		ID:         "r0001",
		Revision:   "rev-1",
		Stage:      StagePromoted,
		Config:     initial,
		Snapshot:   snapshot,
		Reason:     "initial configuration",
		CreatedAt:  time.Now().UTC(),
		PromotedAt: time.Now().UTC(),
	}
	manager.current.Store(first)
	manager.history = []*Release{first}
	manager.sequence.Store(1)
	return manager, nil
}

func (m *Manager) Current() *Release {
	return m.current.Load()
}

func (m *Manager) nextID() string {
	n := m.sequence.Add(1)
	return fmt.Sprintf("r%04d", n)
}

// Validate performs independent validation of a proposed configuration
// without touching the serving snapshot or any pending candidate.
func (m *Manager) Validate(cfg *config.Config) (*config.Snapshot, error) {
	if cfg == nil {
		return nil, fmt.Errorf("configuration is nil")
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("configuration is not serializable: %w", err)
	}
	var copy config.Config
	if err := json.Unmarshal(data, &copy); err != nil {
		return nil, fmt.Errorf("configuration is not valid json: %w", err)
	}
	return copy.Snapshot()
}

// Propose validates the configuration independently and records it as a
// reviewable candidate. Any previous candidate is retained in history.
func (m *Manager) Propose(cfg *config.Config, reason string) (*Release, error) {
	snapshot, err := m.Validate(cfg)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active != nil && m.active.Stage == StageCanary {
		return nil, fmt.Errorf("canary %s is in progress; conclude it before proposing another candidate", m.active.ID)
	}
	parent := m.current.Load()
	candidate := &Release{
		ID:        m.nextID(),
		Revision:  fmt.Sprintf("rev-%d", m.sequence.Load()),
		ParentID:  parent.ID,
		Stage:     StageCandidate,
		Config:    cfg,
		Snapshot:  snapshot,
		Reason:    reason,
		CreatedAt: time.Now().UTC(),
	}
	m.active = candidate
	m.history = append(m.history, candidate)
	return cloneRelease(candidate), nil
}

func (m *Manager) Get(id string) (*Release, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.history {
		if r.ID == id {
			return cloneRelease(r), true
		}
	}
	return nil, false
}

// BeginCanary opens a bounded rehearsal window for a candidate. Only requests
// explicitly tagged with the candidate revision participate.
func (m *Manager) BeginCanary(id string, maxRequests int64) (*Release, error) {
	if maxRequests <= 0 {
		return nil, fmt.Errorf("canary max_requests must be positive")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	candidate, err := m.lockedRequireActive(id)
	if err != nil {
		return nil, err
	}
	if candidate.Stage != StageCandidate {
		return nil, fmt.Errorf("release %s is in stage %s, not candidate", id, candidate.Stage)
	}
	now := time.Now().UTC()
	candidate.Stage = StageCanary
	candidate.Canary = &CanaryReport{MaxRequests: maxRequests, StartedAt: now}
	candidate.Canary.maxRequests = maxRequests
	candidate.Canary.remaining = maxRequests
	return cloneRelease(candidate), nil
}

// EndCanary closes the rehearsal window. The candidate is accepted only when
// at least one rehearsal request ran and every one succeeded.
func (m *Manager) EndCanary(id string) (*Release, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	candidate, err := m.lockedRequireActive(id)
	if err != nil {
		return nil, err
	}
	if candidate.Stage != StageCanary {
		return nil, fmt.Errorf("release %s is not in canary stage", id)
	}
	candidate.Canary.EndedAt = time.Now().UTC()
	if candidate.Canary.Requests == 0 {
		candidate.Stage = StageRejected
		candidate.Reason = appendReason(candidate.Reason, "canary rejected: no rehearsal requests observed")
		m.active = nil
		return cloneRelease(candidate), fmt.Errorf("canary %s rejected: no rehearsal requests observed", id)
	}
	if candidate.Canary.Failures > 0 {
		candidate.Stage = StageRejected
		candidate.Reason = appendReason(candidate.Reason, fmt.Sprintf("canary rejected: %d of %d rehearsal requests failed", candidate.Canary.Failures, candidate.Canary.Requests))
		m.active = nil
		return cloneRelease(candidate), fmt.Errorf("canary %s rejected: %d/%d rehearsal requests failed", id, candidate.Canary.Failures, candidate.Canary.Requests)
	}
	candidate.Stage = StageCandidate
	candidate.canaryPassed = true
	return cloneRelease(candidate), nil
}

// Promote performs the whole-config cutover. Requests that already pinned a
// snapshot keep using the previous revision; only new requests use the new one.
func (m *Manager) Promote(id, reason string) (*Release, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var candidate *Release
	if id != "" {
		var err error
		candidate, err = m.lockedRequireActive(id)
		if err != nil {
			return nil, err
		}
	} else {
		candidate = m.active
		if candidate == nil {
			return nil, fmt.Errorf("no active candidate to promote")
		}
	}
	if candidate.Stage != StageCandidate {
		return nil, fmt.Errorf("release %s is in stage %s; conclude canary before promotion", id, candidate.Stage)
	}
	if !candidate.canaryPassed {
		return nil, fmt.Errorf("release %s must pass a bounded canary rehearsal before promotion", id)
	}
	now := time.Now().UTC()
	candidate.Stage = StagePromoted
	candidate.PromotedAt = now
	if reason != "" {
		candidate.Reason = appendReason(candidate.Reason, reason)
	}
	previous := m.current.Load()
	m.current.Store(candidate)
	m.active = nil
	m.notify(previous, candidate)
	return cloneRelease(candidate), nil
}

// Rollback creates a new, traceable promotion record built from a previous
// release configuration. The failed release stays in history untouched.
func (m *Manager) Rollback(targetID, reason string) (*Release, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var target *Release
	for _, r := range m.history {
		if r.ID == targetID {
			target = r
			break
		}
	}
	if target == nil {
		return nil, fmt.Errorf("release %s not found", targetID)
	}
	if target.Snapshot == nil {
		return nil, fmt.Errorf("release %s has no valid snapshot", targetID)
	}
	current := m.current.Load()
	now := time.Now().UTC()
	rolledBack := &Release{
		ID:         m.nextID(),
		Revision:   fmt.Sprintf("rev-%d", m.sequence.Load()),
		ParentID:   current.ID,
		Stage:      StageRolledBack,
		Config:     target.Config,
		Snapshot:   target.Snapshot,
		Reason:     fmt.Sprintf("rollback to %s: %s", target.Revision, reason),
		CreatedAt:  now,
		PromotedAt: now,
	}
	previous := m.current.Load()
	m.current.Store(rolledBack)
	m.history = append(m.history, rolledBack)
	m.active = nil
	m.notify(previous, rolledBack)
	return cloneRelease(rolledBack), nil
}

// DirectPromote validates and immediately swaps in a new configuration.
// It is used for the legacy file reload path and still records an auditable
// release rather than mutating history in place.
func (m *Manager) DirectPromote(cfg *config.Config, reason string) (*Release, error) {
	candidate, err := m.Propose(cfg, reason)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.active.canaryPassed = true
	m.mu.Unlock()
	return m.Promote(candidate.ID, reason)
}

func (m *Manager) notify(previous, next *Release) {
	if m.OnSwap != nil {
		go m.OnSwap(cloneRelease(previous), cloneRelease(next))
	}
}

// Reject abandons a candidate while retaining it for review.
func (m *Manager) Reject(id, reason string) (*Release, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	candidate, err := m.lockedRequireActive(id)
	if err != nil {
		return nil, err
	}
	candidate.Stage = StageRejected
	candidate.Reason = appendReason(candidate.Reason, "rejected: "+reason)
	m.active = nil
	return cloneRelease(candidate), nil
}

func (m *Manager) History() []*Release {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*Release, 0, len(m.history))
	for _, r := range m.history {
		out = append(out, cloneRelease(r))
	}
	return out
}

// ActiveCandidate returns the candidate currently accepting canary traffic.
func (m *Manager) ActiveCanary() *Release {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active != nil && m.active.Stage == StageCanary {
		return cloneRelease(m.active)
	}
	return nil
}

// CanaryAllow reserves one slot in the bounded rehearsal. Returns false when
// the window is closed or the budget is exhausted.
func (m *Manager) CanaryAllow(revision string) (*Release, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active == nil || m.active.Stage != StageCanary {
		return nil, false
	}
	if m.active.Revision != revision {
		return nil, false
	}
	if m.active.Canary.remaining <= 0 {
		return nil, false
	}
	m.active.Canary.remaining--
	return cloneRelease(m.active), true
}

// CanaryRecord reports the outcome of one rehearsal request.
func (m *Manager) CanaryRecord(id string, status int, failed bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active == nil || m.active.ID != id || m.active.Canary == nil {
		return
	}
	m.active.Canary.Requests++
	m.active.Canary.Statuses = append(m.active.Canary.Statuses, status)
	if failed {
		m.active.Canary.Failures++
	}
}

func (m *Manager) lockedRequireActive(id string) (*Release, error) {
	if m.active == nil {
		return nil, fmt.Errorf("no active candidate release")
	}
	if m.active.ID != id {
		return nil, fmt.Errorf("release %s is not the active candidate", id)
	}
	return m.active, nil
}

func appendReason(existing, addition string) string {
	if existing == "" {
		return addition
	}
	return existing + "; " + addition
}

func cloneRelease(r *Release) *Release {
	copy := *r
	if r.Canary != nil {
		canary := *r.Canary
		canary.Statuses = append([]int(nil), r.Canary.Statuses...)
		copy.Canary = &canary
	}
	return &copy
}
