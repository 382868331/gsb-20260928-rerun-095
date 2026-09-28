package state

import (
	"sync"
	"time"
)

type Result string

const (
	ResultSuccess Result = "success"
	ResultFailure Result = "failure"
)

type UpstreamStatus struct {
	ID                 string    `json:"id"`
	Requests           int64     `json:"requests"`
	Successes          int64     `json:"successes"`
	Failures           int64     `json:"failures"`
	ConsecutiveFailure int64     `json:"consecutive_failures"`
	LastResult         Result    `json:"last_result"`
	LastChangedAt      time.Time `json:"last_changed_at"`
}

type Upstream struct {
	mu     sync.Mutex
	status UpstreamStatus
}

func NewUpstream(id string) *Upstream {
	return &Upstream{status: UpstreamStatus{ID: id}}
}

func (u *Upstream) Record(result Result) UpstreamStatus {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.status.Requests++
	u.status.LastResult = result
	u.status.LastChangedAt = time.Now().UTC()
	if result == ResultSuccess {
		u.status.Successes++
		u.status.ConsecutiveFailure = 0
	} else {
		u.status.Failures++
		u.status.ConsecutiveFailure++
	}
	return u.status
}

func (u *Upstream) Snapshot() UpstreamStatus {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.status
}

type Registry struct {
	upstreams map[string]*Upstream
}

func NewRegistry(ids []string) *Registry {
	upstreams := make(map[string]*Upstream, len(ids))
	for _, id := range ids {
		upstreams[id] = NewUpstream(id)
	}
	return &Registry{upstreams: upstreams}
}

func (r *Registry) EnsureIDs(ids []string) {
	for _, id := range ids {
		if _, exists := r.upstreams[id]; !exists {
			r.upstreams[id] = NewUpstream(id)
		}
	}
}

func (r *Registry) Merge(ids []string) *Registry {
	upstreams := make(map[string]*Upstream, len(ids))
	for _, id := range ids {
		if existing := r.upstreams[id]; existing != nil {
			upstreams[id] = existing
		} else {
			upstreams[id] = NewUpstream(id)
		}
	}
	return &Registry{upstreams: upstreams}
}

func (r *Registry) Get(id string) *Upstream {
	return r.upstreams[id]
}

func (r *Registry) Snapshot() []UpstreamStatus {
	statuses := make([]UpstreamStatus, 0, len(r.upstreams))
	for _, upstream := range r.upstreams {
		statuses = append(statuses, upstream.Snapshot())
	}
	return statuses
}
