package release

import (
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"gateway/internal/config"
)

// RehearsalHeader selects a staged candidate for a single request, which is
// how limited rehearsal traffic is directed before a release is confirmed.
const RehearsalHeader = "X-Gateway-Release"

type Kind string

const (
	KindInitial  Kind = "initial"
	KindPromote  Kind = "promote"
	KindRollback Kind = "rollback"
)

// Release is an immutable audit record of one activation. Rollbacks append a
// new record instead of rewriting history.
type Release struct {
	ID        string    `json:"id"`
	Kind      Kind      `json:"kind"`
	CreatedAt time.Time `json:"created_at"`
	SourceID  string    `json:"source_id,omitempty"`
	Routes    []string  `json:"routes"`

	snapshot *config.Snapshot
}

// Candidate is a validated configuration that only serves rehearsal traffic
// until it is confirmed.
type Candidate struct {
	ID                string    `json:"id"`
	CreatedAt         time.Time `json:"created_at"`
	Routes            []string  `json:"routes"`
	RehearsalRequests int64     `json:"rehearsal_requests"`

	snapshot *config.Snapshot
}

type Store struct {
	mu         sync.Mutex
	active     *config.Snapshot
	activeID   string
	seq        int
	candSeq    int
	candidates map[string]*Candidate
	history    []Release
}

func NewStore(snapshot *config.Snapshot) *Store {
	store := &Store{candidates: make(map[string]*Candidate)}
	store.active = snapshot
	initial := store.appendReleaseLocked(KindInitial, "", snapshot)
	store.activeID = initial.ID
	return store
}

func (s *Store) Current() *config.Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active
}

func (s *Store) CurrentID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.activeID
}

// Select picks the snapshot for one request: rehearsal traffic pinned to a
// candidate via RehearsalHeader uses the candidate snapshot, everything else
// uses the active release. The returned ID identifies the release (or
// candidate) the request belongs to.
func (s *Store) Select(request *http.Request) (*config.Snapshot, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id := request.Header.Get(RehearsalHeader); id != "" {
		if candidate, ok := s.candidates[id]; ok {
			candidate.RehearsalRequests++
			return candidate.snapshot, candidate.ID
		}
	}
	return s.active, s.activeID
}

// Stage registers an already validated snapshot as a candidate. It never
// touches the active release.
func (s *Store) Stage(snapshot *config.Snapshot) Candidate {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.candSeq++
	candidate := &Candidate{
		ID:        fmt.Sprintf("cand-%d", s.candSeq),
		CreatedAt: time.Now(),
		Routes:    snapshot.DescribeRoutes(),
		snapshot:  snapshot,
	}
	s.candidates[candidate.ID] = candidate
	return *candidate
}

// Confirm promotes a candidate to the active release and records the switch
// in the audit history.
func (s *Store) Confirm(candidateID string) (Release, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, ok := s.candidates[candidateID]
	if !ok {
		return Release{}, fmt.Errorf("unknown candidate %q", candidateID)
	}
	delete(s.candidates, candidateID)
	release := s.appendReleaseLocked(KindPromote, candidate.ID, candidate.snapshot)
	s.active = candidate.snapshot
	s.activeID = release.ID
	return release, nil
}

// Rollback activates the configuration of an earlier release as a brand new
// release record, keeping the failed release in history. An empty targetID
// selects the release that was active before the current one.
func (s *Store) Rollback(targetID string) (Release, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var target *Release
	if targetID == "" {
		for i := len(s.history) - 1; i >= 0; i-- {
			if s.history[i].ID != s.activeID {
				target = &s.history[i]
				break
			}
		}
	} else {
		for i := range s.history {
			if s.history[i].ID == targetID {
				target = &s.history[i]
				break
			}
		}
	}
	if target == nil {
		if targetID == "" {
			return Release{}, errors.New("no earlier release to roll back to")
		}
		return Release{}, fmt.Errorf("unknown release %q", targetID)
	}
	release := s.appendReleaseLocked(KindRollback, target.ID, target.snapshot)
	s.active = target.snapshot
	s.activeID = release.ID
	return release, nil
}

func (s *Store) History() []Release {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Release, len(s.history))
	copy(result, s.history)
	return result
}

func (s *Store) Candidates() []Candidate {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Candidate, 0, len(s.candidates))
	for _, candidate := range s.candidates {
		result = append(result, *candidate)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func (s *Store) appendReleaseLocked(kind Kind, sourceID string, snapshot *config.Snapshot) Release {
	s.seq++
	release := Release{
		ID:        fmt.Sprintf("rel-%d", s.seq),
		Kind:      kind,
		CreatedAt: time.Now(),
		SourceID:  sourceID,
		Routes:    snapshot.DescribeRoutes(),
		snapshot:  snapshot,
	}
	s.history = append(s.history, release)
	return release
}
