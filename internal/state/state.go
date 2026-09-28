package state

import (
	"sync"
	"time"
)

type UpstreamState struct {
	ID                string    `json:"id"`
	ActiveRequests    int64     `json:"active_requests"`
	TotalRequests     int64     `json:"total_requests"`
	SuccessRequests   int64     `json:"success_requests"`
	FailedRequests    int64     `json:"failed_requests"`
	RetriedAttempts   int64     `json:"retried_attempts"`
	ConsecutiveErrors int64     `json:"consecutive_errors"`
	LastError         string    `json:"last_error,omitempty"`
	LastUsedAt        time.Time `json:"last_used_at"`
	LastSucceededAt   time.Time `json:"last_succeeded_at,omitempty"`
	LastFailedAt      time.Time `json:"last_failed_at,omitempty"`
}

type Store struct {
	mu     sync.Mutex
	values map[string]*UpstreamState
}

func NewStore() *Store {
	return &Store{values: make(map[string]*UpstreamState)}
}

func (s *Store) Begin(id string) func(bool, string) {
	s.mu.Lock()
	value := s.getLocked(id)
	value.ActiveRequests++
	value.TotalRequests++
	value.LastUsedAt = time.Now()
	s.mu.Unlock()

	return func(success bool, errText string) {
		s.mu.Lock()
		defer s.mu.Unlock()
		value = s.getLocked(id)
		value.ActiveRequests--
		if success {
			value.SuccessRequests++
			value.ConsecutiveErrors = 0
			value.LastError = ""
			value.LastSucceededAt = time.Now()
		} else {
			value.FailedRequests++
			value.ConsecutiveErrors++
			value.LastError = errText
			value.LastFailedAt = time.Now()
		}
	}
}

func (s *Store) AddRetry(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getLocked(id).RetriedAttempts++
}

func (s *Store) Snapshot() []UpstreamState {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]UpstreamState, 0, len(s.values))
	for _, value := range s.values {
		result = append(result, *value)
	}
	return result
}

func (s *Store) getLocked(id string) *UpstreamState {
	value, ok := s.values[id]
	if !ok {
		value = &UpstreamState{ID: id}
		s.values[id] = value
	}
	return value
}
