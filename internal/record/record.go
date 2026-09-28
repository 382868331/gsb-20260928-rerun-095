package record

import (
	"sync"
	"time"
)

type Record struct {
	ID            string        `json:"id"`
	StartedAt     time.Time     `json:"started_at"`
	Duration      time.Duration `json:"duration"`
	RouteID       string        `json:"route_id"`
	Version       string        `json:"version"`
	UpstreamID    string        `json:"upstream_id"`
	Method        string        `json:"method"`
	Path          string        `json:"path"`
	Query         string        `json:"query,omitempty"`
	Status        int           `json:"status"`
	Attempts      int           `json:"attempts"`
	BytesIn       int64         `json:"bytes_in"`
	BytesOut      int64         `json:"bytes_out"`
	ClientAddress string        `json:"client_address"`
	Error         string        `json:"error,omitempty"`
	Canceled      bool          `json:"canceled"`
}

type Store struct {
	mu      sync.Mutex
	records []Record
	limit   int
}

func NewStore(limit int) *Store {
	if limit <= 0 {
		limit = 1000
	}
	return &Store{limit: limit}
}

func (s *Store) Add(record Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, record)
	if len(s.records) > s.limit {
		s.records = s.records[len(s.records)-s.limit:]
	}
}

func (s *Store) Snapshot() []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Record, len(s.records))
	copy(result, s.records)
	return result
}
