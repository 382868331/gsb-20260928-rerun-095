package records

import (
	"sync"
	"time"
)

type Record struct {
	ID           int64     `json:"id"`
	StartedAt    time.Time `json:"started_at"`
	FinishedAt   time.Time `json:"finished_at"`
	Method       string    `json:"method"`
	Path         string    `json:"path"`
	RouteID      string    `json:"route_id"`
	RouteVersion string    `json:"route_version"`
	Attempts     int       `json:"attempts"`
	UpstreamIDs  []string  `json:"upstream_ids"`
	StatusCode   int       `json:"status_code"`
	BytesSent    int64     `json:"bytes_sent"`
	Canceled     bool      `json:"canceled"`
	Error        string    `json:"error,omitempty"`
}

type Recorder struct {
	mu      sync.Mutex
	nextID  int64
	records map[int64]Record
	order   []int64
	limit   int
}

func NewRecorder(limit int) *Recorder {
	if limit <= 0 {
		limit = 1000
	}
	return &Recorder{limit: limit, records: make(map[int64]Record)}
}

func (r *Recorder) Start(method, path string) Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	record := Record{
		ID:        r.nextID,
		StartedAt: time.Now().UTC(),
		Method:    method,
		Path:      path,
	}
	r.records[record.ID] = record
	r.order = append(r.order, record.ID)
	r.trimLocked()
	return record
}

func (r *Recorder) Finish(record Record) {
	record.FinishedAt = time.Now().UTC()
	r.mu.Lock()
	defer r.mu.Unlock()
	if record.ID < 1 {
		return
	}
	r.records[record.ID] = record
	r.trimLocked()
}

func (r *Recorder) Snapshot() []Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]Record, 0, len(r.order))
	for _, id := range r.order {
		result = append(result, r.records[id])
	}
	return result
}

func (r *Recorder) trimLocked() {
	if len(r.order) > r.limit {
		removed := r.order[:len(r.order)-r.limit]
		for _, id := range removed {
			delete(r.records, id)
		}
		r.order = append([]int64(nil), r.order[len(r.order)-r.limit:]...)
	}
}
