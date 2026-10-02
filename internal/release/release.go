package release

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
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

// CandidateState tells why a candidate can or cannot be released.
type CandidateState string

const (
	// StateStaged: no rehearsal has happened yet.
	StateStaged CandidateState = "staged"
	// StatePlanPending: the coverage plan exists but has not been
	// acknowledged before rehearsal start.
	StatePlanPending CandidateState = "plan_pending"
	// StateRehearsing: at least one rehearsal is still in flight.
	StateRehearsing CandidateState = "rehearsing"
	// StateInsufficient: rehearsals finished but did not meet the
	// requirement (failures or too few successes).
	StateInsufficient CandidateState = "insufficient"
	// StateReady: the rehearsal requirement is satisfied.
	StateReady CandidateState = "ready"
	// StateSuperseded: the candidate was revised (routing or limits
	// changed); its rehearsal evidence is invalid.
	StateSuperseded CandidateState = "superseded"
	// StateDiscarded: the candidate was explicitly abandoned.
	StateDiscarded CandidateState = "discarded"
	// StateConfirmed: the candidate was promoted and became a release.
	StateConfirmed CandidateState = "confirmed"
)

var (
	// ErrUnknownCandidate is returned when a candidate id does not exist.
	ErrUnknownCandidate = errors.New("unknown candidate")
	// ErrNotStaged is returned when an operation needs a staged candidate
	// but the candidate has already been resolved.
	ErrNotStaged = errors.New("candidate is no longer staged")
	// ErrPlanPending rejects rehearsal traffic before the coverage plan was
	// acknowledged.
	ErrPlanPending = errors.New("coverage plan has not been acknowledged")
)

// RehearsalSummary is the accumulated evidence gathered by rehearsal traffic.
type RehearsalSummary struct {
	RequiredSuccesses int64 `json:"required_successes"`
	InFlight          int64 `json:"in_flight"`
	Succeeded         int64 `json:"succeeded"`
	Failed            int64 `json:"failed"`
	Canceled          int64 `json:"canceled"`
	Aborted           int64 `json:"aborted"`
}

// Readiness explains whether a candidate is releasable and why.
type Readiness struct {
	State   CandidateState   `json:"state"`
	Ready   bool             `json:"ready"`
	Reason  string           `json:"reason,omitempty"`
	Summary RehearsalSummary `json:"summary"`
	Reasons []string         `json:"reasons,omitempty"`
}

// Release is an immutable audit record of one activation. Rollbacks append a
// new record instead of rewriting history.
type Release struct {
	ID          string            `json:"id"`
	Kind        Kind              `json:"kind"`
	CreatedAt   time.Time         `json:"created_at"`
	SourceID    string            `json:"source_id,omitempty"`
	Routes      []string          `json:"routes"`
	Fingerprint string            `json:"fingerprint"`
	Rehearsal   *RehearsalSummary `json:"rehearsal,omitempty"`
	Coverage    *CoverageBasis    `json:"coverage,omitempty"`

	snapshot *config.Snapshot
}

// Candidate is a validated configuration that only serves rehearsal traffic
// until it is confirmed, discarded, superseded or promoted. Candidates are
// never deleted so the full rehearsal and release trail stays auditable.
type Candidate struct {
	ID            string           `json:"id"`
	CreatedAt     time.Time        `json:"created_at"`
	ResolvedAt    time.Time        `json:"resolved_at,omitempty"`
	State         CandidateState   `json:"state"`
	Routes        []string         `json:"routes"`
	Fingerprint   string           `json:"fingerprint"`
	Rehearsal     RehearsalSummary `json:"rehearsal"`
	Readiness     Readiness        `json:"readiness"`
	SupersededBy  string           `json:"superseded_by,omitempty"`
	InvalidReason string           `json:"invalid_reason,omitempty"`

	snapshot         *config.Snapshot
	plan             *config.CoveragePlan
	planAcknowledged bool
	planAckAt        time.Time
	planAckSource    string
	evidence         []Evidence
	seenSamples      map[string]struct{}
	supplements      []*SupplementBatch
	supplementOpen   map[string]int
	supplementSeq    int
}

// RehearsalOutcome reports how one pinned rehearsal request ended.
type RehearsalOutcome struct {
	Status     int
	Error      string
	Canceled   bool
	Aborted    bool
	Method     string
	Path       string
	Header     http.Header
	RequestID  string
	SampleID   string
	Supplement string
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
	initial := store.appendReleaseLocked(KindInitial, "", snapshot, nil, nil)
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

// Selection is the snapshot one request is bound to for its whole lifetime.
type Selection struct {
	Snapshot    *config.Snapshot
	ID          string
	CandidateID string
	// PlanPending is true when the header names a staged candidate whose
	// coverage plan still needs explicit acknowledgement; the request must
	// not be served by that candidate.
	PlanPending bool
	Supplement  string
}

// Select picks the snapshot for one request: rehearsal traffic pinned to a
// staged candidate via RehearsalHeader uses the candidate snapshot,
// everything else uses the active release. An unknown or resolved candidate
// id falls back to the active release instead of being served as a candidate.
func (s *Store) Select(request *http.Request) Selection {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id := request.Header.Get(RehearsalHeader); id != "" {
		if candidate, ok := s.candidates[id]; ok && candidate.State == StateStaged {
			if candidate.plan != nil && !candidate.planAcknowledged {
				return Selection{Snapshot: s.active, ID: s.activeID, CandidateID: candidate.ID, PlanPending: true}
			}
			supplement := request.Header.Get(SupplementHeader)
			if supplement != "" {
				if _, ok := candidate.supplementOpen[supplement]; !ok {
					// Unknown or closed batches must not silently serve the
					// candidate: fall back to the active release instead.
					return Selection{Snapshot: s.active, ID: s.activeID}
				}
			}
			candidate.Rehearsal.InFlight++
			return Selection{Snapshot: candidate.snapshot, ID: candidate.ID, CandidateID: candidate.ID, Supplement: supplement}
		}
	}
	return Selection{Snapshot: s.active, ID: s.activeID}
}

// FinishRehearsal records the terminal outcome of one pinned rehearsal
// request. Evidence is attributed to the candidate revision the request was
// bound to even if the candidate was superseded or discarded while the
// request was in flight.
func (s *Store) FinishRehearsal(candidateID string, outcome RehearsalOutcome) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, ok := s.candidates[candidateID]
	if !ok {
		return
	}
	if candidate.Rehearsal.InFlight > 0 {
		candidate.Rehearsal.InFlight--
	}
	sampleKey := outcome.SampleID
	if sampleKey == "" {
		sampleKey = outcome.RequestID
	}
	duplicate := false
	if sampleKey != "" {
		if candidate.seenSamples == nil {
			candidate.seenSamples = map[string]struct{}{}
		}
		if _, seen := candidate.seenSamples[sampleKey]; seen {
			duplicate = true
		}
	}
	result := classifyOutcome(outcome)
	categories := matchCategories(candidate.plan, outcome.Method, outcome.Path, outcome.Header)
	evidence := Evidence{
		RequestID:    outcome.RequestID,
		SampleID:     outcome.SampleID,
		Method:       outcome.Method,
		Path:         outcome.Path,
		Status:       outcome.Status,
		Outcome:      result,
		Valid:        result == OutcomeValid && len(categories) > 0,
		Duplicate:    duplicate,
		Categories:   categories,
		SupplementID: outcome.Supplement,
		RecordedAt:   time.Now(),
	}
	if outcome.Supplement != "" {
		if index, ok := candidate.supplementOpen[outcome.Supplement]; ok {
			batch := candidate.supplements[index]
			evidence.Source = batch.Source
			switch {
			case duplicate:
				batch.Duplicates++
			case evidence.Valid:
				batch.Valid++
			case result == OutcomeFailed:
				batch.Failed++
			case result == OutcomeCanceled:
				batch.Canceled++
			case result == OutcomeAborted:
				batch.Aborted++
			}
		}
	}
	if !duplicate {
		if sampleKey != "" {
			candidate.seenSamples[sampleKey] = struct{}{}
		}
		switch result {
		case OutcomeValid:
			candidate.Rehearsal.Succeeded++
		case OutcomeFailed:
			candidate.Rehearsal.Failed++
		case OutcomeCanceled:
			candidate.Rehearsal.Canceled++
		case OutcomeAborted:
			candidate.Rehearsal.Aborted++
		}
	}
	candidate.evidence = append(candidate.evidence, evidence)
}

func classifyOutcome(outcome RehearsalOutcome) EvidenceOutcome {
	switch {
	case outcome.Aborted:
		return OutcomeAborted
	case outcome.Canceled:
		return OutcomeCanceled
	case outcome.Error != "" || outcome.Status == 0 || outcome.Status >= http.StatusInternalServerError:
		return OutcomeFailed
	default:
		return OutcomeValid
	}
}

// Stage registers an already validated snapshot as a candidate. It never
// touches the active release. Routing or limit changes invalidate the
// rehearsal evidence of every still-staged candidate: it is marked
// superseded and can no longer be confirmed. Re-staging an unchanged
// candidate returns the existing staged candidate instead of opening a
// parallel revision.
func (s *Store) Stage(snapshot *config.Snapshot) Candidate {
	s.mu.Lock()
	defer s.mu.Unlock()
	fingerprint := Fingerprint(snapshot)
	s.candSeq++
	revision := fmt.Sprintf("cand-%d", s.candSeq)
	for _, existing := range s.candidates {
		if existing.State != StateStaged {
			continue
		}
		if existing.Fingerprint == fingerprint {
			copied := *existing
			copied.Readiness = existing.readinessLocked()
			return copied
		}
		existing.State = StateSuperseded
		existing.ResolvedAt = time.Now()
		existing.SupersededBy = revision
		existing.InvalidReason = s.supersedeReasonLocked(existing, snapshot)
	}
	candidate := &Candidate{
		ID:          revision,
		CreatedAt:   time.Now(),
		State:       StateStaged,
		Routes:      snapshot.DescribeRoutes(),
		Fingerprint: fingerprint,
		Rehearsal: RehearsalSummary{
			RequiredSuccesses: int64(snapshot.MinSuccessfulRehearsals()),
		},
		snapshot:       snapshot,
		plan:           snapshot.CoveragePlan(),
		seenSamples:    map[string]struct{}{},
		supplementOpen: map[string]int{},
	}
	s.candidates[candidate.ID] = candidate
	return *candidate
}

// Confirm promotes a candidate to the active release and records the switch
// in the audit history. Promotion is gated on completed, successful limited
// rehearsal: a fresh candidate, an unfinished rehearsal, a failed rehearsal
// or a superseded/discarded revision cannot be released.
func (s *Store) Confirm(candidateID string) (Release, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, ok := s.candidates[candidateID]
	if !ok {
		return Release{}, fmt.Errorf("%w %q", ErrUnknownCandidate, candidateID)
	}
	if readiness := candidate.readinessLocked(); !readiness.Ready {
		return Release{}, fmt.Errorf("candidate %q cannot be confirmed: %s%s", candidateID, readiness.Reason,
			func() string {
				if len(readiness.Reasons) == 0 {
					return ""
				}
				return ": " + strings.Join(readiness.Reasons, "; ")
			}())
	}
	summary := candidate.Rehearsal
	candidate.State = StateConfirmed
	candidate.ResolvedAt = time.Now()
	var basis *CoverageBasis
	if candidate.plan != nil {
		b := buildReport(candidate, s.active)
		basis = &CoverageBasis{Overall: b.Overall, Plan: b.Plan, Category: b.Categories, Evidence: b.Evidence}
	}
	release := s.appendReleaseLocked(KindPromote, candidate.ID, candidate.snapshot, &summary, basis)
	s.active = candidate.snapshot
	s.activeID = release.ID
	return release, nil
}

// Discard abandons a staged candidate without promoting it. Its rehearsal
// evidence stays visible in the candidate history. In-flight rehearsal
// requests pinned to it still finish on that snapshot and are attributed to
// the discarded revision.
func (s *Store) Discard(candidateID string) (Candidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, ok := s.candidates[candidateID]
	if !ok {
		return Candidate{}, fmt.Errorf("%w %q", ErrUnknownCandidate, candidateID)
	}
	if candidate.State != StateStaged {
		return Candidate{}, fmt.Errorf("%w: %s", ErrNotStaged, candidate.State)
	}
	candidate.State = StateDiscarded
	candidate.ResolvedAt = time.Now()
	return *candidate, nil
}

// Candidate returns one candidate revision by id.
func (s *Store) Candidate(candidateID string) (Candidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, ok := s.candidates[candidateID]
	if !ok {
		return Candidate{}, fmt.Errorf("%w %q", ErrUnknownCandidate, candidateID)
	}
	return *candidate, nil
}

// SnapshotForCandidate exposes a candidate snapshot and metadata for diffing.
func (s *Store) SnapshotForCandidate(candidateID string) (*config.Snapshot, Candidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, ok := s.candidates[candidateID]
	if !ok {
		return nil, Candidate{}, fmt.Errorf("%w %q", ErrUnknownCandidate, candidateID)
	}
	return candidate.snapshot, *candidate, nil
}

// Rollback activates the configuration of an earlier release as a brand new
// release record, keeping the failed release in history. An empty targetID
// selects the release that was active before the current one. In-flight
// requests keep their selected snapshot; only new requests see the rollback.
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
	release := s.appendReleaseLocked(KindRollback, target.ID, target.snapshot, nil, nil)
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

// Candidates returns every candidate revision, including resolved ones, so
// rehearsal evidence and decisions stay traceable.
func (s *Store) Candidates() []Candidate {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Candidate, 0, len(s.candidates))
	for _, candidate := range s.candidates {
		copied := *candidate
		copied.Readiness = candidate.readinessLocked()
		result = append(result, copied)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func (c *Candidate) readinessLocked() Readiness {
	readiness := Readiness{State: c.State, Summary: c.Rehearsal}
	switch c.State {
	case StateStaged:
		switch {
		case c.plan != nil && !c.planAcknowledged:
			readiness.State = StatePlanPending
			readiness.Reason = fmt.Sprintf("coverage plan with %d category(ies) must be acknowledged before rehearsal starts", len(c.plan.Categories))
		case c.Rehearsal.InFlight > 0:
			readiness.State = StateRehearsing
			readiness.Reason = fmt.Sprintf("rehearsal in progress: %d request(s) still running", c.Rehearsal.InFlight)
		case c.Rehearsal.Succeeded < c.Rehearsal.RequiredSuccesses &&
			(c.Rehearsal.Failed > 0 || c.Rehearsal.Canceled > 0 || c.Rehearsal.Aborted > 0):
			readiness.State = StateInsufficient
			readiness.Reason = fmt.Sprintf("rehearsal did not pass: %d failed, %d canceled, %d aborted",
				c.Rehearsal.Failed, c.Rehearsal.Canceled, c.Rehearsal.Aborted)
		case c.Rehearsal.Succeeded < c.Rehearsal.RequiredSuccesses:
			readiness.State = StateInsufficient
			readiness.Reason = fmt.Sprintf("rehearsal incomplete: %d/%d successful request(s)",
				c.Rehearsal.Succeeded, c.Rehearsal.RequiredSuccesses)
		default:
			if c.plan != nil {
				report := buildCandidateReport(c)
				var reasons []string
				allSatisfied := true
				for _, category := range report.Categories {
					if !category.Satisfied {
						allSatisfied = false
						for _, reason := range category.Reasons {
							reasons = append(reasons, category.Plan.ID+": "+reason)
						}
					}
				}
				if !allSatisfied {
					readiness.State = StateInsufficient
					readiness.Reason = "coverage plan not satisfied"
					readiness.Reasons = reasons
					return readiness
				}
			}
			readiness.State = StateReady
			readiness.Ready = true
		}
	default:
		readiness.Reason = "candidate is " + string(c.State)
	}
	return readiness
}

func (s *Store) appendReleaseLocked(kind Kind, sourceID string, snapshot *config.Snapshot, summary *RehearsalSummary, basis *CoverageBasis) Release {
	s.seq++
	release := Release{
		ID:          fmt.Sprintf("rel-%d", s.seq),
		Kind:        kind,
		CreatedAt:   time.Now(),
		SourceID:    sourceID,
		Routes:      snapshot.DescribeRoutes(),
		Fingerprint: Fingerprint(snapshot),
		snapshot:    snapshot,
	}
	if summary != nil {
		copied := *summary
		release.Rehearsal = &copied
	}
	release.Coverage = basis
	s.history = append(s.history, release)
	return release
}

// Fingerprint hashes the routing and limiting content of a snapshot:

// upstream targets/timeouts and route prefixes, versions and retry limits.
// Listener, admin and drain settings are excluded because changing them does
// not invalidate routing rehearsal evidence.
func Fingerprint(snapshot *config.Snapshot) string {
	payload := struct {
		Upstreams []config.Upstream    `json:"upstreams"`
		Routes    []config.Route       `json:"routes"`
		Coverage  *config.CoveragePlan `json:"coverage,omitempty"`
	}{
		Upstreams: snapshot.Upstreams(),
		Routes:    snapshot.Routes(),
		Coverage:  snapshot.CoveragePlan(),
	}
	data, _ := json.Marshal(payload)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:12])
}
