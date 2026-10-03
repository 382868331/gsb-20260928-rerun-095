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

// RehearsalSourceHeader labels which rehearsal batch a pinned request
// belongs to. Supplementary runs that top up missing coverage categories
// use a distinct source so the report keeps each batch's origin and result.
const RehearsalSourceHeader = "X-Gateway-Rehearsal-Source"

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
)

// RehearsalSummary is the accumulated evidence gathered by rehearsal traffic.
// Counts are deduplicated by request: one rehearsal request contributes at
// most one terminal outcome here even when it matches several coverage
// categories.
type RehearsalSummary struct {
	RequiredSuccesses int64 `json:"required_successes"`
	InFlight          int64 `json:"in_flight"`
	Succeeded         int64 `json:"succeeded"`
	Failed            int64 `json:"failed"`
	Canceled          int64 `json:"canceled"`
	Aborted           int64 `json:"aborted"`
	Invalid           int64 `json:"invalid"`
	// PairCoverage breaks evidence down by how well the baseline and
	// candidate sides of the same business request explain each other.
	PairedTotal   int64 `json:"paired_total"`
	PairedValid   int64 `json:"paired_valid"`
	PairedAdverse int64 `json:"paired_adverse"`
	SingleSided   int64 `json:"single_sided"`
	Undecided     int64 `json:"undecided"`
}

// CategoryStats accumulates per-category rehearsal outcomes. Only
// successful terminal outcomes count as valid coverage; failed, canceled,
// aborted and invalid (unrouted) requests never do.
type CategoryStats struct {
	Required int64 `json:"required"`
	Valid    int64 `json:"valid"`
	Failed   int64 `json:"failed"`
	Canceled int64 `json:"canceled"`
	Aborted  int64 `json:"aborted"`
	Invalid  int64 `json:"invalid"`
}

// EvidenceEntry is one rehearsal request's terminal outcome, kept so the
// final release decision can be traced back to concrete samples instead of
// bare counters. Source identifies which rehearsal batch (initial run or a
// named supplement) produced the sample.
type EvidenceEntry struct {
	RequestID   string    `json:"request_id"`
	BusinessID  string    `json:"business_id,omitempty"`
	Side        Side      `json:"side,omitempty"`
	Attempt     int       `json:"attempt,omitempty"`
	Round       string    `json:"round,omitempty"`
	Source      string    `json:"source"`
	Method      string    `json:"method"`
	Path        string    `json:"path"`
	Categories  []string  `json:"categories"`
	Outcome     string    `json:"outcome"`
	Counted     bool      `json:"counted"`
	Adopted     bool      `json:"adopted,omitempty"`
	Late        bool      `json:"late,omitempty"`
	Conclusion  string    `json:"conclusion,omitempty"`
	At          time.Time `json:"at"`
}

// Readiness explains whether a candidate is releasable and why.
type Readiness struct {
	State   CandidateState   `json:"state"`
	Ready   bool             `json:"ready"`
	Reason  string           `json:"reason,omitempty"`
	Summary RehearsalSummary `json:"summary"`
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
	Coverage    []CategoryReport  `json:"coverage,omitempty"`
	// ReviewVersion identifies the category/evidence rule set under which
	// the adopted pairs were gathered; every release conclusion points at
	// the concrete request pairs it was built from.
	ReviewVersion int              `json:"review_version"`
	EvidencePolicy string          `json:"evidence_policy,omitempty"`
	AdoptedPairs  []AdoptedPairRef `json:"adopted_pairs,omitempty"`

	snapshot *config.Snapshot
}

// Candidate is a validated configuration that only serves rehearsal traffic
// until it is confirmed, discarded, superseded or promoted. Candidates are
// never deleted so the full rehearsal and release trail stays auditable.
type Candidate struct {
	ID            string                    `json:"id"`
	CreatedAt     time.Time                 `json:"created_at"`
	ResolvedAt    time.Time                 `json:"resolved_at,omitempty"`
	State         CandidateState            `json:"state"`
	Routes        []string                  `json:"routes"`
	Fingerprint   string                    `json:"fingerprint"`
	CoveragePlan  []config.CoverageCategory `json:"coverage_plan"`
	PlanConfirmed bool                      `json:"plan_confirmed"`
	Rehearsal     RehearsalSummary          `json:"rehearsal"`
	Coverage      map[string]*CategoryStats `json:"coverage"`
	Evidence      []EvidenceEntry           `json:"evidence"`
	Readiness     Readiness                 `json:"readiness"`
	// EvidencePolicy is the adoption policy fixed before rehearsal
	// starts; retries can never replace the first terminal result later.
	EvidencePolicy string `json:"evidence_policy"`
	// ReviewVersion identifies the category/evidence rule set; changing
	// either opens a new version and supersedes the prior candidate.
	ReviewVersion int `json:"review_version"`
	pairs         map[string]*BusinessPair
	rounds        []*CollectionRound
	currentRound  *CollectionRound
	roundSeq      int
	lateDecisions []LateDecision
	// InvalidatedBy names the newer revision that voided this candidate's
	// evidence, so old reports explain why their release basis expired.
	InvalidatedBy string `json:"invalidated_by,omitempty"`

	snapshot *config.Snapshot
	seen     map[string]struct{}
}

// RehearsalOutcome reports how one pinned rehearsal request ended.
type RehearsalOutcome struct {
	RequestID string
	Source    string
	Method    string
	Path      string
	Status    int
	Error     string
	Canceled  bool
	Aborted   bool
	BusinessID string
	Side       Side
	Attempt    int
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
	initial := store.appendReleaseLocked(KindInitial, "", snapshot, nil)
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
}

// Select picks the snapshot for one request: rehearsal traffic pinned to a
// staged candidate via RehearsalHeader uses the candidate snapshot,
// everything else uses the active release. An unknown or resolved candidate
// id falls back to the active release instead of being served as a candidate.
func (s *Store) Select(request *http.Request) Selection {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id := request.Header.Get(RehearsalHeader); id != "" {
		if candidate, ok := s.candidates[id]; ok && candidate.State == StateStaged && candidate.PlanConfirmed {
			candidate.Rehearsal.InFlight++
			return Selection{Snapshot: candidate.snapshot, ID: candidate.ID, CandidateID: candidate.ID}
		}
	}
	return Selection{Snapshot: s.active, ID: s.activeID}
}

// FinishRehearsal records the terminal outcome of one pinned rehearsal
// request. Evidence is attributed to the candidate revision the request was
// bound to even if the candidate was superseded or discarded while the
// request was in flight. Each request is counted once: a repeated
// RequestID is logged as evidence but never double-counted, so resumed or
// supplemented rehearsals cannot inflate coverage with duplicate samples.
func (s *Store) FinishRehearsal(candidateID string, outcome RehearsalOutcome) {
	outcome.Side = SideCandidate
	_ = s.RecordExecution(candidateID, outcome)
}

// legacyFinishLocked records a rehearsal outcome that carries no business
// correlation. Such entries stay visible in the evidence trail and legacy
// counters but cannot form a paired comparison and therefore never count as
// paired coverage.
func (c *Candidate) legacyFinishLocked(outcome RehearsalOutcome, kind string) {
	counted := true
	if outcome.RequestID != "" {
		if _, seen := c.seen[outcome.RequestID]; seen {
			counted = false
		} else {
			c.seen[outcome.RequestID] = struct{}{}
		}
	}
	source := outcome.Source
	if source == "" {
		source = "rehearsal"
	}
	categories := matchedCategories(c.snapshot, outcome.Method, outcome.Path)
	if counted {
		switch kind {
		case "succeeded":
			c.Rehearsal.Succeeded++
		case "failed":
			c.Rehearsal.Failed++
		case "canceled":
			c.Rehearsal.Canceled++
		case "aborted":
			c.Rehearsal.Aborted++
		case "invalid":
			c.Rehearsal.Invalid++
		}
		for _, id := range categories {
			stats := c.Coverage[id]
			if stats == nil {
				continue
			}
			switch kind {
			case "succeeded":
				stats.Valid++
			case "failed":
				stats.Failed++
			case "canceled":
				stats.Canceled++
			case "aborted":
				stats.Aborted++
			case "invalid":
				stats.Invalid++
			}
		}
	}
	if c.Evidence == nil {
		c.Evidence = []EvidenceEntry{}
	}
	c.Evidence = append(c.Evidence, EvidenceEntry{
		RequestID:  outcome.RequestID,
		Source:     source,
		Method:     outcome.Method,
		Path:       outcome.Path,
		Categories: categories,
		Outcome:    kind,
		Counted:    counted,
		At:         time.Now(),
	})
}

// classifyOutcome maps a terminal request state to its evidence bucket.
// Failed, canceled, aborted and invalid (unrouted) requests are recorded but
// never count as valid coverage.
func classifyOutcome(outcome RehearsalOutcome) string {
	switch {
	case outcome.Aborted:
		return "aborted"
	case outcome.Canceled:
		return "canceled"
	case outcome.Error == "no route matched":
		return "invalid"
	case outcome.Error != "" || outcome.Status == 0 || outcome.Status >= http.StatusInternalServerError:
		return "failed"
	default:
		return "succeeded"
	}
}

func matchedCategories(snapshot *config.Snapshot, method, path string) []string {
	matched := []string{}
	for _, category := range snapshot.CoverageCategories() {
		if category.Matches(method, path) {
			matched = append(matched, category.ID)
		}
	}
	return matched
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
	}
	s.candSeq++
	plan := snapshot.CoverageCategories()
	coverage := make(map[string]*CategoryStats, len(plan))
	for _, category := range plan {
		coverage[category.ID] = &CategoryStats{Required: int64(category.MinSamples)}
	}
	candidate := &Candidate{
		ID:            fmt.Sprintf("cand-%d", s.candSeq),
		CreatedAt:     time.Now(),
		State:         StateStaged,
		Routes:        snapshot.DescribeRoutes(),
		Fingerprint:   fingerprint,
		CoveragePlan:  plan,
		PlanConfirmed: len(plan) == 0,
		Coverage:      coverage,
		Evidence:      []EvidenceEntry{},
		Rehearsal: RehearsalSummary{
			RequiredSuccesses: int64(snapshot.MinSuccessfulRehearsals()),
		},
		snapshot: snapshot,
		seen:     make(map[string]struct{}),
	}
	s.candidates[candidate.ID] = candidate
	for _, existing := range s.candidates {
		if existing.State == StateSuperseded && existing.InvalidatedBy == "" && existing.ID != candidate.ID {
			existing.InvalidatedBy = candidate.ID
		}
	}
	return *candidate
}

// ConfirmPlan acknowledges the coverage plan of a staged candidate before
// rehearsal traffic starts. Requests pinned to a candidate whose plan is not
// confirmed yet are served by the active release instead, so no evidence can
// accumulate before the operator has reviewed the requirements.
func (s *Store) ConfirmPlan(candidateID string) (Candidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, ok := s.candidates[candidateID]
	if !ok {
		return Candidate{}, fmt.Errorf("%w %q", ErrUnknownCandidate, candidateID)
	}
	if candidate.State != StateStaged {
		return Candidate{}, fmt.Errorf("%w: %s", ErrNotStaged, candidate.State)
	}
	candidate.PlanConfirmed = true
	copied := *candidate
	copied.Readiness = candidate.readinessLocked()
	return copied, nil
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
		return Release{}, fmt.Errorf("candidate %q cannot be confirmed: %s", candidateID, readiness.Reason)
	}
	summary := candidate.Rehearsal
	candidate.State = StateConfirmed
	candidate.ResolvedAt = time.Now()
	release := s.appendReleaseLocked(KindPromote, candidate.ID, candidate.snapshot, &summary)
	release.Coverage = candidate.categoryReportsLocked(s.active, s.activeID)
	s.history[len(s.history)-1] = release
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
	release := s.appendReleaseLocked(KindRollback, target.ID, target.snapshot, nil)
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
		case !c.PlanConfirmed:
			readiness.Reason = "coverage plan not confirmed yet: review coverage_plan and confirm it before rehearsal"
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
			readiness.State = StateReady
			readiness.Ready = true
		}
		if readiness.Ready {
			if reason := c.coverageShortfallLocked(); reason != "" {
				readiness.State = StateInsufficient
				readiness.Ready = false
				readiness.Reason = reason
			}
		}
	default:
		readiness.Reason = "candidate is " + string(c.State)
		if c.State == StateSuperseded && c.InvalidatedBy != "" {
			readiness.Reason = fmt.Sprintf("candidate superseded by %s: rehearsal evidence is no longer a valid release basis", c.InvalidatedBy)
		}
	}
	return readiness
}

// coverageShortfallLocked explains which coverage categories miss their
// requirements, or returns "" when every category is satisfied. High-risk
// categories must pass on their own evidence: their failures can never be
// averaged away by the overall success count.
func (c *Candidate) coverageShortfallLocked() string {
	reasons := []string{}
	for _, category := range c.CoveragePlan {
		stats := c.Coverage[category.ID]
		if stats == nil {
			stats = &CategoryStats{Required: int64(category.MinSamples)}
		}
		if stats.Valid < stats.Required {
			reasons = append(reasons, fmt.Sprintf("category %q has %d/%d valid sample(s) (%d missing)",
				category.ID, stats.Valid, stats.Required, stats.Required-stats.Valid))
		}
		if category.HighRisk {
			bad := stats.Failed + stats.Canceled + stats.Aborted
			if bad > 0 {
				reasons = append(reasons, fmt.Sprintf("high-risk category %q has %d failed/canceled/aborted sample(s)", category.ID, bad))
			}
		}
	}
	return strings.Join(reasons, "; ")
}

func (s *Store) appendReleaseLocked(kind Kind, sourceID string, snapshot *config.Snapshot, summary *RehearsalSummary) Release {
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
	s.history = append(s.history, release)
	return release
}

// Fingerprint hashes the routing and limiting content of a snapshot:

// upstream targets/timeouts, route prefixes, versions and retry limits, and
// the rehearsal coverage plan. Listener, admin and drain settings are
// excluded because changing them does not invalidate routing rehearsal
// evidence; changing category rules or coverage requirements does.
func Fingerprint(snapshot *config.Snapshot) string {
	payload := struct {
		Upstreams []config.Upstream         `json:"upstreams"`
		Routes    []config.Route            `json:"routes"`
		Coverage  []config.CoverageCategory `json:"coverage"`
	}{
		Upstreams: snapshot.Upstreams(),
		Routes:    snapshot.Routes(),
		Coverage:  snapshot.CoverageCategories(),
	}
	data, _ := json.Marshal(payload)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:12])
}
