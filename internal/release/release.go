package release

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

// GateStatus reports how far a candidate has progressed through its limited
// rehearsal gate. Rehearsal is a real release condition, not a counter.
type GateStatus string

const (
	// GateNotRehearsed means no rehearsal request has been run at all.
	GateNotRehearsed GateStatus = "not_rehearsed"
	// GateInProgress means rehearsal traffic ran but at least one rehearsal
	// request has not reached a terminal state yet.
	GateInProgress GateStatus = "in_progress"
	// GateIncompleteResults means every rehearsal request finished, but the
	// outcomes did not satisfy the release policy.
	GateIncompleteResults GateStatus = "incomplete_results"
	// GateReady means the rehearsal outcomes satisfy the release policy and
	// the candidate can be confirmed.
	GateReady GateStatus = "ready"
)

// RehearsalOutcome records one rehearsal request's terminal result against a
// specific candidate revision. A success means the upstream interaction
// completed with a 2xx/3xx status without cancellation or abort.
type RehearsalOutcome struct {
	RecordID string    `json:"record_id"`
	At       time.Time `json:"at"`
	Method   string    `json:"method"`
	Path     string    `json:"path"`
	Status   int       `json:"status"`
	Attempts int       `json:"attempts"`
	Success  bool      `json:"success"`
	Canceled bool      `json:"canceled"`
	Aborted  bool      `json:"aborted"`
	Error    string    `json:"error,omitempty"`
	Revision int       `json:"revision"`
}

// RehearsalSummary is embedded in audit records so the exact evidence that
// justified (or belonged to) a revision stays traceable.
type RehearsalSummary struct {
	Revision  int `json:"revision"`
	Total     int `json:"total"`
	InFlight  int `json:"in_flight"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
	Required  int `json:"required"`
}

// Release is an immutable audit record of one activation. Rollbacks append a
// new record instead of rewriting history.
type Release struct {
	ID        string            `json:"id"`
	Kind      Kind              `json:"kind"`
	CreatedAt time.Time         `json:"created_at"`
	SourceID  string            `json:"source_id,omitempty"`
	Routes    []string          `json:"routes"`
	Revision  int               `json:"revision,omitempty"`
	Rehearsal *RehearsalSummary `json:"rehearsal,omitempty"`

	snapshot *config.Snapshot
}

// revisionEvidence is the rehearsal state bound to one candidate revision.
// Changing routes or limits changes the fingerprint and starts fresh
// evidence; old revision evidence is retained for traceability.
type revisionEvidence struct {
	Revision    int
	Fingerprint string
	InFlight    int
	Outcomes    []RehearsalOutcome
}

func (e *revisionEvidence) summary(required int) RehearsalSummary {
	succeeded, failed := 0, 0
	for _, outcome := range e.Outcomes {
		if outcome.Success {
			succeeded++
		} else {
			failed++
		}
	}
	return RehearsalSummary{
		Revision:  e.Revision,
		Total:     len(e.Outcomes),
		InFlight:  e.InFlight,
		Succeeded: succeeded,
		Failed:    failed,
		Required:  required,
	}
}

// Candidate is a validated configuration that only serves rehearsal traffic
// until it is confirmed.
type Candidate struct {
	ID                  string             `json:"id"`
	CreatedAt           time.Time          `json:"created_at"`
	UpdatedAt           time.Time          `json:"updated_at"`
	Revision            int                `json:"revision"`
	Routes              []string           `json:"routes"`
	Required            int                `json:"required_successful_rehearsals"`
	Gate                GateStatus         `json:"gate_status"`
	Rehearsal           RehearsalSummary   `json:"rehearsal"`
	EvidenceInvalidated bool               `json:"evidence_invalidated,omitempty"`
	Invalidations       []RehearsalSummary `json:"invalidated_evidence,omitempty"`
	Outcomes            []RehearsalOutcome `json:"rehearsal_outcomes,omitempty"`

	snapshot *config.Snapshot
	evidence *revisionEvidence
}

// CandidateRecord is the archived form of a candidate kept after promotion,
// discard, or revision, so historical rehearsal evidence is never lost.
type CandidateRecord struct {
	ID         string             `json:"id"`
	Status     string             `json:"status"` // promoted, discarded, superseded
	CreatedAt  time.Time          `json:"created_at"`
	ResolvedAt time.Time          `json:"resolved_at"`
	Revision   int                `json:"revision"`
	Routes     []string           `json:"routes"`
	Gate       GateStatus         `json:"gate_status"`
	Rehearsal  RehearsalSummary   `json:"rehearsal"`
	Revisions  []RehearsalSummary `json:"revisions,omitempty"`
	ReleaseID  string             `json:"release_id,omitempty"`
}

// Binding identifies the snapshot and revision a request is pinned to for
// its whole lifetime.
type Binding struct {
	Snapshot    *config.Snapshot
	ID          string
	Revision    int
	IsCandidate bool
}

// RequestDiff compares active and candidate decisions for the same request.
type RequestDiff struct {
	Method           string              `json:"method"`
	Path             string              `json:"path"`
	ActiveMatched    bool                `json:"active_matched"`
	CandidateMatched bool                `json:"candidate_matched"`
	ActiveRoute      *config.RequestView `json:"active_route,omitempty"`
	CandidateRoute   *config.RequestView `json:"candidate_route,omitempty"`
	SameDecision     bool                `json:"same_decision"`
	ChangedFields    []string            `json:"changed_fields,omitempty"`
}

// SnapshotDiff compares the full configuration of active and candidate.
type SnapshotDiff struct {
	CandidateID       string      `json:"candidate_id"`
	Revision          int         `json:"revision"`
	Same              bool        `json:"same"`
	Active            config.View `json:"active"`
	Candidate         config.View `json:"candidate"`
	ChangedUpstreams  []string    `json:"changed_upstreams,omitempty"`
	ChangedRoutes     []string    `json:"changed_routes,omitempty"`
	RouteOrderChanged bool        `json:"route_order_changed,omitempty"`
}

type Store struct {
	mu         sync.Mutex
	active     *config.Snapshot
	activeID   string
	seq        int
	candSeq    int
	candidates map[string]*Candidate
	// evidenceByRevision retains rehearsal evidence even after a candidate is
	// revised, promoted, or discarded, so in-flight reports and audit views
	// keep working for every revision that ever served traffic.
	evidenceByKey map[string]*revisionEvidence
	archive       map[string]*CandidateRecord
	archiveOrder  []string
	history       []Release
}

func NewStore(snapshot *config.Snapshot) *Store {
	store := &Store{
		candidates:    make(map[string]*Candidate),
		evidenceByKey: make(map[string]*revisionEvidence),
		archive:       make(map[string]*CandidateRecord),
	}
	store.active = snapshot
	initial := store.appendReleaseLocked(KindInitial, "", snapshot, 0, nil)
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

// Select pins the snapshot for one request at entry time: rehearsal traffic
// pinned to a candidate via RehearsalHeader uses that candidate revision,
// everything else uses the active release. The binding never changes for the
// request's lifetime, so confirmation, rollback, discard or shutdown cannot
// switch an in-flight request mid-flight.
func (s *Store) Select(request *http.Request) Binding {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id := request.Header.Get(RehearsalHeader); id != "" {
		if candidate, ok := s.candidates[id]; ok {
			candidate.evidence.InFlight++
			candidate.refreshGateLocked()
			return Binding{Snapshot: candidate.snapshot, ID: candidate.ID, Revision: candidate.Revision, IsCandidate: true}
		}
	}
	return Binding{Snapshot: s.active, ID: s.activeID}
}

// FinishRehearsal records the terminal outcome of a rehearsal request. It
// works for live, revised, promoted and discarded candidates because
// evidence is keyed by candidate id and revision.
func (s *Store) FinishRehearsal(candidateID string, revision int, outcome RehearsalOutcome) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := evidenceKey(candidateID, revision)
	evidence := s.evidenceByKey[key]
	if evidence == nil {
		return
	}
	if evidence.InFlight > 0 {
		evidence.InFlight--
	}
	outcome.Revision = revision
	evidence.Outcomes = append(evidence.Outcomes, outcome)
	if candidate, ok := s.candidates[candidateID]; ok && candidate.Revision == revision {
		candidate.refreshGateLocked()
	}
}

// Stage registers an already validated snapshot as a new candidate. It never
// touches the active release and starts with empty rehearsal evidence.
func (s *Store) Stage(snapshot *config.Snapshot) Candidate {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.candSeq++
	revision := 1
	required := snapshot.MinSuccessfulRehearsals
	if required <= 0 {
		required = 1
	}
	evidence := &revisionEvidence{Revision: revision, Fingerprint: Fingerprint(snapshot)}
	candidate := &Candidate{
		ID:        fmt.Sprintf("cand-%d", s.candSeq),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Revision:  revision,
		Routes:    snapshot.DescribeRoutes(),
		Required:  required,
		Gate:      GateNotRehearsed,
		snapshot:  snapshot,
		evidence:  evidence,
	}
	s.candidates[candidate.ID] = candidate
	s.evidenceByKey[evidenceKey(candidate.ID, revision)] = evidence
	s.archive[candidate.ID] = &CandidateRecord{
		ID: candidate.ID, Status: "open", CreatedAt: candidate.CreatedAt,
		Revision: revision, Routes: candidate.Routes, Gate: GateNotRehearsed,
	}
	s.archiveOrder = append(s.archiveOrder, candidate.ID)
	return *candidate
}

// Revise replaces a candidate's snapshot. If the routing or limiting
// configuration actually changed, the fingerprint changes and all rehearsal
// success from the previous revision is invalidated: it cannot justify the
// new revision. Restaging an identical snapshot is a no-op that keeps the
// existing evidence.
func (s *Store) Revise(candidateID string, snapshot *config.Snapshot) (Candidate, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, ok := s.candidates[candidateID]
	if !ok {
		return Candidate{}, false, fmt.Errorf("unknown candidate %q", candidateID)
	}
	fingerprint := Fingerprint(snapshot)
	if fingerprint == candidate.evidence.Fingerprint {
		return *candidate, false, nil
	}
	// In-flight requests from the old revision keep their bound snapshot and
	// report back to the old revision's evidence.
	invalidated := candidate.evidence.summary(candidate.Required)
	candidate.Invalidations = append(candidate.Invalidations, invalidated)
	candidate.EvidenceInvalidated = true
	candidate.Revision++
	revision := candidate.Revision
	required := snapshot.MinSuccessfulRehearsals
	if required <= 0 {
		required = 1
	}
	evidence := &revisionEvidence{Revision: revision, Fingerprint: fingerprint}
	candidate.evidence = evidence
	candidate.snapshot = snapshot
	candidate.Routes = snapshot.DescribeRoutes()
	candidate.Required = required
	candidate.UpdatedAt = time.Now()
	candidate.refreshGateLocked()
	s.evidenceByKey[evidenceKey(candidateID, revision)] = evidence
	if record, ok := s.archive[candidateID]; ok {
		record.Revision = revision
		record.Routes = snapshot.DescribeRoutes()
		record.Gate = candidate.Gate
		record.Rehearsal = candidate.Rehearsal
		record.Revisions = append([]RehearsalSummary(nil), candidate.Invalidations...)
	}
	return *candidate, true, nil
}

// Confirm promotes a candidate to the active release and records the switch
// in the audit history. Confirmation is refused unless the candidate's
// limited rehearsal has actually been completed and met the release policy.
func (s *Store) Confirm(candidateID string) (Release, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, ok := s.candidates[candidateID]
	if !ok {
		return Release{}, ErrUnknownCandidate(candidateID)
	}
	if status := candidate.gateStatusLocked(); status != GateReady {
		return Release{}, &GateError{CandidateID: candidateID, Status: status, Summary: candidate.evidence.summary(candidate.Required)}
	}
	rehearsal := candidate.evidence.summary(candidate.Required)
	release := s.appendReleaseLocked(KindPromote, candidate.ID, candidate.snapshot, candidate.Revision, &rehearsal)
	s.active = candidate.snapshot
	s.activeID = release.ID
	delete(s.candidates, candidateID)
	s.resolveArchiveLocked(candidate, "promoted", release.ID)
	return release, nil
}

// Discard abandons a candidate without changing the active release. The
// candidate and its rehearsal history remain in the audit archive.
func (s *Store) Discard(candidateID string) (CandidateRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, ok := s.candidates[candidateID]
	if !ok {
		return CandidateRecord{}, ErrUnknownCandidate(candidateID)
	}
	if candidate.evidence.InFlight > 0 {
		return CandidateRecord{}, fmt.Errorf("candidate %q has %d rehearsal request(s) still in flight", candidateID, candidate.evidence.InFlight)
	}
	delete(s.candidates, candidateID)
	record := s.resolveArchiveLocked(candidate, "discarded", "")
	return record, nil
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
	release := s.appendReleaseLocked(KindRollback, target.ID, target.snapshot, 0, nil)
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
		view := *candidate
		view.Outcomes = append([]RehearsalOutcome(nil), candidate.evidence.Outcomes...)
		result = append(result, view)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

// ArchivedCandidates returns the full lifecycle records of every candidate,
// including superseded revisions, discards and promotions.
func (s *Store) ArchivedCandidates() []CandidateRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]CandidateRecord, 0, len(s.archiveOrder))
	for _, id := range s.archiveOrder {
		result = append(result, *s.archive[id])
	}
	return result
}

// DiffSnapshot compares the active release and a candidate configuration so
// users can see what would change before publishing or abandoning it.
func (s *Store) DiffSnapshot(candidateID string) (SnapshotDiff, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, ok := s.candidates[candidateID]
	if !ok {
		return SnapshotDiff{}, ErrUnknownCandidate(candidateID)
	}
	activeView := s.active.View()
	candidateView := candidate.snapshot.View()
	diff := SnapshotDiff{
		CandidateID: candidate.ID,
		Revision:    candidate.Revision,
		Active:      activeView,
		Candidate:   candidateView,
	}
	activeUpstreams := make(map[string]config.UpstreamView, len(activeView.Upstreams))
	for _, upstream := range activeView.Upstreams {
		activeUpstreams[upstream.ID] = upstream
	}
	candidateUpstreams := make(map[string]config.UpstreamView, len(candidateView.Upstreams))
	for _, upstream := range candidateView.Upstreams {
		candidateUpstreams[upstream.ID] = upstream
		if old, exists := activeUpstreams[upstream.ID]; !exists || old != upstream {
			diff.ChangedUpstreams = append(diff.ChangedUpstreams, upstream.ID)
		}
	}
	for id := range activeUpstreams {
		if _, exists := candidateUpstreams[id]; !exists {
			diff.ChangedUpstreams = append(diff.ChangedUpstreams, id)
		}
	}
	activeRoutes := make(map[string]config.RouteView, len(activeView.Routes))
	for _, route := range activeView.Routes {
		activeRoutes[route.ID] = route
	}
	candidateRoutes := make(map[string]config.RouteView, len(candidateView.Routes))
	for _, route := range candidateView.Routes {
		candidateRoutes[route.ID] = route
		if old, exists := activeRoutes[route.ID]; !exists || !routesEqual(old, route) {
			diff.ChangedRoutes = append(diff.ChangedRoutes, route.ID)
		}
	}
	for id := range activeRoutes {
		if _, exists := candidateRoutes[id]; !exists {
			diff.ChangedRoutes = append(diff.ChangedRoutes, id)
		}
	}
	if len(activeView.Routes) == len(candidateView.Routes) {
		for i := range activeView.Routes {
			if activeView.Routes[i].ID != candidateView.Routes[i].ID {
				diff.RouteOrderChanged = true
				break
			}
		}
	}
	sort.Strings(diff.ChangedUpstreams)
	sort.Strings(diff.ChangedRoutes)
	diff.Same = len(diff.ChangedUpstreams) == 0 && len(diff.ChangedRoutes) == 0 && !diff.RouteOrderChanged &&
		activeView.Listen == candidateView.Listen && activeView.AdminPath == candidateView.AdminPath &&
		activeView.DrainTimeout == candidateView.DrainTimeout
	return diff, nil
}

// DiffRequest runs the same request through both the active release and the
// candidate, so users can compare routing and limiting decisions side by
// side before deciding to publish or abandon the candidate.
func (s *Store) DiffRequest(candidateID, method, requestPath string) (RequestDiff, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, ok := s.candidates[candidateID]
	if !ok {
		return RequestDiff{}, ErrUnknownCandidate(candidateID)
	}
	if method == "" {
		method = http.MethodGet
	}
	diff := RequestDiff{Method: method, Path: requestPath}
	if activeRoute, matched := s.active.MatchView(method, requestPath); matched {
		diff.ActiveMatched = true
		diff.ActiveRoute = &activeRoute
	}
	if candidateRoute, matched := candidate.snapshot.MatchView(method, requestPath); matched {
		diff.CandidateMatched = true
		diff.CandidateRoute = &candidateRoute
	}
	switch {
	case !diff.ActiveMatched && !diff.CandidateMatched:
		diff.SameDecision = true
	case diff.ActiveMatched && diff.CandidateMatched:
		diff.SameDecision = decisionsEqual(*diff.ActiveRoute, *diff.CandidateRoute)
		if !diff.SameDecision {
			diff.ChangedFields = decisionFieldChanges(*diff.ActiveRoute, *diff.CandidateRoute)
		}
	default:
		diff.ChangedFields = []string{"matched"}
	}
	return diff, nil
}

func (s *Store) appendReleaseLocked(kind Kind, sourceID string, snapshot *config.Snapshot, revision int, rehearsal *RehearsalSummary) Release {
	s.seq++
	release := Release{
		ID:        fmt.Sprintf("rel-%d", s.seq),
		Kind:      kind,
		CreatedAt: time.Now(),
		SourceID:  sourceID,
		Routes:    snapshot.DescribeRoutes(),
		Revision:  revision,
		Rehearsal: rehearsal,
		snapshot:  snapshot,
	}
	s.history = append(s.history, release)
	return release
}

func (s *Store) resolveArchiveLocked(candidate *Candidate, status, releaseID string) CandidateRecord {
	record, ok := s.archive[candidate.ID]
	if !ok {
		record = &CandidateRecord{ID: candidate.ID, CreatedAt: candidate.CreatedAt}
		s.archive[candidate.ID] = record
		s.archiveOrder = append(s.archiveOrder, candidate.ID)
	}
	record.Status = status
	record.ResolvedAt = time.Now()
	record.Revision = candidate.Revision
	record.Routes = candidate.Routes
	record.Gate = candidate.gateStatusLocked()
	record.Rehearsal = candidate.evidence.summary(candidate.Required)
	record.Revisions = append([]RehearsalSummary(nil), candidate.Invalidations...)
	record.ReleaseID = releaseID
	return *record
}

func (c *Candidate) gateStatusLocked() GateStatus {
	summary := c.evidence.summary(c.Required)
	switch {
	case summary.Total == 0 && summary.InFlight == 0:
		return GateNotRehearsed
	case summary.InFlight > 0:
		return GateInProgress
	case summary.Succeeded < c.Required:
		return GateIncompleteResults
	default:
		return GateReady
	}
}

func (c *Candidate) refreshGateLocked() {
	c.Gate = c.gateStatusLocked()
	c.Rehearsal = c.evidence.summary(c.Required)
}

// Fingerprint hashes everything that affects routing or request limiting: the
// listen/admin settings, upstreams, route order, matching, stripping and
// retry policy. A changed fingerprint invalidates prior rehearsal evidence.
func Fingerprint(snapshot *config.Snapshot) string {
	data, _ := json.Marshal(snapshot.View())
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:12])
}

func evidenceKey(candidateID string, revision int) string {
	return fmt.Sprintf("%s#%d", candidateID, revision)
}

// ErrUnknownCandidate reports an unknown candidate id as a distinct error.
func ErrUnknownCandidate(id string) error {
	return fmt.Errorf("unknown candidate %q", id)
}

// GateError explains why a candidate cannot be confirmed yet.
type GateError struct {
	CandidateID string
	Status      GateStatus
	Summary     RehearsalSummary
}

func (e *GateError) Error() string {
	switch e.Status {
	case GateNotRehearsed:
		return fmt.Sprintf("candidate %q has not been rehearsed; run limited rehearsal traffic before confirming", e.CandidateID)
	case GateInProgress:
		return fmt.Sprintf("candidate %q rehearsal is incomplete: %d request(s) still in flight", e.CandidateID, e.Summary.InFlight)
	case GateIncompleteResults:
		return fmt.Sprintf("candidate %q rehearsal did not meet requirements: %d/%d successful, %d failed", e.CandidateID, e.Summary.Succeeded, e.Summary.Required, e.Summary.Failed)
	default:
		return fmt.Sprintf("candidate %q is not ready (status %s)", e.CandidateID, e.Status)
	}
}

func routesEqual(left, right config.RouteView) bool {
	leftRetry, _ := json.Marshal(left.Retry)
	rightRetry, _ := json.Marshal(right.Retry)
	return left.ID == right.ID && left.Version == right.Version && left.Prefix == right.Prefix &&
		left.UpstreamID == right.UpstreamID && left.BaseURL == right.BaseURL &&
		left.StripPrefix == right.StripPrefix && string(leftRetry) == string(rightRetry)
}

// decisionsEqual compares the complete per-request decision including the
// forwarded path and the retry limit that applies to the request method.
func decisionsEqual(left, right config.RequestView) bool {
	return routesEqual(left.RouteView, right.RouteView) &&
		left.ForwardPath == right.ForwardPath &&
		left.RetryAllowed == right.RetryAllowed &&
		left.MaxAttempts == right.MaxAttempts
}

func decisionFieldChanges(left, right config.RequestView) []string {
	changes := routeFieldChanges(left.RouteView, right.RouteView)
	if left.ForwardPath != right.ForwardPath {
		changes = append(changes, "forward_path")
	}
	if left.RetryAllowed != right.RetryAllowed {
		changes = append(changes, "retry_allowed")
	}
	if left.MaxAttempts != right.MaxAttempts {
		changes = append(changes, "max_attempts")
	}
	return changes
}

func routeFieldChanges(left, right config.RouteView) []string {
	changes := make([]string, 0, 8)
	if left.ID != right.ID {
		changes = append(changes, "id")
	}
	if left.Version != right.Version {
		changes = append(changes, "version")
	}
	if left.Prefix != right.Prefix {
		changes = append(changes, "prefix")
	}
	if left.UpstreamID != right.UpstreamID {
		changes = append(changes, "upstream_id")
	}
	if left.BaseURL != right.BaseURL {
		changes = append(changes, "upstream_base_url")
	}
	if left.StripPrefix != right.StripPrefix {
		changes = append(changes, "strip_prefix")
	}
	leftRetry, _ := json.Marshal(left.Retry)
	rightRetry, _ := json.Marshal(right.Retry)
	if string(leftRetry) != string(rightRetry) {
		changes = append(changes, "retry_policy")
	}
	return changes
}
