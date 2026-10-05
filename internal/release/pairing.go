package release

import (
	"fmt"
	"sort"
	"time"
)

type Side string

const (
	SideBaseline  Side = "baseline"
	SideCandidate Side = "candidate"
)

const (
	BusinessIDHeader      = "X-Gateway-Business-ID"
	ExecutionSideHeader   = "X-Gateway-Execution-Side"
	CollectionRoundHeader = "X-Gateway-Collection-Round"
)

// Execution is the immutable terminal result of one side of a business
// request. Only the first terminal execution of a side is adopted.
type Execution struct {
	RequestID  string
	Source     string
	Method     string
	Path       string
	Attempt    int
	RoundID    string
	Outcome    string
	Status     int
	Error      string
	Categories []string
	At         time.Time
}

// BusinessPair pairs the baseline and candidate executions identified by the
// caller-provided business request id. The pair is the smallest evidence unit
// that can support a release conclusion.
type BusinessPair struct {
	BusinessID string
	RoundID    string
	Baseline   *Execution
	Candidate  *Execution
	Decided    bool
	Conclusion string
	Categories []string
}

// CollectionRound fixes the set of evidence being reviewed. Executions that
// arrive after a round is closed are retained, but receive a late decision
// and cannot alter the round's adopted evidence.
type CollectionRound struct {
	ID        string    `json:"id"`
	StartedAt time.Time `json:"started_at"`
	ClosedAt  time.Time `json:"closed_at,omitempty"`
	Open      bool      `json:"open"`
}

// LateDecision explains why an execution arriving after a round was closed
// was excluded from adoption.
type LateDecision struct {
	BusinessID string    `json:"business_id"`
	Side       Side      `json:"side"`
	RequestID  string    `json:"request_id"`
	RoundID    string    `json:"round_id"`
	Reason     string    `json:"reason"`
	At         time.Time `json:"at"`
}

// AdoptedPairRef is the audit pointer stored on a release. It identifies the
// concrete baseline/candidate request ids behind every counted conclusion.
type AdoptedPairRef struct {
	BusinessID         string   `json:"business_id"`
	RoundID            string   `json:"round_id,omitempty"`
	Conclusion         string   `json:"conclusion"`
	BaselineRequestID  string   `json:"baseline_request_id,omitempty"`
	CandidateRequestID string   `json:"candidate_request_id,omitempty"`
	BaselineAttempt    int      `json:"baseline_attempt,omitempty"`
	CandidateAttempt   int      `json:"candidate_attempt,omitempty"`
	Method             string   `json:"method,omitempty"`
	Path               string   `json:"path,omitempty"`
	Categories         []string `json:"categories,omitempty"`
}

// StartCollectionRound opens a new evidence collection round for a candidate.
func (s *Store) StartCollectionRound(candidateID string) (CollectionRound, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, ok := s.candidates[candidateID]
	if !ok {
		return CollectionRound{}, fmt.Errorf("%w %q", ErrUnknownCandidate, candidateID)
	}
	if candidate.State != StateStaged {
		return CollectionRound{}, fmt.Errorf("%w: %s", ErrNotStaged, candidate.State)
	}
	if candidate.currentRound != nil && candidate.currentRound.Open {
		return CollectionRound{}, fmt.Errorf("collection round %s is already open", candidate.currentRound.ID)
	}
	candidate.roundSeq++
	round := &CollectionRound{
		ID:        fmt.Sprintf("round-%d", candidate.roundSeq),
		StartedAt: time.Now(),
		Open:      true,
	}
	candidate.rounds = append(candidate.rounds, round)
	candidate.currentRound = round
	return *round, nil
}

// CloseCollectionRound closes the current open round and freezes its adopted
// evidence. Incomplete pairs in that round become undecided.
func (s *Store) CloseCollectionRound(candidateID string) (CollectionRound, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, ok := s.candidates[candidateID]
	if !ok {
		return CollectionRound{}, fmt.Errorf("%w %q", ErrUnknownCandidate, candidateID)
	}
	if candidate.currentRound == nil || !candidate.currentRound.Open {
		return CollectionRound{}, fmt.Errorf("candidate %q has no open collection round", candidateID)
	}
	candidate.closeCurrentRoundLocked()
	return *candidate.currentRound, nil
}

// RecordExecution records one terminal baseline or candidate execution. It
// implements first-terminal adoption, request pairing and late-round policy.
func (s *Store) RecordExecution(candidateID string, outcome RehearsalOutcome) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, ok := s.candidates[candidateID]
	if !ok {
		return fmt.Errorf("%w %q", ErrUnknownCandidate, candidateID)
	}
	if outcome.Side == "" {
		outcome.Side = SideCandidate
	}
	if outcome.Side != SideBaseline && outcome.Side != SideCandidate {
		return fmt.Errorf("unknown execution side %q", outcome.Side)
	}
	if outcome.Side == SideCandidate && candidate.Rehearsal.InFlight > 0 {
		candidate.Rehearsal.InFlight--
	}
	if outcome.BusinessID == "" {
		kind := classifyOutcome(outcome)
		candidate.legacyFinishLocked(outcome, kind)
		return nil
	}

	kind := classifyOutcome(outcome)
	source := outcome.Source
	if source == "" {
		source = "rehearsal"
	}
	if outcome.Attempt == 0 {
		outcome.Attempt = 1
	}
	roundID := outcome.Round
	if roundID == "" && candidate.currentRound != nil {
		roundID = candidate.currentRound.ID
	}
	now := time.Now()
	late, lateReason := candidate.latenessLocked(roundID)
	pair := candidate.pairs[outcome.BusinessID]
	if pair == nil {
		pair = &BusinessPair{BusinessID: outcome.BusinessID, RoundID: roundID}
		candidate.pairs[outcome.BusinessID] = pair
	}
	if pair.RoundID == "" {
		pair.RoundID = roundID
	}
	if roundID == "" {
		roundID = pair.RoundID
		late, lateReason = candidate.latenessLocked(roundID)
	}
	existing := pair.side(outcome.Side)
	adopted := !late && existing == nil
	categories := matchedCategories(candidate.snapshot, outcome.Method, outcome.Path)
	execution := &Execution{
		RequestID:  outcome.RequestID,
		Source:     source,
		Method:     outcome.Method,
		Path:       outcome.Path,
		Attempt:    outcome.Attempt,
		RoundID:    roundID,
		Outcome:    kind,
		Status:     outcome.Status,
		Error:      outcome.Error,
		Categories: categories,
		At:         now,
	}
	conclusion := "retry_ignored"
	if late {
		conclusion = "late_ignored"
		candidate.lateDecisions = append(candidate.lateDecisions, LateDecision{
			BusinessID: outcome.BusinessID,
			Side:       outcome.Side,
			RequestID:  outcome.RequestID,
			RoundID:    roundID,
			Reason:     lateReason,
			At:         now,
		})
	} else if existing == nil {
		pair.setSide(outcome.Side, execution)
		pair.Categories = unionStrings(pair.Categories, categories)
		conclusion = "adopted"
		if pair.Baseline == nil || pair.Candidate == nil {
			candidate.Rehearsal.SingleSided++
		} else {
			candidate.Rehearsal.SingleSided--
			candidate.Rehearsal.PairedTotal++
			if pair.Baseline.Outcome == "succeeded" && pair.Candidate.Outcome == "succeeded" {
				pair.Conclusion = "valid"
				pair.Decided = true
				candidate.Rehearsal.PairedValid++
				candidate.Rehearsal.Succeeded++
				candidate.addCategoryOutcomeLocked(categories, "succeeded")
			} else {
				pair.Conclusion = "adverse"
				pair.Decided = true
				candidate.Rehearsal.PairedAdverse++
				candidate.Rehearsal.Failed++
				candidate.addCategoryOutcomeLocked(categories, pairFailureKind(pair))
			}
			conclusion = pair.Conclusion
		}
	}
	candidate.Evidence = append(candidate.Evidence, EvidenceEntry{
		RequestID:  outcome.RequestID,
		BusinessID: outcome.BusinessID,
		Side:       outcome.Side,
		Attempt:    outcome.Attempt,
		Round:      roundID,
		Source:     source,
		Method:     outcome.Method,
		Path:       outcome.Path,
		Categories: categories,
		Outcome:    kind,
		Counted:    adopted,
		Adopted:    adopted,
		Late:       late,
		Conclusion: conclusion,
		At:         now,
	})
	return nil
}

func (p *BusinessPair) side(side Side) *Execution {
	if side == SideBaseline {
		return p.Baseline
	}
	return p.Candidate
}

func (p *BusinessPair) setSide(side Side, execution *Execution) {
	if side == SideBaseline {
		p.Baseline = execution
	} else {
		p.Candidate = execution
	}
}

// latenessLocked reports whether an execution arriving now can no longer be
// adopted, and why. Once the candidate is resolved (confirmed, discarded or
// superseded) its adopted evidence is frozen: every further execution is a
// queryable supplemental fact that can never change the published
// success/failure/category conclusions. The same holds for executions that
// declare an already closed collection round.
func (c *Candidate) latenessLocked(roundID string) (bool, string) {
	if c.State != StateStaged {
		return true, fmt.Sprintf("candidate %s is %s: adopted evidence is frozen and cannot alter the published conclusions", c.ID, c.State)
	}
	if roundID == "" {
		return false, ""
	}
	for _, round := range c.rounds {
		if round.ID == roundID {
			if !round.Open {
				return true, fmt.Sprintf("collection round %s is closed", roundID)
			}
			return false, ""
		}
	}
	return false, ""
}

func (c *Candidate) closeCurrentRoundLocked() {
	round := c.currentRound
	round.Open = false
	round.ClosedAt = time.Now()
	for _, pair := range c.pairs {
		if pair.RoundID != round.ID || pair.Decided {
			continue
		}
		if pair.Baseline != nil || pair.Candidate != nil {
			pair.Decided = true
			pair.Conclusion = "undecided"
			if c.Rehearsal.SingleSided > 0 {
				c.Rehearsal.SingleSided--
			}
			c.Rehearsal.Undecided++
			for i := range c.Evidence {
				if c.Evidence[i].BusinessID == pair.BusinessID && c.Evidence[i].Round == round.ID && c.Evidence[i].Conclusion == "adopted" {
					c.Evidence[i].Conclusion = "undecided"
				}
			}
		}
	}
}

func (c *Candidate) addCategoryOutcomeLocked(categories []string, kind string) {
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

func pairFailureKind(pair *BusinessPair) string {
	if pair.Candidate != nil && pair.Candidate.Outcome != "succeeded" {
		return pair.Candidate.Outcome
	}
	if pair.Baseline != nil && pair.Baseline.Outcome != "succeeded" {
		return pair.Baseline.Outcome
	}
	return "failed"
}

func unionStrings(existing, additional []string) []string {
	for _, value := range additional {
		if !containsString(existing, value) {
			existing = append(existing, value)
		}
	}
	return existing
}

func (c *Candidate) adoptedPairRefsLocked() []AdoptedPairRef {
	refs := []AdoptedPairRef{}
	for _, pair := range c.pairs {
		if !pair.Decided {
			continue
		}
		refs = append(refs, pair.refLocked())
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].BusinessID < refs[j].BusinessID })
	return refs
}

func (c *Candidate) pairRefsLocked() []AdoptedPairRef {
	refs := []AdoptedPairRef{}
	for _, pair := range c.pairs {
		if !pair.Decided {
			continue
		}
		refs = append(refs, pair.refLocked())
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].BusinessID < refs[j].BusinessID })
	return refs
}

func (p *BusinessPair) refLocked() AdoptedPairRef {
	ref := AdoptedPairRef{
		BusinessID: p.BusinessID,
		RoundID:    p.RoundID,
		Conclusion: p.Conclusion,
		Categories: append([]string(nil), p.Categories...),
	}
	if p.Baseline != nil {
		ref.BaselineRequestID = p.Baseline.RequestID
		ref.BaselineAttempt = p.Baseline.Attempt
		ref.Method = p.Baseline.Method
		ref.Path = p.Baseline.Path
	}
	if p.Candidate != nil {
		ref.CandidateRequestID = p.Candidate.RequestID
		ref.CandidateAttempt = p.Candidate.Attempt
		if ref.Method == "" {
			ref.Method = p.Candidate.Method
			ref.Path = p.Candidate.Path
		}
	}
	return ref
}

func ParseExecutionSide(value string) Side {
	switch Side(value) {
	case SideBaseline:
		return SideBaseline
	default:
		return SideCandidate
	}
}
