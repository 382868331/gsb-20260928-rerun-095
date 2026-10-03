package release

import (
	"errors"
	"fmt"
	"time"
)

// Pair sides and statuses.
const (
	PairSideBaseline  = "baseline"
	PairSideCandidate = "candidate"

	// PairStatusPaired: both sides have an adopted terminal outcome, so the
	// correspondence is explainable and the pair enters diff conclusions.
	PairStatusPaired = "paired"
	// PairStatusBaselineOnly / PairStatusCandidateOnly: one side reported,
	// the other side has not produced any outcome yet.
	PairStatusBaselineOnly  = "baseline_only"
	PairStatusCandidateOnly = "candidate_only"
	// PairStatusUndetermined: at least one side is still in flight, or all
	// known outcomes are late arrivals waiting for a round decision.
	PairStatusUndetermined = "undetermined"
)

// Diff conclusions for a paired sample.
const (
	ConclusionMatch           = "match"
	ConclusionCandidateBetter = "candidate_better"
	ConclusionAdverse         = "adverse"
	ConclusionBothFailed      = "both_failed"
)

// Round decisions about late arrivals.
const (
	RoundDecisionAdopted  = "adopted"
	RoundDecisionRejected = "rejected"
)

// ErrRoundClosed is returned when a round decision is requested for a round
// that is still collecting.
var ErrRoundClosed = errors.New("round is still open")

// ErrNoOpenRound is returned when closing a round while none is collecting.
var ErrNoOpenRound = errors.New("no open collection round")

// PairAttempt is one execution of one side of a business request. Retries
// after an interruption are kept individually with their source, but the
// pair's category coverage never counts them as independent samples.
type PairAttempt struct {
	RequestID string `json:"request_id"`
	Source    string `json:"source"`
	Outcome   string `json:"outcome"`
	Status    int    `json:"status"`
	// Late marks arrivals after the pair's collection round was closed;
	// they complete the round's facts but only enter the release basis
	// after an explicit round decision.
	Late bool      `json:"late,omitempty"`
	At   time.Time `json:"at"`
}

// PairSide accumulates the executions of one side (baseline or candidate).
type PairSide struct {
	Attempts []PairAttempt
}

// Pair correlates every execution of one business request on the baseline
// and on the candidate. Coverage counts pairs, not attempts.
type Pair struct {
	ID         string
	Round      int
	Method     string
	Path       string
	Categories []string
	Baseline   PairSide
	Candidate  PairSide
	open       map[string]string // request id -> side, for in-flight attempts
}

// PairRound is one collection round. Closing a round freezes which facts
// count; late arrivals only complete the round's facts and need an explicit
// decision to enter the confirmed release basis.
type PairRound struct {
	Number    int        `json:"number"`
	Open      bool       `json:"open"`
	ClosedAt  time.Time  `json:"closed_at,omitempty"`
	Decision  string     `json:"decision,omitempty"`
	DecidedAt *time.Time `json:"decided_at,omitempty"`
}

// PairConclusion is the adopted, traceable result of one paired business
// request as it entered a release decision.
type PairConclusion struct {
	PairID      string   `json:"pair_id"`
	Round       int      `json:"round"`
	Categories  []string `json:"categories"`
	Baseline    string   `json:"baseline"`
	Candidate   string   `json:"candidate"`
	Conclusion  string   `json:"conclusion"`
	BaselineID  string   `json:"baseline_request_id"`
	CandidateID string   `json:"candidate_request_id"`
}

// PairCounts summarizes pair states for one scope (overall or a category).
type PairCounts struct {
	Paired       int64 `json:"paired"`
	PairedValid  int64 `json:"paired_valid"`
	SingleSide   int64 `json:"single_side"`
	Undetermined int64 `json:"undetermined"`
	Adverse      int64 `json:"adverse"`
}

// StagedCandidateID returns the currently staged, plan-confirmed candidate
// that baseline-side pair observations attach to, or "" when there is none.
func (s *Store) StagedCandidateID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, candidate := range s.candidates {
		if candidate.State == StateStaged && candidate.PlanConfirmed {
			return candidate.ID
		}
	}
	return ""
}

// BeginPairObservation registers an in-flight execution of one side of a
// business request. Until FinishPairObservation arrives, the pair is
// undetermined.
func (s *Store) BeginPairObservation(candidateID, pairID, side, requestID, source, method, path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, ok := s.candidates[candidateID]
	if !ok || !candidate.PlanConfirmed {
		return
	}
	pair := candidate.pairLocked(pairID, method, path)
	if pair.open == nil {
		pair.open = make(map[string]string)
	}
	pair.open[requestID] = side
}

// FinishPairObservation records the terminal outcome of one execution. The
// attempt is appended to its side (keeping retries and their sources), the
// side's adopted attempt is recomputed by the fixed adoption rule, and a
// repeated request id never produces a second attempt.
func (s *Store) FinishPairObservation(candidateID, pairID, side string, outcome RehearsalOutcome) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, ok := s.candidates[candidateID]
	if !ok {
		return
	}
	pair := candidate.pairLocked(pairID, outcome.Method, outcome.Path)
	if pair.open != nil {
		delete(pair.open, outcome.RequestID)
	}
	attempts := pair.sideAttempts(side)
	if outcome.RequestID != "" {
		for _, attempt := range *attempts {
			if attempt.RequestID == outcome.RequestID {
				return
			}
		}
	}
	round := candidate.Rounds[pair.Round-1]
	*attempts = append(*attempts, PairAttempt{
		RequestID: outcome.RequestID,
		Source:    outcome.Source,
		Outcome:   classifyOutcome(outcome),
		Status:    outcome.Status,
		Late:      !round.Open,
		At:        time.Now(),
	})
}

// RoundClose reports the round that was closed and the requests whose pair
// was still incomplete at close time.
type RoundClose struct {
	Round    int        `json:"round"`
	Unpaired []PairView `json:"unpaired"`
}

// CloseRound ends the current collection round and lists the requests that
// are not paired yet. Observations for new business requests afterwards open
// the next round; late arrivals for this round's pairs only complete its
// facts until DecideRound explicitly adopts them into the release basis.
func (s *Store) CloseRound(candidateID string) (RoundClose, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, ok := s.candidates[candidateID]
	if !ok {
		return RoundClose{}, fmt.Errorf("%w %q", ErrUnknownCandidate, candidateID)
	}
	last := candidate.Rounds[len(candidate.Rounds)-1]
	if !last.Open {
		return RoundClose{}, ErrNoOpenRound
	}
	last.Open = false
	last.ClosedAt = time.Now()
	result := RoundClose{Round: last.Number, Unpaired: []PairView{}}
	for _, pair := range candidate.pairs {
		if pair.Round != last.Number {
			continue
		}
		if candidate.liveStatus(pair) != PairStatusPaired {
			result.Unpaired = append(result.Unpaired, candidate.pairView(pair))
		}
	}
	return result, nil
}

// DecideRound records the explicit decision whether late arrivals of a
// closed round enter the confirmed release basis (adopt) or stay excluded
// (reject). The decision is kept on the round for audit.
func (s *Store) DecideRound(candidateID string, round int, adopt bool) (Candidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, ok := s.candidates[candidateID]
	if !ok {
		return Candidate{}, fmt.Errorf("%w %q", ErrUnknownCandidate, candidateID)
	}
	if round < 1 || round > len(candidate.Rounds) {
		return Candidate{}, fmt.Errorf("unknown round %d", round)
	}
	target := candidate.Rounds[round-1]
	if target.Open {
		return Candidate{}, ErrRoundClosed
	}
	if adopt {
		target.Decision = RoundDecisionAdopted
	} else {
		target.Decision = RoundDecisionRejected
	}
	now := time.Now()
	target.DecidedAt = &now
	copied := *candidate
	copied.Readiness = candidate.readinessLocked()
	return copied, nil
}

// pairLocked finds or creates the pair for one business request. A pair
// first observed while every round is closed opens the next round.
func (c *Candidate) pairLocked(pairID, method, path string) *Pair {
	if pair, ok := c.pairByID[pairID]; ok {
		return pair
	}
	last := c.Rounds[len(c.Rounds)-1]
	if !last.Open {
		last = &PairRound{Number: last.Number + 1, Open: true}
		c.Rounds = append(c.Rounds, last)
	}
	pair := &Pair{
		ID:         pairID,
		Round:      last.Number,
		Method:     method,
		Path:       path,
		Categories: matchedCategories(c.snapshot, method, path),
		open:       make(map[string]string),
	}
	c.pairs = append(c.pairs, pair)
	c.pairByID[pairID] = pair
	return pair
}

func (p *Pair) sideAttempts(side string) *[]PairAttempt {
	if side == PairSideCandidate {
		return &p.Candidate.Attempts
	}
	return &p.Baseline.Attempts
}

// adopt picks the attempt that represents one side according to the fixed
// adoption rule: the first or the latest terminal execution.
func adopt(attempts []PairAttempt, rule string) *PairAttempt {
	if len(attempts) == 0 {
		return nil
	}
	if rule == "first" {
		chosen := attempts[0]
		return &chosen
	}
	chosen := attempts[len(attempts)-1]
	return &chosen
}

// basisAttempts filters a side's attempts down to the release basis: late
// arrivals of a closed round count only after that round was explicitly
// adopted.
func (c *Candidate) basisAttempts(pair *Pair, side string) []PairAttempt {
	round := c.Rounds[pair.Round-1]
	includeLate := round.Decision == RoundDecisionAdopted
	result := []PairAttempt{}
	for _, attempt := range *pair.sideAttempts(side) {
		if attempt.Late && !includeLate {
			continue
		}
		result = append(result, attempt)
	}
	return result
}

// liveStatus classifies the pair over every known attempt, including late
// arrivals that are not (yet) part of the release basis.
func (c *Candidate) liveStatus(pair *Pair) string {
	if len(pair.open) > 0 {
		return PairStatusUndetermined
	}
	baseline := adopt(pair.Baseline.Attempts, c.PairAdoption)
	candidate := adopt(pair.Candidate.Attempts, c.PairAdoption)
	switch {
	case baseline != nil && candidate != nil:
		return PairStatusPaired
	case baseline != nil:
		return PairStatusBaselineOnly
	case candidate != nil:
		return PairStatusCandidateOnly
	default:
		return PairStatusUndetermined
	}
}

// basisStatus classifies the pair over the release basis only.
func (c *Candidate) basisStatus(pair *Pair) string {
	if len(pair.open) > 0 {
		return PairStatusUndetermined
	}
	baseline := adopt(c.basisAttempts(pair, PairSideBaseline), c.PairAdoption)
	candidate := adopt(c.basisAttempts(pair, PairSideCandidate), c.PairAdoption)
	switch {
	case baseline != nil && candidate != nil:
		return PairStatusPaired
	case baseline != nil || candidate != nil:
		if baseline != nil {
			return PairStatusBaselineOnly
		}
		return PairStatusCandidateOnly
	default:
		return PairStatusUndetermined
	}
}

// pairConclusion compares the adopted outcomes of a paired sample.
func pairConclusion(baseline, candidate *PairAttempt) string {
	baselineOK := baseline.Outcome == "succeeded"
	candidateOK := candidate.Outcome == "succeeded"
	switch {
	case baselineOK && candidateOK:
		return ConclusionMatch
	case candidateOK:
		return ConclusionCandidateBetter
	case baselineOK:
		return ConclusionAdverse
	default:
		return ConclusionBothFailed
	}
}

// pairCountsLocked aggregates pair states over the release basis, optionally
// restricted to one category. Each pair counts once per scope no matter how
// many retries or categories it has.
func (c *Candidate) pairCountsLocked(categoryIDs ...string) PairCounts {
	counts := PairCounts{}
	for _, pair := range c.pairs {
		if len(categoryIDs) > 0 && !containsAny(pair.Categories, categoryIDs) {
			continue
		}
		switch c.basisStatus(pair) {
		case PairStatusUndetermined:
			counts.Undetermined++
		case PairStatusBaselineOnly, PairStatusCandidateOnly:
			counts.SingleSide++
		case PairStatusPaired:
			counts.Paired++
			baseline := adopt(c.basisAttempts(pair, PairSideBaseline), c.PairAdoption)
			candidate := adopt(c.basisAttempts(pair, PairSideCandidate), c.PairAdoption)
			switch pairConclusion(baseline, candidate) {
			case ConclusionMatch, ConclusionCandidateBetter:
				counts.PairedValid++
			case ConclusionAdverse:
				counts.Adverse++
			}
		}
	}
	return counts
}

// pairCoverageShortfallLocked explains which categories miss paired-valid
// samples or carry adverse high-risk diffs, or returns "" when pairing-based
// coverage is satisfied. Adverse diffs in a high-risk category can never be
// averaged away by other categories' successes.
func (c *Candidate) pairCoverageShortfallLocked() string {
	reasons := []string{}
	for _, category := range c.CoveragePlan {
		counts := c.pairCountsLocked(category.ID)
		required := int64(category.MinSamples)
		if counts.PairedValid < required {
			reasons = append(reasons, fmt.Sprintf("category %q has %d/%d paired valid sample(s) (%d missing)",
				category.ID, counts.PairedValid, required, required-counts.PairedValid))
		}
		if category.HighRisk && counts.Adverse > 0 {
			reasons = append(reasons, fmt.Sprintf("high-risk category %q has %d adverse pair(s)", category.ID, counts.Adverse))
		}
	}
	if len(reasons) == 0 {
		return ""
	}
	return joinReasons(reasons)
}

// pairConclusionsLocked snapshots the adopted request pairs that form the
// release basis, so a later release record stays traceable to them.
func (c *Candidate) pairConclusionsLocked() []PairConclusion {
	conclusions := []PairConclusion{}
	for _, pair := range c.pairs {
		if c.basisStatus(pair) != PairStatusPaired {
			continue
		}
		baseline := adopt(c.basisAttempts(pair, PairSideBaseline), c.PairAdoption)
		candidate := adopt(c.basisAttempts(pair, PairSideCandidate), c.PairAdoption)
		conclusions = append(conclusions, PairConclusion{
			PairID:      pair.ID,
			Round:       pair.Round,
			Categories:  append([]string(nil), pair.Categories...),
			Baseline:    baseline.Outcome,
			Candidate:   candidate.Outcome,
			Conclusion:  pairConclusion(baseline, candidate),
			BaselineID:  baseline.RequestID,
			CandidateID: candidate.RequestID,
		})
	}
	return conclusions
}

func containsAny(haystack, needles []string) bool {
	for _, needle := range needles {
		for _, item := range haystack {
			if item == needle {
				return true
			}
		}
	}
	return false
}

func joinReasons(reasons []string) string {
	result := ""
	for i, reason := range reasons {
		if i > 0 {
			result += "; "
		}
		result += reason
	}
	return result
}
