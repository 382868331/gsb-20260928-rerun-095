package release

import (
	"fmt"
	"sort"
	"time"
)

// Side identifies which side of a paired business request produced an
// outcome: the baseline (the artifact already in production) or the
// candidate under rehearsal.
type Side string

const (
	// SideBaseline is the original artifact's side of a paired request.
	SideBaseline Side = "baseline"
	// SideCandidate is the rehearsed candidate's side of a paired request.
	SideCandidate Side = "candidate"
)

// EvidencePolicyFirstTerminal is the adoption policy fixed when a candidate
// is staged: the first terminal result of each side of a business request is
// adopted, and later retries are kept as evidence but can never replace it.
const EvidencePolicyFirstTerminal = "first-terminal-result"

// Pair conclusions stored on BusinessPair.Conclusion and surfaced through
// AdoptedPairRef so every release conclusion traces back to concrete pairs.
const (
	// ConclusionUndecided: only the baseline side has reported; the pair
	// is still waiting on the candidate side.
	ConclusionUndecided = "undecided"
	// ConclusionSingleSided: only the candidate side has reported, so no
	// baseline comparison exists for this business request.
	ConclusionSingleSided = "single_sided"
	// ConclusionPairedValid: both sides reported and the candidate side
	// succeeded.
	ConclusionPairedValid = "paired_valid"
	// ConclusionPairedAdverse: both sides reported and the candidate side
	// failed, was canceled, aborted or invalid.
	ConclusionPairedAdverse = "paired_adverse"
)

// PairSide is the adopted (first terminal) outcome of one side of a paired
// business request.
type PairSide struct {
	RequestID string    `json:"request_id"`
	Outcome   string    `json:"outcome"`
	Attempt   int       `json:"attempt,omitempty"`
	At        time.Time `json:"at"`
}

// BusinessPair correlates the baseline and candidate executions of one
// business request inside one collection round.
type BusinessPair struct {
	BusinessID string    `json:"business_id"`
	Round      string    `json:"round"`
	Baseline   *PairSide `json:"baseline,omitempty"`
	Candidate  *PairSide `json:"candidate,omitempty"`
	Conclusion string    `json:"conclusion"`
}

// CollectionRound is one bounded evidence-collection window of a candidate.
// A round opens lazily with the first paired execution and closes when the
// candidate is resolved (confirmed, discarded or superseded). Outcomes that
// arrive after the round closed are late and handled via LateDecision.
type CollectionRound struct {
	ID          string    `json:"id"`
	OpenedAt    time.Time `json:"opened_at"`
	ClosedAt    time.Time `json:"closed_at,omitempty"`
	Closed      bool      `json:"closed"`
	CloseReason string    `json:"close_reason,omitempty"`
}

// LateDecision records how a paired outcome that arrived after its
// collection round closed was handled. Late outcomes stay visible in the
// evidence trail but never change counts, coverage or conclusions.
type LateDecision struct {
	RequestID  string    `json:"request_id"`
	BusinessID string    `json:"business_id"`
	Side       Side      `json:"side"`
	Round      string    `json:"round,omitempty"`
	Decision   string    `json:"decision"`
	Reason     string    `json:"reason"`
	At         time.Time `json:"at"`
}

// AdoptedPairRef points a release conclusion at the concrete request pair it
// was built from: the business request, the collection round and the adopted
// request ids of both sides.
type AdoptedPairRef struct {
	BusinessID         string `json:"business_id"`
	Round              string `json:"round"`
	BaselineRequestID  string `json:"baseline_request_id,omitempty"`
	CandidateRequestID string `json:"candidate_request_id,omitempty"`
	Conclusion         string `json:"conclusion"`
}

// RecordExecution records the terminal outcome of one execution of a
// rehearsal-bound request. Outcomes carrying a BusinessID take part in
// paired baseline-vs-candidate correlation inside the candidate's open
// collection round; outcomes without one fall back to the legacy
// uncorrelated evidence path. Returns ErrUnknownCandidate for unknown ids.
func (s *Store) RecordExecution(candidateID string, outcome RehearsalOutcome) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, ok := s.candidates[candidateID]
	if !ok {
		return fmt.Errorf("%w %q", ErrUnknownCandidate, candidateID)
	}
	if candidate.Rehearsal.InFlight > 0 {
		candidate.Rehearsal.InFlight--
	}
	kind := classifyOutcome(outcome)
	if outcome.BusinessID == "" {
		candidate.legacyFinishLocked(outcome, kind)
		return nil
	}
	candidate.ensureRoundLocked()
	candidate.pairedFinishLocked(outcome, kind)
	return nil
}

// pairedFinishLocked records one business-correlated terminal outcome. The
// first terminal result of each side is adopted; retries of an already
// concluded side are kept as evidence but never replace the adopted result.
// Outcomes arriving after the collection round closed are late: they are
// logged with a LateDecision and never counted.
func (c *Candidate) pairedFinishLocked(outcome RehearsalOutcome, kind string) {
	source := outcome.Source
	if source == "" {
		source = "rehearsal"
	}
	categories := matchedCategories(c.snapshot, outcome.Method, outcome.Path)
	entry := EvidenceEntry{
		RequestID:  outcome.RequestID,
		BusinessID: outcome.BusinessID,
		Side:       outcome.Side,
		Attempt:    outcome.Attempt,
		Source:     source,
		Method:     outcome.Method,
		Path:       outcome.Path,
		Categories: categories,
		Outcome:    kind,
		At:         time.Now(),
	}

	if c.currentRound == nil || c.currentRound.Closed {
		roundID := ""
		reason := "no collection round was ever opened"
		if c.currentRound != nil {
			roundID = c.currentRound.ID
			reason = fmt.Sprintf("collection round %s closed: %s", c.currentRound.ID, c.currentRound.CloseReason)
		}
		entry.Round = roundID
		entry.Late = true
		entry.Counted = false
		entry.Conclusion = "late-not-counted"
		c.Evidence = append(c.Evidence, entry)
		c.lateDecisions = append(c.lateDecisions, LateDecision{
			RequestID:  outcome.RequestID,
			BusinessID: outcome.BusinessID,
			Side:       outcome.Side,
			Round:      roundID,
			Decision:   "recorded-not-counted",
			Reason:     reason,
			At:         entry.At,
		})
		return
	}

	entry.Round = c.currentRound.ID

	counted := true
	if outcome.RequestID != "" {
		if _, seen := c.seen[outcome.RequestID]; seen {
			counted = false
		} else {
			c.seen[outcome.RequestID] = struct{}{}
		}
	}
	if !counted {
		entry.Counted = false
		entry.Conclusion = "duplicate-not-counted"
		c.Evidence = append(c.Evidence, entry)
		return
	}

	pair := c.pairs[outcome.BusinessID]
	if pair == nil {
		pair = &BusinessPair{
			BusinessID: outcome.BusinessID,
			Round:      c.currentRound.ID,
			Conclusion: ConclusionUndecided,
		}
		c.pairs[outcome.BusinessID] = pair
	}

	var side **PairSide
	switch outcome.Side {
	case SideBaseline:
		side = &pair.Baseline
	default:
		side = &pair.Candidate
	}
	if *side != nil {
		// A retry of an already concluded side: kept as evidence, but the
		// first terminal result stays adopted and counts are untouched.
		entry.Counted = false
		entry.Conclusion = "retry-not-adopted"
		c.Evidence = append(c.Evidence, entry)
		return
	}

	*side = &PairSide{
		RequestID: outcome.RequestID,
		Outcome:   kind,
		Attempt:   outcome.Attempt,
		At:        entry.At,
	}
	entry.Counted = true
	entry.Adopted = true

	if outcome.Side == SideCandidate {
		// Only the candidate side measures the rehearsal itself; baseline
		// outcomes are the reference the candidate is compared against.
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

	pair.Conclusion = concludePair(pair)
	entry.Conclusion = pair.Conclusion
	c.Evidence = append(c.Evidence, entry)
	c.recomputePairStatsLocked()
}

// concludePair derives a pair's conclusion from the adopted sides.
func concludePair(pair *BusinessPair) string {
	switch {
	case pair.Baseline != nil && pair.Candidate != nil:
		if pair.Candidate.Outcome == "succeeded" {
			return ConclusionPairedValid
		}
		return ConclusionPairedAdverse
	case pair.Candidate != nil:
		return ConclusionSingleSided
	default:
		return ConclusionUndecided
	}
}

// recomputePairStatsLocked rebuilds the pair breakdown of the rehearsal
// summary from the adopted pairs, so retries and duplicates can never skew
// the counts.
func (c *Candidate) recomputePairStatsLocked() {
	var pairedTotal, pairedValid, pairedAdverse, singleSided, undecided int64
	for _, pair := range c.pairs {
		switch concludePair(pair) {
		case ConclusionPairedValid:
			pairedTotal++
			pairedValid++
		case ConclusionPairedAdverse:
			pairedTotal++
			pairedAdverse++
		case ConclusionSingleSided:
			singleSided++
		default:
			undecided++
		}
	}
	c.Rehearsal.PairedTotal = pairedTotal
	c.Rehearsal.PairedValid = pairedValid
	c.Rehearsal.PairedAdverse = pairedAdverse
	c.Rehearsal.SingleSided = singleSided
	c.Rehearsal.Undecided = undecided
}

// openRoundLocked starts a new collection round for paired evidence.
func (c *Candidate) openRoundLocked() {
	c.roundSeq++
	round := &CollectionRound{
		ID:       fmt.Sprintf("round-%d", c.roundSeq),
		OpenedAt: time.Now(),
	}
	c.rounds = append(c.rounds, round)
	c.currentRound = round
}

// closeRoundLocked closes the open collection round, if any. Paired outcomes
// that arrive afterwards are late and handled via LateDecision.
func (c *Candidate) closeRoundLocked(reason string) {
	if c.currentRound == nil || c.currentRound.Closed {
		return
	}
	c.currentRound.Closed = true
	c.currentRound.ClosedAt = time.Now()
	c.currentRound.CloseReason = reason
}

// ensureRoundLocked opens a collection round on demand when paired evidence
// arrives and none is open.
func (c *Candidate) ensureRoundLocked() {
	if c.currentRound == nil {
		c.openRoundLocked()
	}
}

// adoptedPairsLocked flattens the adopted pairs into stable, sorted
// references so every release conclusion traces back to its request pair.
func (c *Candidate) adoptedPairsLocked() []AdoptedPairRef {
	if len(c.pairs) == 0 {
		return nil
	}
	ids := make([]string, 0, len(c.pairs))
	for id := range c.pairs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	refs := make([]AdoptedPairRef, 0, len(ids))
	for _, id := range ids {
		pair := c.pairs[id]
		ref := AdoptedPairRef{
			BusinessID: pair.BusinessID,
			Round:      pair.Round,
			Conclusion: concludePair(pair),
		}
		if pair.Baseline != nil {
			ref.BaselineRequestID = pair.Baseline.RequestID
		}
		if pair.Candidate != nil {
			ref.CandidateRequestID = pair.Candidate.RequestID
		}
		refs = append(refs, ref)
	}
	return refs
}

// pairViewsLocked returns a sorted copy of the adopted pairs for reporting.
func (c *Candidate) pairViewsLocked() []BusinessPair {
	if len(c.pairs) == 0 {
		return nil
	}
	ids := make([]string, 0, len(c.pairs))
	for id := range c.pairs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	views := make([]BusinessPair, 0, len(ids))
	for _, id := range ids {
		pair := c.pairs[id]
		view := BusinessPair{
			BusinessID: pair.BusinessID,
			Round:      pair.Round,
			Conclusion: concludePair(pair),
		}
		if pair.Baseline != nil {
			baseline := *pair.Baseline
			view.Baseline = &baseline
		}
		if pair.Candidate != nil {
			candidate := *pair.Candidate
			view.Candidate = &candidate
		}
		views = append(views, view)
	}
	return views
}

// roundViewsLocked returns a copy of the collection rounds for reporting.
func (c *Candidate) roundViewsLocked() []CollectionRound {
	if len(c.rounds) == 0 {
		return nil
	}
	views := make([]CollectionRound, len(c.rounds))
	for i := range c.rounds {
		views[i] = *c.rounds[i]
	}
	return views
}

// lateDecisionsLocked returns a copy of the late-arrival decisions.
func (c *Candidate) lateDecisionsLocked() []LateDecision {
	if len(c.lateDecisions) == 0 {
		return nil
	}
	decisions := make([]LateDecision, len(c.lateDecisions))
	copy(decisions, c.lateDecisions)
	return decisions
}
