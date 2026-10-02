package release

import (
	"fmt"
	"reflect"
	"sort"
	"time"

	"gateway/internal/config"
)

// AcknowledgePlan records that the operator confirmed the required request
// categories, per-category minimum valid samples and high-risk rules before
// any rehearsal traffic is accepted. Evidence gathered before this point is
// impossible because candidate traffic is refused while pending.
func (s *Store) AcknowledgePlan(candidateID, source string) (Candidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, err := s.stagedLocked(candidateID)
	if err != nil {
		return Candidate{}, err
	}
	if candidate.plan == nil {
		return Candidate{}, fmt.Errorf("candidate %q has no coverage plan to acknowledge", candidateID)
	}
	if source == "" {
		source = "manual"
	}
	candidate.planAcknowledged = true
	candidate.planAckAt = time.Now()
	candidate.planAckSource = source
	return *candidate, nil
}

// StartSupplement opens a traceable top-up batch for categories that are
// still uncovered. The returned batch id must be sent with each supplemental
// request via SupplementHeader; every request's source and outcome stay
// attached to the batch.
func (s *Store) StartSupplement(candidateID, source string, categoryIDs []string) (SupplementBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, err := s.stagedLocked(candidateID)
	if err != nil {
		return SupplementBatch{}, err
	}
	if candidate.plan == nil {
		return SupplementBatch{}, fmt.Errorf("candidate %q has no coverage plan", candidateID)
	}
	if !candidate.planAcknowledged {
		return SupplementBatch{}, fmt.Errorf("%w for candidate %q", ErrPlanPending, candidateID)
	}
	for _, id := range categoryIDs {
		if categoryByID(candidate.plan, id) == nil {
			return SupplementBatch{}, fmt.Errorf("unknown coverage category %q", id)
		}
	}
	if source == "" {
		source = "manual"
	}
	candidate.supplementSeq++
	batch := &SupplementBatch{
		ID:          fmt.Sprintf("%s-sup-%d", candidateID, candidate.supplementSeq),
		CreatedAt:   time.Now(),
		Source:      source,
		CategoryIDs: append([]string(nil), categoryIDs...),
	}
	candidate.supplements = append(candidate.supplements, batch)
	candidate.supplementOpen[batch.ID] = len(candidate.supplements) - 1
	return *batch, nil
}

// EndSupplement closes a top-up batch so its id can no longer be used to
// send candidate traffic. Existing evidence stays attached to the batch.
func (s *Store) EndSupplement(batchID string) (SupplementBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, index, ok := s.findSupplementLocked(batchID)
	if !ok {
		return SupplementBatch{}, fmt.Errorf("unknown supplement batch %q", batchID)
	}
	delete(candidate.supplementOpen, batchID)
	return *candidate.supplements[index], nil
}

func (s *Store) findSupplementLocked(batchID string) (*Candidate, int, bool) {
	for _, candidate := range s.candidates {
		if index, ok := candidate.supplementOpen[batchID]; ok {
			return candidate, index, true
		}
	}
	return nil, 0, false
}

// CoverageReport renders the full per-category report for a candidate. It is
// available for every candidate revision, including ones whose evidence is
// no longer a valid basis for release.
func (s *Store) CoverageReport(candidateID string) (CoverageReport, Candidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, ok := s.candidates[candidateID]
	if !ok {
		return CoverageReport{}, Candidate{}, fmt.Errorf("%w %q", ErrUnknownCandidate, candidateID)
	}
	report := buildReport(candidate, s.active)
	copied := *candidate
	copied.Readiness = candidate.readinessLocked()
	return report, copied, nil
}

func (s *Store) stagedLocked(candidateID string) (*Candidate, error) {
	candidate, ok := s.candidates[candidateID]
	if !ok {
		return nil, fmt.Errorf("%w %q", ErrUnknownCandidate, candidateID)
	}
	if candidate.State != StateStaged {
		return nil, fmt.Errorf("%w: %s", ErrNotStaged, candidate.State)
	}
	return candidate, nil
}

func buildCandidateReport(candidate *Candidate) CoverageReport {
	return buildReport(candidate, nil)
}

func buildReport(candidate *Candidate, active *config.Snapshot) CoverageReport {
	report := CoverageReport{
		Required:           candidate.plan != nil,
		Acknowledged:       candidate.planAcknowledged,
		AcknowledgedAt:     candidate.planAckAt,
		AcknowledgedSource: candidate.planAckSource,
		Plan:               categoryViews(candidate.plan),
		Categories:         []CategoryReport{},
		Supplements:        supplementsCopy(candidate.supplements),
		Evidence:           append([]Evidence(nil), candidate.evidence...),
	}
	report.Overall.InFlight = candidate.Rehearsal.InFlight

	counts := make(map[string]*CategoryReport)
	if candidate.plan != nil {
		for _, view := range report.Plan {
			counts[view.ID] = &CategoryReport{Plan: view, BaselineMatches: []MatchView{}, CandidateMatches: []MatchView{}, Representatives: []RequestDiff{}}
		}
	}
	seen := map[string]struct{}{}
	for _, evidence := range candidate.evidence {
		if evidence.Duplicate {
			report.Overall.Duplicates++
			continue
		}
		key := evidence.SampleID
		if key == "" {
			key = evidence.RequestID
		}
		if _, ok := seen[key]; ok {
			report.Overall.Duplicates++
			continue
		}
		seen[key] = struct{}{}
		report.Overall.DistinctSamples++
		addOverall(&report.Overall, evidence.Outcome)
		for _, id := range evidence.Categories {
			category := counts[id]
			if category == nil {
				continue
			}
			category.Matched++
			addCategory(category, evidence)
		}
	}

	if candidate.plan != nil {
		for _, definition := range candidate.plan.Categories {
			category := counts[definition.ID]
			populateRepresentatives(category, definition, active, candidate.snapshot)
			category.MissingSamples = int64(definition.MinValidSamples) - category.Valid
			if category.MissingSamples < 0 {
				category.MissingSamples = 0
			}
			reasons := missingReason(definition, category)
			category.Reasons = reasons
			category.Satisfied = len(reasons) == 0
		}
		ids := make([]string, 0, len(counts))
		for id := range counts {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		for _, id := range ids {
			report.Categories = append(report.Categories, *counts[id])
		}
	}

	if candidate.State == StateSuperseded {
		report.Invalid = true
		report.InvalidReason = candidate.InvalidReason
		if report.InvalidReason == "" {
			report.InvalidReason = "candidate was superseded"
		}
	} else if candidate.State == StateDiscarded || candidate.State == StateConfirmed {
		report.Invalid = true
		report.InvalidReason = "candidate is " + string(candidate.State) + "; this evidence is a historical record, not a current release basis"
	} else if !candidate.planAcknowledged {
		report.Invalid = true
		report.InvalidReason = "coverage plan was not acknowledged before rehearsal"
	}
	return report
}

func addOverall(overall *OverallCoverage, outcome EvidenceOutcome) {
	switch outcome {
	case OutcomeValid:
		overall.Valid++
	case OutcomeFailed:
		overall.Failed++
	case OutcomeCanceled:
		overall.Canceled++
	case OutcomeAborted:
		overall.Aborted++
	}
}

func addCategory(category *CategoryReport, evidence Evidence) {
	switch evidence.Outcome {
	case OutcomeValid:
		category.Valid++
	case OutcomeFailed:
		category.Failed++
	case OutcomeCanceled:
		category.Canceled++
	case OutcomeAborted:
		category.Aborted++
	}
}

func supplementsCopy(batches []*SupplementBatch) []SupplementBatch {
	result := make([]SupplementBatch, 0, len(batches))
	for _, batch := range batches {
		result = append(result, *batch)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

func populateRepresentatives(category *CategoryReport, definition config.CoverageCategory, active, candidateSnapshot *config.Snapshot) {
	if category == nil {
		return
	}
	for _, request := range representativeRequests(&config.CoveragePlan{Categories: []config.CoverageCategory{definition}}) {
		var activeMatch MatchView
		if active != nil {
			activeMatch = matchView(active, request)
		}
		candidateMatch := matchView(candidateSnapshot, request)
		category.BaselineMatches = append(category.BaselineMatches, activeMatch)
		category.CandidateMatches = append(category.CandidateMatches, candidateMatch)
		category.Representatives = append(category.Representatives, RequestDiff{
			Method: request.Method, Path: request.URL.Path,
			Active: &activeMatch, Candidate: &candidateMatch,
			Changed: !equalJSON(activeMatch, candidateMatch),
		})
	}
}

func (s *Store) supersedeReasonLocked(old *Candidate, newSnapshot *config.Snapshot) string {
	newPlan := newSnapshot.CoveragePlan()
	if !reflect.DeepEqual(old.plan, newPlan) {
		if old.plan == nil || newPlan == nil {
			return "coverage plan was added or removed; prior evidence no longer satisfies the release gate"
		}
		return "coverage category rules or minimum sample requirements changed; prior per-category evidence is invalid as a release basis"
	}
	if Fingerprint(old.snapshot) == Fingerprint(newSnapshot) {
		return "candidate was superseded"
	}
	return "routing, upstream target, timeout or retry limits changed; prior rehearsal evidence is invalid as a release basis"
}
