package release

import (
	"fmt"
	"net/http"
	"net/url"

	"gateway/internal/config"
)

// CategoryReport shows one coverage category's rehearsal result: how many
// valid samples it has against its requirement, what is still missing, why
// it fails, and how the active release and the candidate would route a
// representative request of this category.
type CategoryReport struct {
	ID        string   `json:"id"`
	HighRisk  bool     `json:"high_risk"`
	Required  int64    `json:"required"`
	Valid     int64    `json:"valid"`
	Missing   int64    `json:"missing"`
	Failed    int64    `json:"failed"`
	Canceled  int64    `json:"canceled"`
	Aborted   int64    `json:"aborted"`
	Invalid   int64    `json:"invalid"`
	Satisfied bool     `json:"satisfied"`
	Reasons   []string `json:"reasons"`
	// Diff compares the baseline (active release) with the candidate for a
	// representative request of this category.
	Diff *RequestDiff `json:"diff,omitempty"`
}

// SupplementReport aggregates the evidence contributed by one rehearsal
// source (the initial run or a named supplement batch), so every top-up of
// missing categories keeps its origin and result traceable.
type SupplementReport struct {
	Source     string   `json:"source"`
	Requests   int64    `json:"requests"`
	Counted    int64    `json:"counted"`
	Duplicates int64    `json:"duplicates"`
	Succeeded  int64    `json:"succeeded"`
	Failed     int64    `json:"failed"`
	Canceled   int64    `json:"canceled"`
	Aborted    int64    `json:"aborted"`
	Invalid    int64    `json:"invalid"`
	Categories []string `json:"categories"`
}

// CandidateReport is the full rehearsal report for one candidate revision.
// Reports of superseded or discarded candidates stay available, with
// EvidenceValid=false and InvalidatedReason explaining which release basis
// expired.
type CandidateReport struct {
	CandidateID       string             `json:"candidate_id"`
	State             CandidateState     `json:"state"`
	EvidenceValid     bool               `json:"evidence_valid"`
	InvalidatedReason string             `json:"invalidated_reason,omitempty"`
	PlanConfirmed     bool               `json:"plan_confirmed"`
	Overall           RehearsalSummary   `json:"overall"`
	Categories        []CategoryReport   `json:"categories"`
	Supplements       []SupplementReport `json:"supplements"`
	Evidence          []EvidenceEntry    `json:"evidence"`
}

// Report builds the rehearsal report for one candidate, including resolved
// revisions so old evidence stays auditable.
func (s *Store) Report(candidateID string) (CandidateReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, ok := s.candidates[candidateID]
	if !ok {
		return CandidateReport{}, fmt.Errorf("%w %q", ErrUnknownCandidate, candidateID)
	}
	report := CandidateReport{
		CandidateID:   candidate.ID,
		State:         candidate.State,
		EvidenceValid: candidate.State == StateStaged || candidate.State == StateConfirmed,
		PlanConfirmed: candidate.PlanConfirmed,
		Overall:       candidate.Rehearsal,
		Categories:    candidate.categoryReportsLocked(s.active, s.activeID),
		Supplements:   supplementReports(candidate.Evidence),
		Evidence:      append([]EvidenceEntry(nil), candidate.Evidence...),
	}
	switch candidate.State {
	case StateSuperseded:
		if candidate.InvalidatedBy != "" {
			report.InvalidatedReason = fmt.Sprintf("superseded by %s: rehearsal evidence is no longer a valid release basis", candidate.InvalidatedBy)
		} else {
			report.InvalidatedReason = "superseded: rehearsal evidence is no longer a valid release basis"
		}
	case StateDiscarded:
		report.InvalidatedReason = "discarded: rehearsal evidence is no longer a valid release basis"
	}
	return report, nil
}

// categoryReportsLocked evaluates every planned category against the
// accumulated evidence and attaches a baseline-vs-candidate routing diff
// for a representative request of the category.
func (c *Candidate) categoryReportsLocked(active *config.Snapshot, activeID string) []CategoryReport {
	reports := make([]CategoryReport, 0, len(c.CoveragePlan))
	for _, category := range c.CoveragePlan {
		stats := c.Coverage[category.ID]
		if stats == nil {
			stats = &CategoryStats{Required: int64(category.MinSamples)}
		}
		report := CategoryReport{
			ID:       category.ID,
			HighRisk: category.HighRisk,
			Required: stats.Required,
			Valid:    stats.Valid,
			Failed:   stats.Failed,
			Canceled: stats.Canceled,
			Aborted:  stats.Aborted,
			Invalid:  stats.Invalid,
			Reasons:  []string{},
		}
		if stats.Valid < stats.Required {
			report.Missing = stats.Required - stats.Valid
			report.Reasons = append(report.Reasons, fmt.Sprintf("valid samples %d/%d: %d missing", stats.Valid, stats.Required, report.Missing))
		}
		if category.HighRisk {
			bad := stats.Failed + stats.Canceled + stats.Aborted
			if bad > 0 {
				report.Reasons = append(report.Reasons, fmt.Sprintf("high-risk category has %d failed/canceled/aborted sample(s)", bad))
			}
		}
		report.Satisfied = len(report.Reasons) == 0
		report.Diff = categoryDiff(active, activeID, c, category)
		reports = append(reports, report)
	}
	return reports
}

// categoryDiff compares how the baseline and the candidate route one
// representative request of the category (first declared method/prefix).
func categoryDiff(active *config.Snapshot, activeID string, candidate *Candidate, category config.CoverageCategory) *RequestDiff {
	if active == nil {
		return nil
	}
	method := http.MethodGet
	if len(category.Methods) > 0 {
		method = category.Methods[0]
	}
	path := "/"
	if len(category.Prefixes) > 0 {
		path = category.Prefixes[0]
	}
	request := &http.Request{Method: method, URL: &url.URL{Path: path}}
	activeMatch := matchView(active, request)
	candidateMatch := matchView(candidate.snapshot, request)
	return &RequestDiff{
		Method:    method,
		Path:      path,
		Active:    &activeMatch,
		Candidate: &candidateMatch,
		Changed:   !equalJSON(activeMatch, candidateMatch),
	}
}

// supplementReports groups evidence entries by their source, preserving
// first-seen order, so each rehearsal batch and each supplement of missing
// categories keeps a traceable origin and result.
func supplementReports(evidence []EvidenceEntry) []SupplementReport {
	order := []string{}
	bySource := map[string]*SupplementReport{}
	for _, entry := range evidence {
		group, ok := bySource[entry.Source]
		if !ok {
			group = &SupplementReport{Source: entry.Source, Categories: []string{}}
			bySource[entry.Source] = group
			order = append(order, entry.Source)
		}
		group.Requests++
		if entry.Counted {
			group.Counted++
		} else {
			group.Duplicates++
		}
		switch entry.Outcome {
		case "succeeded":
			group.Succeeded++
		case "failed":
			group.Failed++
		case "canceled":
			group.Canceled++
		case "aborted":
			group.Aborted++
		case "invalid":
			group.Invalid++
		}
		for _, id := range entry.Categories {
			if !containsString(group.Categories, id) {
				group.Categories = append(group.Categories, id)
			}
		}
	}
	result := make([]SupplementReport, 0, len(order))
	for _, source := range order {
		result = append(result, *bySource[source])
	}
	return result
}

func containsString(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}
