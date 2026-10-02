package release

import (
	"fmt"
	"net/http"
	"sort"
	"time"

	"gateway/internal/config"
)

// SupplementHeader marks rehearsal traffic that belongs to a declared
// supplemental evidence batch used to fill categories still uncovered.
const SupplementHeader = "X-Gateway-Supplement"

// SampleIDHeader optionally carries a stable sample identifier supplied by
// the caller. Repeated rehearsal submissions with the same sample id are
// deduplicated so evidence cannot be inflated by replaying one request.
const SampleIDHeader = "X-Gateway-Sample-ID"

// EvidenceOutcome is how a rehearsal sample ended.
type EvidenceOutcome string

const (
	OutcomeValid    EvidenceOutcome = "valid"
	OutcomeFailed   EvidenceOutcome = "failed"
	OutcomeCanceled EvidenceOutcome = "canceled"
	OutcomeAborted  EvidenceOutcome = "aborted"
)

// Evidence is one terminal rehearsal request, retained as auditable proof.
// One evidence record belongs to every category it matches, but is counted
// only once in the overall totals.
type Evidence struct {
	RequestID    string          `json:"request_id"`
	SampleID     string          `json:"sample_id,omitempty"`
	Method       string          `json:"method"`
	Path         string          `json:"path"`
	Status       int             `json:"status"`
	Outcome      EvidenceOutcome `json:"outcome"`
	Valid        bool            `json:"valid"`
	Duplicate    bool            `json:"duplicate,omitempty"`
	Categories   []string        `json:"categories"`
	SupplementID string          `json:"supplement_id,omitempty"`
	Source       string          `json:"source,omitempty"`
	RecordedAt   time.Time       `json:"recorded_at"`
}

// SupplementBatch records one user-declared top-up of still-uncovered
// categories, including where the extra traffic came from and how it ended.
type SupplementBatch struct {
	ID          string    `json:"id"`
	CreatedAt   time.Time `json:"created_at"`
	Source      string    `json:"source"`
	CategoryIDs []string  `json:"category_ids"`
	Valid       int64     `json:"valid"`
	Failed      int64     `json:"failed"`
	Canceled    int64     `json:"canceled"`
	Aborted     int64     `json:"aborted"`
	Duplicates  int64     `json:"duplicates"`
}

// CategoryView is the plan definition for one business category.
type CategoryView struct {
	ID              string   `json:"id"`
	Description     string   `json:"description,omitempty"`
	Methods         []string `json:"methods,omitempty"`
	PathPrefixes    []string `json:"path_prefixes,omitempty"`
	HeaderName      string   `json:"header_name,omitempty"`
	HeaderValue     string   `json:"header_value,omitempty"`
	HighRisk        bool     `json:"high_risk"`
	MinValidSamples int      `json:"min_valid_samples"`
}

// CategoryReport is the coverage result of one business category. It reports
// baseline/candidate routing differences for representative requests and the
// concrete reason the category is or is not satisfied.
type CategoryReport struct {
	Plan             CategoryView  `json:"plan"`
	Matched          int64         `json:"matched"`
	Valid            int64         `json:"valid"`
	Failed           int64         `json:"failed"`
	Canceled         int64         `json:"canceled"`
	Aborted          int64         `json:"aborted"`
	MissingSamples   int64         `json:"missing_samples"`
	Satisfied        bool          `json:"satisfied"`
	Reasons          []string      `json:"reasons"`
	BaselineMatches  []MatchView   `json:"baseline_matches"`
	CandidateMatches []MatchView   `json:"candidate_matches"`
	Representatives  []RequestDiff `json:"representatives"`
}

// OverallCoverage separates the per-category views from the global
// deduplicated totals: a request matching several categories is counted once
// here.
type OverallCoverage struct {
	DistinctSamples int64 `json:"distinct_samples"`
	Valid           int64 `json:"valid"`
	Failed          int64 `json:"failed"`
	Canceled        int64 `json:"canceled"`
	Aborted         int64 `json:"aborted"`
	Duplicates      int64 `json:"duplicates_ignored"`
	InFlight        int64 `json:"in_flight"`
}

// CoverageReport is the full per-category rehearsal report attached to a
// candidate. It is always viewable, even after the supporting evidence was
// invalidated by a revision or a coverage-rule change.
type CoverageReport struct {
	Required           bool              `json:"required"`
	Acknowledged       bool              `json:"acknowledged"`
	AcknowledgedAt     time.Time         `json:"acknowledged_at,omitempty"`
	AcknowledgedSource string            `json:"acknowledged_source,omitempty"`
	Plan               []CategoryView    `json:"plan"`
	Overall            OverallCoverage   `json:"overall"`
	Categories         []CategoryReport  `json:"categories"`
	Supplements        []SupplementBatch `json:"supplements"`
	Evidence           []Evidence        `json:"evidence"`
	Invalid            bool              `json:"invalid,omitempty"`
	InvalidReason      string            `json:"invalid_reason,omitempty"`
}

// CoverageBasis is the frozen, traceable set of evidence embedded into a
// promote release record. Cherry-picking favorable results is impossible
// because the gate evaluates this exact set.
type CoverageBasis struct {
	Overall  OverallCoverage  `json:"overall"`
	Plan     []CategoryView   `json:"plan"`
	Category []CategoryReport `json:"categories"`
	Evidence []Evidence       `json:"evidence"`
}

func categoryViews(plan *config.CoveragePlan) []CategoryView {
	if plan == nil {
		return []CategoryView{}
	}
	views := make([]CategoryView, 0, len(plan.Categories))
	for _, category := range plan.Categories {
		views = append(views, CategoryView{
			ID:              category.ID,
			Description:     category.Description,
			Methods:         append([]string(nil), category.Methods...),
			PathPrefixes:    append([]string(nil), category.PathPrefixes...),
			HeaderName:      category.HeaderName,
			HeaderValue:     category.HeaderValue,
			HighRisk:        category.HighRisk,
			MinValidSamples: category.MinValidSamples,
		})
	}
	return views
}

func matchCategories(plan *config.CoveragePlan, method, path string, headers http.Header) []string {
	if plan == nil {
		return nil
	}
	var categories []string
	for _, category := range plan.Categories {
		if category.Matches(method, path, headers) {
			categories = append(categories, category.ID)
		}
	}
	sort.Strings(categories)
	return categories
}

func categoryByID(plan *config.CoveragePlan, id string) *config.CoverageCategory {
	for i := range plan.Categories {
		if plan.Categories[i].ID == id {
			return &plan.Categories[i]
		}
	}
	return nil
}

// representativeRequests builds one representative GET (or the first allowed
// method) per category path prefix so baseline/candidate routing can be
// compared per category.
func representativeRequests(plan *config.CoveragePlan) []*http.Request {
	seen := map[string]struct{}{}
	var requests []*http.Request
	for _, category := range plan.Categories {
		method := http.MethodGet
		if len(category.Methods) > 0 {
			method = category.Methods[0]
		}
		for _, prefix := range category.PathPrefixes {
			key := method + " " + prefix
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			request, _ := http.NewRequest(method, prefix, nil)
			requests = append(requests, request)
		}
	}
	return requests
}

func missingReason(category config.CoverageCategory, report *CategoryReport) []string {
	var reasons []string
	if report.Valid < int64(category.MinValidSamples) {
		reasons = append(reasons, fmt.Sprintf("only %d/%d distinct valid sample(s)", report.Valid, category.MinValidSamples))
	}
	if category.HighRisk {
		if report.Failed > 0 {
			reasons = append(reasons, fmt.Sprintf("high-risk category has %d failed sample(s); the overall average cannot cover this", report.Failed))
		}
		if report.Canceled > 0 {
			reasons = append(reasons, fmt.Sprintf("high-risk category has %d canceled sample(s); the overall average cannot cover this", report.Canceled))
		}
		if report.Aborted > 0 {
			reasons = append(reasons, fmt.Sprintf("high-risk category has %d aborted sample(s); the overall average cannot cover this", report.Aborted))
		}
	}
	return reasons
}
