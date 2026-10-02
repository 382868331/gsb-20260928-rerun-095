package release

import (
	"strings"
	"testing"

	"net/http/httptest"

	"gateway/internal/config"
)

func coverageSnapshot(routeID string, categories []config.CoverageCategory) *config.Snapshot {
	cfg := &config.Config{
		Listen:    "127.0.0.1:0",
		AdminPath: "/__gateway",
		Release: config.ReleasePolicy{
			MinSuccessfulRehearsals: 1,
			CoverageCategories:      categories,
		},
		Upstreams: []config.Upstream{{ID: "up-" + routeID, BaseURL: "http://" + routeID + ".example"}},
		Routes: []config.Route{{
			ID:         routeID,
			Version:    routeID,
			Prefix:     "/v1/",
			UpstreamID: "up-" + routeID,
		}},
	}
	snapshot, err := cfg.Snapshot()
	if err != nil {
		panic(err)
	}
	return snapshot
}

func testCategories() []config.CoverageCategory {
	return []config.CoverageCategory{
		{ID: "reads", Methods: []string{"GET"}, Prefixes: []string{"/v1/"}, MinSamples: 2},
		{ID: "payments", Methods: []string{"POST"}, Prefixes: []string{"/v1/pay"}, MinSamples: 1, HighRisk: true},
	}
}

func rehearse(store *Store, candidateID, requestID, source, method, path string, outcome RehearsalOutcome) {
	request := httptest.NewRequest(method, path, nil)
	request.Header.Set(RehearsalHeader, candidateID)
	selection := store.Select(request)
	outcome.RequestID = requestID
	outcome.Source = source
	outcome.Method = method
	outcome.Path = path
	store.FinishRehearsal(selection.CandidateID, outcome)
}

func TestCoveragePlanMustBeConfirmedBeforeRehearsal(t *testing.T) {
	store := NewStore(coverageSnapshot("v1", nil))
	candidate := store.Stage(coverageSnapshot("v2", testCategories()))
	if candidate.PlanConfirmed {
		t.Fatal("plan should require explicit confirmation")
	}
	// Traffic pinned before plan confirmation falls back to the active release.
	request := httptest.NewRequest("GET", "/v1/echo", nil)
	request.Header.Set(RehearsalHeader, candidate.ID)
	if selection := store.Select(request); selection.CandidateID != "" {
		t.Fatalf("unconfirmed plan still pinned candidate %s", selection.CandidateID)
	}
	if _, err := store.Confirm(candidate.ID); err == nil ||
		!strings.Contains(err.Error(), "coverage plan not confirmed") {
		t.Fatalf("confirm before plan confirmation: %v", err)
	}
	if _, err := store.ConfirmPlan(candidate.ID); err != nil {
		t.Fatal(err)
	}
	if selection := store.Select(request); selection.CandidateID != candidate.ID {
		t.Fatalf("after plan confirmation selected %s", selection.CandidateID)
	}
}

func TestCategoryCoverageAndOverallDedup(t *testing.T) {
	store := NewStore(coverageSnapshot("v1", nil))
	candidate := store.Stage(coverageSnapshot("v2", testCategories()))
	if _, err := store.ConfirmPlan(candidate.ID); err != nil {
		t.Fatal(err)
	}
	// One GET matches only "reads"; two samples needed.
	rehearse(store, candidate.ID, "r1", "", "GET", "/v1/echo", RehearsalOutcome{Status: 200})
	rehearse(store, candidate.ID, "r2", "", "GET", "/v1/other", RehearsalOutcome{Status: 200})
	// A failed POST in the high-risk category never counts as valid coverage.
	rehearse(store, candidate.ID, "r3", "", "POST", "/v1/pay", RehearsalOutcome{Status: 502, Error: "bad gateway"})
	// Duplicate request id must not inflate coverage.
	store.FinishRehearsal(candidate.ID, RehearsalOutcome{RequestID: "r2", Method: "GET", Path: "/v1/other", Status: 200})

	report, err := store.Report(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if report.Overall.Succeeded != 2 || report.Overall.Failed != 1 {
		t.Fatalf("overall = %+v", report.Overall)
	}
	reads := report.Categories[0]
	if reads.ID != "reads" || reads.Valid != 2 || !reads.Satisfied {
		t.Fatalf("reads category = %+v", reads)
	}
	payments := report.Categories[1]
	if payments.Valid != 0 || payments.Missing != 1 || payments.Failed != 1 || payments.Satisfied {
		t.Fatalf("payments category = %+v", payments)
	}
	if len(payments.Reasons) != 2 {
		t.Fatalf("payments reasons = %v", payments.Reasons)
	}
	if payments.Diff == nil || payments.Diff.Active == nil || payments.Diff.Candidate == nil {
		t.Fatal("category diff missing baseline/candidate comparison")
	}
	// High-risk failure cannot be masked by the satisfied overall average.
	if _, err := store.Confirm(candidate.ID); err == nil || !strings.Contains(err.Error(), "payments") {
		t.Fatalf("confirm with high-risk failure: %v", err)
	}
}

func TestSupplementCoversMissingCategoryAndIsTraceable(t *testing.T) {
	store := NewStore(coverageSnapshot("v1", nil))
	candidate := store.Stage(coverageSnapshot("v2", testCategories()))
	if _, err := store.ConfirmPlan(candidate.ID); err != nil {
		t.Fatal(err)
	}
	rehearse(store, candidate.ID, "r1", "", "GET", "/v1/a", RehearsalOutcome{Status: 200})
	rehearse(store, candidate.ID, "r2", "", "GET", "/v1/b", RehearsalOutcome{Status: 200})
	// Supplement the missing high-risk category with a named batch.
	rehearse(store, candidate.ID, "s1", "topup-1", "POST", "/v1/pay", RehearsalOutcome{Status: 200})

	report, err := store.Report(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Supplements) != 2 {
		t.Fatalf("supplements = %+v", report.Supplements)
	}
	topup := report.Supplements[1]
	if topup.Source != "topup-1" || topup.Succeeded != 1 || len(topup.Categories) != 1 || topup.Categories[0] != "payments" {
		t.Fatalf("supplement batch = %+v", topup)
	}
	for _, category := range report.Categories {
		if !category.Satisfied {
			t.Fatalf("category %s unsatisfied: %v", category.ID, category.Reasons)
		}
	}
	promoted, err := store.Confirm(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(promoted.Coverage) != 2 {
		t.Fatalf("release coverage evidence = %+v", promoted.Coverage)
	}
	if len(report.Evidence) != 3 {
		t.Fatalf("evidence entries = %d", len(report.Evidence))
	}
}

func TestInvalidAndCanceledRequestsNeverCountAsCoverage(t *testing.T) {
	store := NewStore(coverageSnapshot("v1", nil))
	candidate := store.Stage(coverageSnapshot("v2", testCategories()))
	if _, err := store.ConfirmPlan(candidate.ID); err != nil {
		t.Fatal(err)
	}
	rehearse(store, candidate.ID, "r1", "", "GET", "/nowhere", RehearsalOutcome{Status: 404, Error: "no route matched"})
	rehearse(store, candidate.ID, "r2", "", "GET", "/v1/a", RehearsalOutcome{Status: 499, Canceled: true})
	listed := store.Candidates()[0]
	if listed.Rehearsal.Invalid != 1 || listed.Rehearsal.Canceled != 1 || listed.Rehearsal.Succeeded != 0 {
		t.Fatalf("summary = %+v", listed.Rehearsal)
	}
	if got := listed.Coverage["reads"].Valid; got != 0 {
		t.Fatalf("canceled request counted as valid coverage: %d", got)
	}
}

func TestChangingCoverageRulesInvalidatesOldEvidence(t *testing.T) {
	store := NewStore(coverageSnapshot("v1", nil))
	first := store.Stage(coverageSnapshot("v2", testCategories()))
	if _, err := store.ConfirmPlan(first.ID); err != nil {
		t.Fatal(err)
	}
	rehearse(store, first.ID, "r1", "", "GET", "/v1/a", RehearsalOutcome{Status: 200})
	// Adjusting coverage requirements supersedes the staged revision.
	changed := testCategories()
	changed[0].MinSamples = 3
	second := store.Stage(coverageSnapshot("v2", changed))
	if second.ID == first.ID {
		t.Fatal("changed coverage plan reused the old candidate")
	}
	report, err := store.Report(first.ID)
	if err != nil {
		t.Fatal("old report must stay viewable")
	}
	if report.EvidenceValid || !strings.Contains(report.InvalidatedReason, second.ID) {
		t.Fatalf("invalidation not reported: %+v", report)
	}
	if len(report.Evidence) != 1 {
		t.Fatalf("old evidence lost: %+v", report.Evidence)
	}
	if _, err := store.Confirm(first.ID); err == nil {
		t.Fatal("superseded candidate confirmed")
	}
}
