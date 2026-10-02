package release

import (
	"net/http/httptest"
	"strings"
	"testing"

	"gateway/internal/config"
)

func coverageSnapshot(t *testing.T, host string) *config.Snapshot {
	t.Helper()
	cfg := &config.Config{
		Listen:    "127.0.0.1:0",
		AdminPath: "/__gateway",
		Upstreams: []config.Upstream{{ID: "up-" + host, BaseURL: "http://" + host}},
		Routes:    []config.Route{{ID: "api", Version: host, Prefix: "/v1/", UpstreamID: "up-" + host}},
		Release: config.ReleasePolicy{
			MinSuccessfulRehearsals: 1,
			Coverage: &config.CoveragePlan{Categories: []config.CoverageCategory{
				{ID: "read", Methods: []string{"GET"}, PathPrefixes: []string{"/v1/health", "/v1/echo/"}, MinValidSamples: 2},
				{ID: "writes", Methods: []string{"POST"}, PathPrefixes: []string{"/v1/echo/"}, HighRisk: true, MinValidSamples: 1},
				{ID: "flagged", HeaderName: "X-Biz", HeaderValue: "pay", HighRisk: true, MinValidSamples: 1},
			}},
		},
	}
	snapshot, err := cfg.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func ackAndRehearse(t *testing.T, store *Store, candidateID string) {
	t.Helper()
	if _, err := store.AcknowledgePlan(candidateID, "ops-runbook"); err != nil {
		t.Fatal(err)
	}
}

func success(store *Store, candidateID, method, path string, headers map[string]string) {
	request := httptest.NewRequest(method, path, nil)
	request.Header.Set(RehearsalHeader, candidateID)
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	selection := store.Select(request)
	if selection.PlanPending {
		panic("plan still pending")
	}
	store.FinishRehearsal(selection.CandidateID, RehearsalOutcome{
		Status: 200, Method: method, Path: path, Header: request.Header,
		RequestID: path + "-" + method, SampleID: request.Header.Get(SampleIDHeader),
		Supplement: request.Header.Get(SupplementHeader),
	})
}

func finishOutcome(store *Store, candidateID, method, path string, outcome RehearsalOutcome) {
	request := httptest.NewRequest(method, path, nil)
	request.Header.Set(RehearsalHeader, candidateID)
	selection := store.Select(request)
	outcome.Method = method
	outcome.Path = path
	outcome.Header = request.Header
	outcome.RequestID = path + "-" + method + "-fail"
	store.FinishRehearsal(selection.CandidateID, outcome)
}

// Rehearsal traffic must be refused until the coverage plan is acknowledged.
func TestRehearsalRequiresPlanAcknowledgement(t *testing.T) {
	store := NewStore(coverageSnapshot(t, "old"))
	candidate := store.Stage(coverageSnapshot(t, "new"))
	if state := store.Candidates()[0].Readiness; state.State != StatePlanPending {
		t.Fatalf("fresh planned candidate state = %s", state.State)
	}
	request := httptest.NewRequest("GET", "/v1/health", nil)
	request.Header.Set(RehearsalHeader, candidate.ID)
	selection := store.Select(request)
	if !selection.PlanPending || selection.CandidateID != candidate.ID {
		t.Fatalf("unacknowledged selection = %+v", selection)
	}
	if _, err := store.Confirm(candidate.ID); err == nil || !strings.Contains(err.Error(), "acknowledged") {
		t.Fatalf("confirm without acknowledgement: %v", err)
	}
	ackAndRehearse(t, store, candidate.ID)
	success(store, candidate.ID, "GET", "/v1/health", nil)
	success(store, candidate.ID, "GET", "/v1/echo/x", nil)
	success(store, candidate.ID, "POST", "/v1/echo/x", nil)
	success(store, candidate.ID, "POST", "/v1/echo/pay", map[string]string{"X-Biz": "pay"})
	activated, err := store.Confirm(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if activated.Coverage == nil || len(activated.Coverage.Evidence) != 4 {
		t.Fatalf("promote record lost traceable coverage basis: %+v", activated.Coverage)
	}
}

// One request can belong to several categories but counts once overall; the
// same sample replayed is a duplicate and never inflates coverage.
func TestMultiCategoryAndDeduplication(t *testing.T) {
	store := NewStore(coverageSnapshot(t, "old"))
	candidate := store.Stage(coverageSnapshot(t, "new"))
	ackAndRehearse(t, store, candidate.ID)

	header := map[string]string{SampleIDHeader: "sample-1", "X-Biz": "pay"}
	success(store, candidate.ID, "GET", "/v1/health", header)
	// Same stable sample id again: duplicate, must not add coverage anywhere.
	success(store, candidate.ID, "GET", "/v1/echo/x", header)
	success(store, candidate.ID, "GET", "/v1/echo/y", nil)
	success(store, candidate.ID, "POST", "/v1/echo/z", map[string]string{"X-Biz": "pay"})
	success(store, candidate.ID, "GET", "/v1/health", map[string]string{"X-Biz": "pay"})

	report, _, err := store.CoverageReport(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if report.Overall.DistinctSamples != 4 || report.Overall.Duplicates != 1 || report.Overall.Valid != 4 {
		t.Fatalf("overall totals = %+v", report.Overall)
	}
	byID := map[string]CategoryReport{}
	for _, category := range report.Categories {
		byID[category.Plan.ID] = category
	}
	// sample-1 matched read+flagged (GET /v1/health, X-Biz=pay).
	if byID["read"].Valid != 3 {
		t.Fatalf("read valid = %d", byID["read"].Valid)
	}
	if byID["flagged"].Valid != 3 {
		t.Fatalf("flagged valid = %d", byID["flagged"].Valid)
	}
	if byID["writes"].Valid != 1 {
		t.Fatalf("writes valid = %d", byID["writes"].Valid)
	}
}

// Failed, canceled and aborted requests are recorded per category but never
// count as valid coverage; high-risk categories cannot be rescued by healthy
// averages elsewhere.
func TestInvalidOutcomesAndHighRiskGate(t *testing.T) {
	store := NewStore(coverageSnapshot(t, "old"))
	candidate := store.Stage(coverageSnapshot(t, "new"))
	ackAndRehearse(t, store, candidate.ID)
	success(store, candidate.ID, "GET", "/v1/health", nil)
	success(store, candidate.ID, "GET", "/v1/echo/x", nil)
	success(store, candidate.ID, "POST", "/v1/echo/x", nil)
	// A third read sample is canceled: it is evidence, but never valid
	// coverage, so read still has exactly its minimum of two valid samples.
	finishOutcome(store, candidate.ID, "GET", "/v1/echo/a", RehearsalOutcome{Canceled: true, Status: 499, Error: "client canceled"})
	success(store, candidate.ID, "GET", "/v1/echo/pay", map[string]string{"X-Biz": "pay"})

	report, _, _ := store.CoverageReport(candidate.ID)
	var read CategoryReport
	for _, category := range report.Categories {
		if category.Plan.ID == "read" {
			read = category
		}
	}
	if read.Canceled != 1 {
		t.Fatalf("canceled read sample was counted as valid coverage: %+v", read)
	}

	// One high-risk failure keeps the gate closed even with overall success
	// and even though every minimum sample count is met. The failing request
	// belongs only to flagged (header match, outside the write prefix).
	request := httptest.NewRequest("GET", "/v1/other", nil)
	request.Header.Set("X-Biz", "pay")
	request.Header.Set(RehearsalHeader, candidate.ID)
	selection := store.Select(request)
	store.FinishRehearsal(selection.CandidateID, RehearsalOutcome{
		Status: 502, Error: "bad gateway", Method: "GET", Path: "/v1/other",
		Header: request.Header, RequestID: "flagged-fail",
	})
	if _, err := store.Confirm(candidate.ID); err == nil || !strings.Contains(err.Error(), "high-risk") {
		t.Fatalf("high-risk failure hidden by average: %v", err)
	}
	report, _, _ = store.CoverageReport(candidate.ID)
	for _, category := range report.Categories {
		if category.Plan.ID != "flagged" {
			continue
		}
		if category.Satisfied || !strings.Contains(category.Reasons[0], "high-risk") {
			t.Fatalf("writes reasons = %+v", category.Reasons)
		}
	}
}

// Supplement batches fill uncovered categories and retain source/results;
// unknown batch ids cannot drive candidate traffic.
func TestSupplementBatchTraceability(t *testing.T) {
	store := NewStore(coverageSnapshot(t, "old"))
	candidate := store.Stage(coverageSnapshot(t, "new"))
	ackAndRehearse(t, store, candidate.ID)
	batch, err := store.StartSupplement(candidate.ID, "traffic-replay", []string{"writes"})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("POST", "/v1/echo/pay", nil)
	request.Header.Set(RehearsalHeader, candidate.ID)
	request.Header.Set(SupplementHeader, batch.ID)
	selection := store.Select(request)
	if selection.ID != candidate.ID {
		t.Fatalf("supplement served by %s", selection.ID)
	}
	store.FinishRehearsal(candidate.ID, RehearsalOutcome{
		Status: 200, Method: "POST", Path: "/v1/echo/pay", Header: request.Header,
		RequestID: "sup-1", Supplement: batch.ID,
	})
	if _, err := store.EndSupplement(batch.ID); err != nil {
		t.Fatal(err)
	}
	// Closed batch must fall back to active release, not serve the candidate.
	request2 := httptest.NewRequest("POST", "/v1/echo/pay2", nil)
	request2.Header.Set(RehearsalHeader, candidate.ID)
	request2.Header.Set(SupplementHeader, batch.ID)
	if selection := store.Select(request2); selection.ID == candidate.ID {
		t.Fatal("closed supplement still served candidate")
	}
	report, _, _ := store.CoverageReport(candidate.ID)
	if len(report.Supplements) != 1 || report.Supplements[0].Source != "traffic-replay" || report.Supplements[0].Valid != 1 {
		t.Fatalf("supplement record = %+v", report.Supplements)
	}
	if report.Evidence[0].Source != "traffic-replay" {
		t.Fatalf("evidence lost supplement source: %+v", report.Evidence[0])
	}
}

// Changing coverage rules supersedes the candidate; the old report stays
// viewable and is explicitly flagged as an invalid release basis.
func TestCoverageRuleChangeInvalidatesBasis(t *testing.T) {
	store := NewStore(coverageSnapshot(t, "old"))
	first := store.Stage(coverageSnapshot(t, "new"))
	ackAndRehearse(t, store, first.ID)
	success(store, first.ID, "GET", "/v1/health", nil)

	revised := coverageSnapshot(t, "new")
	plan := revised.CoveragePlan()
	categories := make([]config.CoverageCategory, len(plan.Categories))
	copy(categories, plan.Categories)
	categories[0].MinValidSamples = 5
	revisedCfg := &config.Config{
		Listen: "127.0.0.1:0", AdminPath: "/__gateway",
		Upstreams: revised.Upstreams(), Routes: revised.Routes(),
		Release: config.ReleasePolicy{MinSuccessfulRehearsals: 1, Coverage: &config.CoveragePlan{Categories: categories}},
	}
	newSnapshot, err := revisedCfg.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	second := store.Stage(newSnapshot)

	report, old, err := store.CoverageReport(first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Invalid || !strings.Contains(report.InvalidReason, "minimum sample") {
		t.Fatalf("old report not flagged invalid: %+v", report)
	}
	if old.SupersededBy != second.ID {
		t.Fatalf("superseded by = %q want %s", old.SupersededBy, second.ID)
	}
	if _, err := store.Confirm(first.ID); err == nil {
		t.Fatal("invalid basis was confirmed")
	}
}
