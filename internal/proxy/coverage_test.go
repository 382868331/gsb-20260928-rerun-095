package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gateway/internal/admin"
	"gateway/internal/config"
	"gateway/internal/record"
	"gateway/internal/release"
	"gateway/internal/state"
)

func coverageProxySnapshot(t *testing.T, host string) *config.Snapshot {
	cfg := &config.Config{
		Listen:    "127.0.0.1:0",
		AdminPath: "/__gateway",
		Upstreams: []config.Upstream{{ID: "up-" + host, BaseURL: "http://" + host, Timeout: config.Duration(5000000000)}},
		Routes:    []config.Route{{ID: "api", Version: host, Prefix: "/v1/", UpstreamID: "up-" + host, StripPrefix: true, Retry: config.RetryPolicy{MaxAttempts: 1}}},
		Release: config.ReleasePolicy{MinSuccessfulRehearsals: 1, Coverage: &config.CoveragePlan{Categories: []config.CoverageCategory{
			{ID: "reads", Methods: []string{"GET"}, PathPrefixes: []string{"/v1/health", "/v1/echo/"}, MinValidSamples: 1},
			{ID: "pay", Methods: []string{"POST"}, PathPrefixes: []string{"/v1/echo/"}, HighRisk: true, MinValidSamples: 1},
		}}},
	}
	snapshot, err := cfg.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

// End-to-end coverage flow through the proxy and admin API: traffic is
// refused with 409 until the plan is acknowledged, supplement batches are
// declared before top-up traffic, and the final gate uses the evidence set.
func TestCoveragePlanProxyAndAdminFlow(t *testing.T) {
	releases := release.NewStore(coverageProxySnapshot(t, "old.example"))
	records := record.NewStore(100)
	gateway := NewWithTransport(releases, state.NewStore(), records, hostTransport(nil))
	api := admin.New(releases, state.NewStore(), records,
		func() (release.Candidate, error) { return releases.Stage(coverageProxySnapshot(t, "new.example")), nil },
		func(id string) (release.Release, error) { return releases.Confirm(id) },
		func(id string) (release.Release, error) { return releases.Rollback(id) })
	candidate := releases.Stage(coverageProxySnapshot(t, "new.example"))

	// Before acknowledgement, candidate traffic is refused with 409 and
	// never reaches the candidate upstream.
	resp := get(gateway, "/v1/health", map[string]string{release.RehearsalHeader: candidate.ID})
	if resp.Code != http.StatusConflict {
		t.Fatalf("pending plan status = %d", resp.Code)
	}
	ack := httptest.NewRequest(http.MethodPost, "/__gateway/candidates/"+candidate.ID+"/coverage/acknowledge?source=runbook", nil)
	ackRec := httptest.NewRecorder()
	api.ServeHTTP(ackRec, ack)
	if ackRec.Code != http.StatusOK {
		t.Fatalf("acknowledge status = %d body=%s", ackRec.Code, ackRec.Body.String())
	}

	// GET coverage alone is insufficient for the pay category.
	if body := get(gateway, "/v1/health", map[string]string{release.RehearsalHeader: candidate.ID}).Body.String(); body != "new.example" {
		t.Fatalf("rehearsal body = %q", body)
	}
	if _, err := releases.Confirm(candidate.ID); err == nil {
		t.Fatal("candidate confirmed without high-risk pay coverage")
	}

	// Start a supplement batch for the missing category.
	body := strings.NewReader(`{"source":"traffic-replay","category_ids":["pay"]}`)
	start := httptest.NewRequest(http.MethodPost, "/__gateway/candidates/"+candidate.ID+"/supplements", body)
	startRec := httptest.NewRecorder()
	api.ServeHTTP(startRec, start)
	if startRec.Code != http.StatusCreated {
		t.Fatalf("start supplement = %d %s", startRec.Code, startRec.Body.String())
	}
	var batch release.SupplementBatch
	if err := json.Unmarshal(startRec.Body.Bytes(), &batch); err != nil {
		t.Fatal(err)
	}
	post := httptest.NewRequest(http.MethodPost, "/v1/echo/pay", nil)
	post.Header.Set(release.RehearsalHeader, candidate.ID)
	post.Header.Set(release.SupplementHeader, batch.ID)
	postRec := httptest.NewRecorder()
	gateway.ServeHTTP(postRec, post)
	if postRec.Code != http.StatusOK || postRec.Body.String() != "new.example" {
		t.Fatalf("supplement response = %d %q", postRec.Code, postRec.Body.String())
	}

	// Report exposes per-category baseline/candidate differences and results.
	reportReq := httptest.NewRequest(http.MethodGet, "/__gateway/candidates/"+candidate.ID+"/coverage", nil)
	reportRec := httptest.NewRecorder()
	api.ServeHTTP(reportRec, reportReq)
	if reportRec.Code != http.StatusOK {
		t.Fatalf("coverage report = %d", reportRec.Code)
	}
	var envelope struct {
		Report release.CoverageReport `json:"report"`
	}
	if err := json.Unmarshal(reportRec.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if len(envelope.Report.Categories) != 2 {
		t.Fatalf("categories = %d", len(envelope.Report.Categories))
	}
	for _, category := range envelope.Report.Categories {
		if len(category.Representatives) == 0 {
			t.Fatalf("category %s missing baseline/candidate differences", category.Plan.ID)
		}
	}
	if len(envelope.Report.Supplements) != 1 || envelope.Report.Supplements[0].Valid != 1 {
		t.Fatalf("supplements = %+v", envelope.Report.Supplements)
	}
	activated, err := releases.Confirm(candidate.ID)
	if err != nil {
		t.Fatalf("confirm after complete coverage: %v", err)
	}
	if activated.Coverage == nil || activated.Coverage.Overall.Valid != 2 {
		t.Fatalf("release record missing traceable basis: %+v", activated.Coverage)
	}
}
