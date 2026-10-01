package admin

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gateway/internal/config"
	"gateway/internal/proxy"
	"gateway/internal/record"
	"gateway/internal/release"
	"gateway/internal/state"
)

func testSnapshot(host, version, prefix string, minRehearsals int) *config.Snapshot {
	cfg := &config.Config{Listen: "127.0.0.1:0", AdminPath: "/__gateway", DrainTimeout: config.Duration(time.Second), Release: config.ReleasePolicy{MinSuccessfulRehearsals: minRehearsals},
		Upstreams: []config.Upstream{{ID: "up", BaseURL: "http://" + host, Timeout: config.Duration(5 * time.Second)}},
		Routes:    []config.Route{{ID: "api", Version: version, Prefix: prefix, UpstreamID: "up", StripPrefix: true}},
	}
	snapshot, err := cfg.Snapshot()
	if err != nil {
		panic(err)
	}
	return snapshot
}

type harness struct {
	admin    *Admin
	releases *release.Store
	proxyH   http.Handler
	next     *config.Snapshot
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	releases := release.NewStore(testSnapshot("old.example", "v1", "/v1/", 1))
	records := record.NewStore(100)
	transport := testRoundTrip(func(request *http.Request) (*http.Response, error) {
		return testResponse(http.StatusOK, request.URL.Host), nil
	})
	proxyH := proxy.NewWithTransport(releases, state.NewStore(), records, transport)
	h := &harness{releases: releases, proxyH: proxyH, next: testSnapshot("new.example", "v2", "/v2/", 1)}
	stage := func() (release.Candidate, error) { return releases.Stage(h.next), nil }
	revise := func(id string) (release.Candidate, bool, error) { return releases.Revise(id, h.next) }
	confirm := func(id string) (release.Release, error) { return releases.Confirm(id) }
	discard := func(id string) (release.CandidateRecord, error) { return releases.Discard(id) }
	rollback := func(target string) (release.Release, error) { return releases.Rollback(target) }
	h.admin = New(releases, state.NewStore(), records, stage, revise, confirm, discard, rollback, releases.DiffSnapshot, releases.DiffRequest)
	return h
}

func (h *harness) request(method, target string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, target, nil)
	recorder := httptest.NewRecorder()
	h.admin.ServeHTTP(recorder, request)
	return recorder
}

func (h *harness) rehearsalAt(candidateID, path string) {
	request := httptest.NewRequest(http.MethodGet, path, nil)
	request.Header.Set(release.RehearsalHeader, candidateID)
	h.proxyH.ServeHTTP(httptest.NewRecorder(), request)
}

func (h *harness) rehearsal(candidateID string) { h.rehearsalAt(candidateID, "/v2/echo") }

func TestConfirmBlockedUntilRehearsalSatisfied(t *testing.T) {
	h := newHarness(t)
	stage := h.request(http.MethodPost, "/__gateway/candidates")
	if stage.Code != http.StatusCreated {
		t.Fatalf("stage = %d %s", stage.Code, stage.Body.String())
	}
	var candidate release.Candidate
	if err := json.Unmarshal(stage.Body.Bytes(), &candidate); err != nil {
		t.Fatal(err)
	}
	if candidate.Gate != release.GateNotRehearsed {
		t.Fatalf("fresh gate = %s", candidate.Gate)
	}
	blocked := h.request(http.MethodPost, "/__gateway/candidates/"+candidate.ID+"/confirm")
	if blocked.Code != http.StatusConflict || !strings.Contains(blocked.Body.String(), "not been rehearsed") {
		t.Fatalf("confirm = %d %s", blocked.Code, blocked.Body.String())
	}
	h.rehearsal(candidate.ID)
	listed := h.request(http.MethodGet, "/__gateway/candidates")
	var view struct {
		Candidates []release.Candidate `json:"candidates"`
	}
	if err := json.Unmarshal(listed.Body.Bytes(), &view); err != nil {
		t.Fatal(err)
	}
	if view.Candidates[0].Gate != release.GateReady || view.Candidates[0].Rehearsal.Succeeded != 1 {
		t.Fatalf("gate after rehearsal = %+v", view.Candidates[0])
	}
	confirmed := h.request(http.MethodPost, "/__gateway/candidates/"+candidate.ID+"/confirm")
	if confirmed.Code != http.StatusOK {
		t.Fatalf("confirm = %d %s", confirmed.Code, confirmed.Body.String())
	}
	releases := h.request(http.MethodGet, "/__gateway/releases")
	if !strings.Contains(releases.Body.String(), "rehearsal") || !strings.Contains(releases.Body.String(), candidate.ID) {
		t.Fatalf("release history missing evidence: %s", releases.Body.String())
	}
}

func TestReviseInvalidatesEvidenceAndDiffShowsRequestChange(t *testing.T) {
	h := newHarness(t)
	var candidate release.Candidate
	json.Unmarshal(h.request(http.MethodPost, "/__gateway/candidates").Body.Bytes(), &candidate)
	h.rehearsal(candidate.ID)
	if h.request(http.MethodPost, "/__gateway/candidates/"+candidate.ID+"/confirm").Code != http.StatusOK {
		t.Fatal("first candidate should confirm")
	}
	h.next = testSnapshot("other.example", "v3", "/v3/", 1)
	var second release.Candidate
	json.Unmarshal(h.request(http.MethodPost, "/__gateway/candidates").Body.Bytes(), &second)
	h.rehearsalAt(second.ID, "/v3/echo")
	diff := h.request(http.MethodPost, "/__gateway/candidates/"+second.ID+"/diff?path=/v3/echo")
	if diff.Code != http.StatusOK || !strings.Contains(diff.Body.String(), "\"same_decision\":false") {
		t.Fatalf("request diff = %d %s", diff.Code, diff.Body.String())
	}
	h.next = testSnapshot("third.example", "v4", "/v4/", 1)
	revised := h.request(http.MethodPost, "/__gateway/candidates/"+second.ID+"/revise")
	if revised.Code != http.StatusCreated || !strings.Contains(revised.Body.String(), "\"evidence_invalidated\":true") {
		t.Fatalf("revise = %d %s", revised.Code, revised.Body.String())
	}
	blocked := h.request(http.MethodPost, "/__gateway/candidates/"+second.ID+"/confirm")
	if blocked.Code != http.StatusConflict || !strings.Contains(blocked.Body.String(), "not been rehearsed") {
		t.Fatalf("confirm after revise = %d %s", blocked.Code, blocked.Body.String())
	}
	fullDiff := h.request(http.MethodPost, "/__gateway/candidates/"+second.ID+"/diff")
	if !strings.Contains(fullDiff.Body.String(), "changed_routes") {
		t.Fatalf("full diff missing changed routes: %s", fullDiff.Body.String())
	}
}

func TestDiscardKeepsArchive(t *testing.T) {
	h := newHarness(t)
	var candidate release.Candidate
	json.Unmarshal(h.request(http.MethodPost, "/__gateway/candidates").Body.Bytes(), &candidate)
	discarded := h.request(http.MethodPost, "/__gateway/candidates/"+candidate.ID+"/discard")
	if discarded.Code != http.StatusOK {
		t.Fatalf("discard = %d %s", discarded.Code, discarded.Body.String())
	}
	unknown := h.request(http.MethodPost, "/__gateway/candidates/"+candidate.ID+"/confirm")
	if unknown.Code != http.StatusNotFound {
		t.Fatalf("confirm discarded = %d", unknown.Code)
	}
	listed := h.request(http.MethodGet, "/__gateway/candidates")
	if !strings.Contains(listed.Body.String(), "discarded") {
		t.Fatalf("archive missing discarded record: %s", listed.Body.String())
	}
}

type testRoundTrip func(*http.Request) (*http.Response, error)

func (f testRoundTrip) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func testResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body))}
}
