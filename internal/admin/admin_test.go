package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gateway/internal/config"
	"gateway/internal/record"
	"gateway/internal/release"
	"gateway/internal/state"
)

func snapshotForAdmin(routeID, upstreamHost string) *config.Snapshot {
	cfg := &config.Config{
		Listen:       "127.0.0.1:0",
		AdminPath:    "/__gateway",
		DrainTimeout: config.Duration(time.Second),
		Upstreams:    []config.Upstream{{ID: "up-" + routeID, BaseURL: "http://" + upstreamHost, Timeout: config.Duration(time.Second)}},
		Routes: []config.Route{{
			ID:         routeID,
			Version:    routeID,
			Prefix:     "/" + routeID + "/",
			UpstreamID: "up-" + routeID,
		}},
	}
	snapshot, err := cfg.Snapshot()
	if err != nil {
		panic(err)
	}
	return snapshot
}

func newTestAdmin(t *testing.T, next *config.Snapshot) (*Admin, *release.Store) {
	t.Helper()
	stores := release.NewStore(snapshotForAdmin("v1", "old.example"))
	handler := New(stores, state.NewStore(), record.NewStore(10),
		func() (release.Candidate, error) { return stores.Stage(next), nil },
		stores.Confirm,
		stores.Rollback,
	)
	return handler, stores
}

func TestConfirmFreshCandidateReturnsConflict(t *testing.T) {
	handler, stores := newTestAdmin(t, snapshotForAdmin("v2", "new.example"))
	candidate := stores.Stage(snapshotForAdmin("v2", "new.example"))

	request := httptest.NewRequest(http.MethodPost, "/__gateway/candidates/"+candidate.ID+"/confirm", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("confirm status = %d body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestDiffAndDiscardEndpoints(t *testing.T) {
	handler, stores := newTestAdmin(t, snapshotForAdmin("v2", "new.example"))
	candidate := stores.Stage(snapshotForAdmin("v2", "new.example"))

	request := httptest.NewRequest(http.MethodGet,
		"/__gateway/candidates/"+candidate.ID+"/diff?path=/v1/echo&path=/v2/echo", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("diff status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	var diff release.Diff
	if err := json.Unmarshal(recorder.Body.Bytes(), &diff); err != nil {
		t.Fatal(err)
	}
	if len(diff.Requests) != 2 {
		t.Fatalf("diff requests = %d", len(diff.Requests))
	}
	if !diff.Requests[0].Active.Matched || diff.Requests[0].Candidate.Matched ||
		diff.Requests[1].Active.Matched || !diff.Requests[1].Candidate.Matched {
		t.Fatalf("unexpected sides: active0=%+v cand0=%+v active1=%+v cand1=%+v",
			diff.Requests[0].Active, diff.Requests[0].Candidate,
			diff.Requests[1].Active, diff.Requests[1].Candidate)
	}
	if len(diff.RoutesAdded) != 1 || len(diff.RoutesRemoved) != 1 {
		t.Fatalf("route diff = %+v", diff)
	}

	// Discard abandons the candidate.
	discard := httptest.NewRequest(http.MethodPost,
		"/__gateway/candidates/"+candidate.ID+"/discard", nil)
	discardRecorder := httptest.NewRecorder()
	handler.ServeHTTP(discardRecorder, discard)
	if discardRecorder.Code != http.StatusOK {
		t.Fatalf("discard status = %d", discardRecorder.Code)
	}
	// A second discard conflicts because the candidate is already resolved.
	secondDiscard := httptest.NewRequest(http.MethodPost,
		"/__gateway/candidates/"+candidate.ID+"/discard", nil)
	secondRecorder := httptest.NewRecorder()
	handler.ServeHTTP(secondRecorder, secondDiscard)
	if secondRecorder.Code != http.StatusConflict {
		t.Fatalf("second discard status = %d", secondRecorder.Code)
	}
	// Unknown candidate yields 404.
	unknown := httptest.NewRequest(http.MethodGet, "/__gateway/candidates/cand-99/diff", nil)
	unknownRecorder := httptest.NewRecorder()
	handler.ServeHTTP(unknownRecorder, unknown)
	if unknownRecorder.Code != http.StatusNotFound {
		t.Fatalf("unknown diff status = %d", unknownRecorder.Code)
	}
}
