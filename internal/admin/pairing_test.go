package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gateway/internal/release"
)

// Round close lists unpaired requests; the late candidate side only enters
// the release basis after the explicit adopt decision endpoint is called.
func TestRoundCloseAndLateDecisionEndpoints(t *testing.T) {
	handler, stores := newTestAdmin(t, snapshotForAdmin("v2", "new.example"))
	candidate := stores.Stage(snapshotForAdmin("v2", "new.example"))

	stores.BeginPairObservation(candidate.ID, "biz-1", release.PairSideBaseline, "b1", "", "GET", "/v2/echo")
	stores.FinishPairObservation(candidate.ID, "biz-1", release.PairSideBaseline, release.RehearsalOutcome{
		RequestID: "b1", Method: "GET", Path: "/v2/echo", Status: 200,
	})

	closeReq := httptest.NewRequest(http.MethodPost, "/__gateway/candidates/"+candidate.ID+"/rounds/close", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, closeReq)
	if recorder.Code != http.StatusOK {
		t.Fatalf("close status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	var closed release.RoundClose
	if err := json.Unmarshal(recorder.Body.Bytes(), &closed); err != nil {
		t.Fatal(err)
	}
	if closed.Round != 1 || len(closed.Unpaired) != 1 || closed.Unpaired[0].ID != "biz-1" {
		t.Fatalf("closed round = %+v", closed)
	}

	// Late arrival completes the facts but not the release basis.
	stores.FinishPairObservation(candidate.ID, "biz-1", release.PairSideCandidate, release.RehearsalOutcome{
		RequestID: "c1", Method: "GET", Path: "/v2/echo", Status: 200,
	})
	report, _ := stores.Report(candidate.ID)
	if report.Pairing.Overall.PairedValid != 0 {
		t.Fatalf("late arrival entered basis: %+v", report.Pairing.Overall)
	}

	// Deciding on an open round conflicts; adopting the closed round works.
	openDecision := httptest.NewRequest(http.MethodPost,
		"/__gateway/candidates/"+candidate.ID+"/rounds/2/decision", strings.NewReader(`{"adopt": true}`))
	openRecorder := httptest.NewRecorder()
	handler.ServeHTTP(openRecorder, openDecision)
	if openRecorder.Code != http.StatusConflict {
		t.Fatalf("open round decision status = %d", openRecorder.Code)
	}
	decision := httptest.NewRequest(http.MethodPost,
		"/__gateway/candidates/"+candidate.ID+"/rounds/1/decision", strings.NewReader(`{"adopt": true}`))
	decisionRecorder := httptest.NewRecorder()
	handler.ServeHTTP(decisionRecorder, decision)
	if decisionRecorder.Code != http.StatusOK {
		t.Fatalf("decision status = %d body = %s", decisionRecorder.Code, decisionRecorder.Body.String())
	}
	report, _ = stores.Report(candidate.ID)
	if report.Pairing.Overall.PairedValid != 1 || report.Pairing.Rounds[0].Decision != release.RoundDecisionAdopted {
		t.Fatalf("after adopt = %+v", report.Pairing)
	}
}
