package release

import (
	"errors"
	"net/http/httptest"
	"testing"

	"gateway/internal/config"
)

func testSnapshot(routeID string) *config.Snapshot {
	cfg := &config.Config{
		Listen:    "127.0.0.1:0",
		AdminPath: "/__gateway",
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

func TestStageConfirmRollbackKeepsAuditTrail(t *testing.T) {
	store := NewStore(testSnapshot("v1"))
	if got := store.CurrentID(); got != "rel-1" {
		t.Fatalf("initial release = %s", got)
	}

	candidate := store.Stage(testSnapshot("v2"))
	if candidate.ID != "cand-1" {
		t.Fatalf("candidate id = %s", candidate.ID)
	}
	if got := store.CurrentID(); got != "rel-1" {
		t.Fatalf("staging changed active release to %s", got)
	}

	// Rehearsal traffic pinned to the candidate uses its snapshot.
	rehearsal := httptest.NewRequest("GET", "/v1/echo", nil)
	rehearsal.Header.Set(RehearsalHeader, candidate.ID)
	if binding := store.Select(rehearsal); binding.ID != candidate.ID {
		t.Fatalf("rehearsal selected %s", binding.ID)
	}
	plain := httptest.NewRequest("GET", "/v1/echo", nil)
	if binding := store.Select(plain); binding.ID != "rel-1" {
		t.Fatalf("normal traffic selected %s", binding.ID)
	}
	if got := store.Candidates()[0].Rehearsal.InFlight; got != 1 {
		t.Fatalf("in-flight rehearsals = %d", got)
	}
	if got := store.Candidates()[0].Gate; got != GateInProgress {
		t.Fatalf("gate before outcome = %s", got)
	}
	// No candidate can be confirmed without completed, successful rehearsal.
	if _, err := store.Confirm(candidate.ID); err == nil {
		t.Fatal("confirm while rehearsal is in flight succeeded")
	}
	store.FinishRehearsal(candidate.ID, 1, RehearsalOutcome{Status: 200, Success: true})
	if got := store.Candidates()[0].Gate; got != GateReady {
		t.Fatalf("gate after success = %s", got)
	}

	promoted, err := store.Confirm(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if promoted.Kind != KindPromote || promoted.SourceID != candidate.ID {
		t.Fatalf("promote record = %+v", promoted)
	}
	if got := store.CurrentID(); got != promoted.ID {
		t.Fatalf("active release = %s", got)
	}
	if len(store.Candidates()) != 0 {
		t.Fatal("confirmed candidate still staged")
	}

	// Rollback creates a new release record instead of erasing the failed one.
	rolledBack, err := store.Rollback("")
	if err != nil {
		t.Fatal(err)
	}
	if rolledBack.Kind != KindRollback || rolledBack.SourceID != "rel-1" {
		t.Fatalf("rollback record = %+v", rolledBack)
	}
	history := store.History()
	if len(history) != 3 {
		t.Fatalf("history length = %d", len(history))
	}
	if history[1].ID != promoted.ID {
		t.Fatal("failed release was erased from history")
	}
	if binding := store.Select(plain); binding.ID != rolledBack.ID {
		t.Fatalf("after rollback selected %s", binding.ID)
	}
}

func TestConfirmRejectsUnrehearsedAndFailedCandidates(t *testing.T) {
	store := NewStore(testSnapshot("v1"))
	candidate := store.Stage(testSnapshot("v2"))

	// Fresh candidate cannot be confirmed with zero rehearsal.
	_, err := store.Confirm(candidate.ID)
	var gateError *GateError
	if !errors.As(err, &gateError) || gateError.Status != GateNotRehearsed {
		t.Fatalf("fresh confirm error = %v", err)
	}

	// A rehearsal that finishes with a failure does not satisfy the gate.
	request := httptest.NewRequest("GET", "/v1/echo", nil)
	request.Header.Set(RehearsalHeader, candidate.ID)
	store.Select(request)
	store.FinishRehearsal(candidate.ID, 1, RehearsalOutcome{Status: 503, Success: false})
	_, err = store.Confirm(candidate.ID)
	if !errors.As(err, &gateError) || gateError.Status != GateIncompleteResults {
		t.Fatalf("failed-rehearsal confirm error = %v", err)
	}

	// A success on top of the failure satisfies the default requirement (1).
	request2 := httptest.NewRequest("GET", "/v1/echo", nil)
	request2.Header.Set(RehearsalHeader, candidate.ID)
	store.Select(request2)
	store.FinishRehearsal(candidate.ID, 1, RehearsalOutcome{Status: 200, Success: true})
	if store.Candidates()[0].Gate != GateReady {
		t.Fatal("gate not ready after a success")
	}
}

func TestRevisedCandidateInvalidatesRehearsalEvidence(t *testing.T) {
	store := NewStore(testSnapshot("v1"))
	candidate := store.Stage(testSnapshot("v2"))
	request := httptest.NewRequest("GET", "/v1/echo", nil)
	request.Header.Set(RehearsalHeader, candidate.ID)
	store.Select(request)
	store.FinishRehearsal(candidate.ID, 1, RehearsalOutcome{Status: 200, Success: true})
	if store.Candidates()[0].Gate != GateReady {
		t.Fatal("candidate not ready after success")
	}

	// Revising with a changed route invalidates the old success record.
	revised, invalidated, err := store.Revise(candidate.ID, testSnapshot("v3"))
	if err != nil || !invalidated || revised.Revision != 2 {
		t.Fatalf("revise = %+v invalidated=%v err=%v", revised, invalidated, err)
	}
	if revised.Gate != GateNotRehearsed {
		t.Fatalf("gate after revise = %s", revised.Gate)
	}
	if len(revised.Invalidations) != 1 || revised.Invalidations[0].Succeeded != 1 {
		t.Fatalf("invalidated evidence = %+v", revised.Invalidations)
	}
	if _, err := store.Confirm(candidate.ID); err == nil {
		t.Fatal("confirm after revise without new rehearsal succeeded")
	}

	// Restaging an identical configuration keeps the evidence (no-op).
	_, invalidated, err = store.Revise(candidate.ID, testSnapshot("v3"))
	if err != nil || invalidated {
		t.Fatalf("identical revise invalidated=%v err=%v", invalidated, err)
	}

	// Old revision in-flight outcome still lands on the old revision evidence.
	store.FinishRehearsal(candidate.ID, 1, RehearsalOutcome{Status: 200, Success: true})
	if store.Candidates()[0].Gate != GateNotRehearsed {
		t.Fatal("old revision outcome affected the new revision gate")
	}
}

func TestDiscardKeepsArchiveAndBlocksInFlight(t *testing.T) {
	store := NewStore(testSnapshot("v1"))
	candidate := store.Stage(testSnapshot("v2"))
	request := httptest.NewRequest("GET", "/v1/echo", nil)
	request.Header.Set(RehearsalHeader, candidate.ID)
	store.Select(request)
	if _, err := store.Discard(candidate.ID); err == nil {
		t.Fatal("discard with in-flight rehearsal succeeded")
	}
	store.FinishRehearsal(candidate.ID, 1, RehearsalOutcome{Status: 200, Success: true})
	record, err := store.Discard(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Status != "discarded" || record.Rehearsal.Succeeded != 1 {
		t.Fatalf("discard record = %+v", record)
	}
	if len(store.Candidates()) != 0 {
		t.Fatal("discarded candidate still active")
	}
	archive := store.ArchivedCandidates()
	if len(archive) != 1 || archive[0].Status != "discarded" {
		t.Fatalf("archive = %+v", archive)
	}
}

func TestRollbackToExplicitRelease(t *testing.T) {
	store := NewStore(testSnapshot("v1"))
	first := store.Stage(testSnapshot("v2"))
	second := store.Stage(testSnapshot("v3"))
	rehearseSuccess := func(id string, revision int) {
		request := httptest.NewRequest("GET", "/v1/echo", nil)
		request.Header.Set(RehearsalHeader, id)
		store.Select(request)
		store.FinishRehearsal(id, revision, RehearsalOutcome{Status: 200, Success: true})
	}
	rehearseSuccess(first.ID, 1)
	if _, err := store.Confirm(first.ID); err != nil {
		t.Fatal(err)
	}
	rehearseSuccess(second.ID, 1)
	if _, err := store.Confirm(second.ID); err != nil {
		t.Fatal(err)
	}
	rolledBack, err := store.Rollback("rel-1")
	if err != nil {
		t.Fatal(err)
	}
	if rolledBack.SourceID != "rel-1" || rolledBack.Routes[0] != "v1@v1" {
		t.Fatalf("rollback record = %+v", rolledBack)
	}
}

func TestUnknownCandidateAndReleaseErrors(t *testing.T) {
	store := NewStore(testSnapshot("v1"))
	if _, err := store.Confirm("cand-99"); err == nil {
		t.Fatal("confirming unknown candidate succeeded")
	}
	if _, err := store.Rollback("rel-99"); err == nil {
		t.Fatal("rollback to unknown release succeeded")
	}
	if _, err := store.Rollback(""); err == nil {
		t.Fatal("rollback without earlier release succeeded")
	}
}
