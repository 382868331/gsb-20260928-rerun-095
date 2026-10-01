package release

import (
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
	if selection := store.Select(rehearsal); selection.ID != candidate.ID {
		t.Fatalf("rehearsal selected %s", selection.ID)
	}
	plain := httptest.NewRequest("GET", "/v1/echo", nil)
	if selection := store.Select(plain); selection.ID != "rel-1" {
		t.Fatalf("normal traffic selected %s", selection.ID)
	}
	store.FinishRehearsal(candidate.ID, RehearsalOutcome{Status: 200})
	listed := store.Candidates()[0]
	if got := listed.Rehearsal.InFlight; got != 0 {
		t.Fatalf("rehearsal in flight = %d", got)
	}
	if got := listed.Rehearsal.Succeeded; got != 1 {
		t.Fatalf("successful rehearsals = %d", got)
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
	confirmedList := store.Candidates()
	if len(confirmedList) != 1 || confirmedList[0].State != StateConfirmed {
		t.Fatalf("confirmed candidate not archived as confirmed: %+v", confirmedList)
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
	if selection := store.Select(plain); selection.ID != rolledBack.ID {
		t.Fatalf("after rollback selected %s", selection.ID)
	}
}

func TestRollbackToExplicitRelease(t *testing.T) {
	store := NewStore(testSnapshot("v1"))
	requireRehearsal := func(id string) {
		request := httptest.NewRequest("GET", "/v1/echo", nil)
		request.Header.Set(RehearsalHeader, id)
		selection := store.Select(request)
		store.FinishRehearsal(selection.CandidateID, RehearsalOutcome{Status: 200})
	}
	first := store.Stage(testSnapshot("v2"))
	requireRehearsal(first.ID)
	second := store.Stage(testSnapshot("v3"))
	requireRehearsal(second.ID)
	// The v3 revision supersedes v2, so v2 must no longer be confirmable.
	if _, err := store.Confirm(first.ID); err == nil {
		t.Fatal("superseded candidate was confirmed")
	}
	if state := store.Candidates()[0]; state.State != StateSuperseded {
		t.Fatalf("superseded state = %s", state.State)
	}
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

// A brand new candidate, an in-flight rehearsal or a failed rehearsal are
// distinct non-releasable states; none may skip the release gate.
func TestConfirmRequiresPassedRehearsal(t *testing.T) {
	store := NewStore(testSnapshot("v1"))
	candidate := store.Stage(testSnapshot("v2"))

	if _, err := store.Confirm(candidate.ID); err == nil {
		t.Fatal("fresh candidate was confirmed without rehearsal")
	}
	if readiness := store.Candidates()[0].Readiness; readiness.State != StateInsufficient || readiness.Ready {
		t.Fatalf("fresh readiness = %+v", readiness)
	}

	request := httptest.NewRequest("GET", "/v1/echo", nil)
	request.Header.Set(RehearsalHeader, candidate.ID)
	selection := store.Select(request)
	if readiness := store.Candidates()[0].Readiness; readiness.State != StateRehearsing || readiness.Ready {
		t.Fatalf("in-flight readiness = %+v", readiness)
	}
	if _, err := store.Confirm(selection.CandidateID); err == nil {
		t.Fatal("candidate confirmed while rehearsal in flight")
	}
	store.FinishRehearsal(selection.CandidateID, RehearsalOutcome{Status: 502, Error: "bad gateway"})
	if readiness := store.Candidates()[0].Readiness; readiness.State != StateInsufficient || readiness.Summary.Failed != 1 {
		t.Fatalf("failed readiness = %+v", readiness)
	}
	if _, err := store.Confirm(candidate.ID); err == nil {
		t.Fatal("candidate with failed rehearsal was confirmed")
	}

	request = httptest.NewRequest("GET", "/v1/echo", nil)
	request.Header.Set(RehearsalHeader, candidate.ID)
	selection = store.Select(request)
	store.FinishRehearsal(selection.CandidateID, RehearsalOutcome{Status: 200})
	activated, err := store.Confirm(candidate.ID)
	if err != nil {
		t.Fatalf("confirm after success: %v", err)
	}
	if activated.Rehearsal == nil || activated.Rehearsal.Failed != 1 || activated.Rehearsal.Succeeded != 1 {
		t.Fatalf("release record lost rehearsal evidence: %+v", activated.Rehearsal)
	}
	listed, err := store.Candidate(candidate.ID)
	if err != nil || listed.State != StateConfirmed {
		t.Fatalf("confirmed candidate not retained: %+v err=%v", listed, err)
	}
}

// A revision that changes routing invalidates evidence gathered against the
// earlier candidate, even though the older rehearsal had passed.
func TestRevisionInvalidatesEvidence(t *testing.T) {
	store := NewStore(testSnapshot("v1"))
	first := store.Stage(testSnapshot("v2"))
	request := httptest.NewRequest("GET", "/v1/echo", nil)
	request.Header.Set(RehearsalHeader, first.ID)
	selection := store.Select(request)
	store.FinishRehearsal(selection.CandidateID, RehearsalOutcome{Status: 200})
	if !store.Candidates()[0].Readiness.Ready {
		t.Fatal("first candidate should be ready before revision")
	}

	store.Stage(testSnapshot("v3"))
	firstState, err := store.Candidate(first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if firstState.State != StateSuperseded || firstState.Readiness.Ready {
		t.Fatalf("superseded candidate still releasable: %+v", firstState)
	}
	if firstState.Rehearsal.Succeeded != 1 {
		t.Fatal("superseding erased rehearsal evidence")
	}
	if _, err := store.Confirm(first.ID); err == nil {
		t.Fatal("superseded candidate was confirmed")
	}
}

// Re-staging an unchanged snapshot reuses the staged revision and keeps its
// rehearsal evidence.
func TestUnchangedRestageKeepsEvidence(t *testing.T) {
	store := NewStore(testSnapshot("v1"))
	first := store.Stage(testSnapshot("v2"))
	request := httptest.NewRequest("GET", "/v1/echo", nil)
	request.Header.Set(RehearsalHeader, first.ID)
	selection := store.Select(request)
	store.FinishRehearsal(selection.CandidateID, RehearsalOutcome{Status: 200})

	again := store.Stage(testSnapshot("v2"))
	if again.ID != first.ID {
		t.Fatalf("unchanged restage created %s, want %s", again.ID, first.ID)
	}
	if !again.Readiness.Ready {
		t.Fatal("unchanged restage lost rehearsal evidence")
	}
}

func TestDiscardKeepsTrailAndBlocksConfirm(t *testing.T) {
	store := NewStore(testSnapshot("v1"))
	candidate := store.Stage(testSnapshot("v2"))
	discarded, err := store.Discard(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if discarded.State != StateDiscarded {
		t.Fatalf("discarded state = %s", discarded.State)
	}
	if _, err := store.Confirm(candidate.ID); err == nil {
		t.Fatal("discarded candidate was confirmed")
	}
	if _, err := store.Discard(candidate.ID); err == nil {
		t.Fatal("discarding a resolved candidate succeeded")
	}
	if got := len(store.Candidates()); got != 1 {
		t.Fatalf("candidate history length = %d", got)
	}
}
