package release

import (
	"net/http/httptest"
	"testing"

	"gateway/internal/config"
)

func testSnapshotWithRequirement(routeID string, required int) *config.Snapshot {
	snapshot := testSnapshot(routeID)
	snapshot.MinSuccessfulRehearsals = required
	return snapshot
}

func runRehearsal(store *Store, id string, revision int, success bool) {
	request := httptest.NewRequest("GET", "/v1/echo", nil)
	request.Header.Set(RehearsalHeader, id)
	store.Select(request)
	status := 200
	if !success {
		status = 500
	}
	store.FinishRehearsal(id, revision, RehearsalOutcome{Status: status, Success: success})
}

func TestGateRequiresConfiguredNumberOfSuccesses(t *testing.T) {
	store := NewStore(testSnapshotWithRequirement("v1", 3))
	candidate := store.Stage(testSnapshotWithRequirement("v2", 3))

	runRehearsal(store, candidate.ID, 1, true)
	runRehearsal(store, candidate.ID, 1, false)
	if got := store.Candidates()[0].Gate; got != GateIncompleteResults {
		t.Fatalf("gate with 1/3 success = %s", got)
	}
	if _, err := store.Confirm(candidate.ID); err == nil {
		t.Fatal("confirm with 1/3 successes succeeded")
	}

	runRehearsal(store, candidate.ID, 1, true)
	runRehearsal(store, candidate.ID, 1, true)
	if got := store.Candidates()[0].Gate; got != GateReady {
		t.Fatalf("gate with 3/3 success = %s", got)
	}
	if _, err := store.Confirm(candidate.ID); err != nil {
		t.Fatalf("confirm with 3/3 successes: %v", err)
	}
}

func TestPromoteRecordCarriesRehearsalEvidence(t *testing.T) {
	store := NewStore(testSnapshot("v1"))
	candidate := store.Stage(testSnapshot("v2"))
	runRehearsal(store, candidate.ID, 1, false)
	runRehearsal(store, candidate.ID, 1, true)
	promoted, err := store.Confirm(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if promoted.Rehearsal == nil || promoted.Rehearsal.Succeeded != 1 || promoted.Rehearsal.Failed != 1 || promoted.Revision != 1 {
		t.Fatalf("promote evidence = %+v", promoted.Rehearsal)
	}
}
