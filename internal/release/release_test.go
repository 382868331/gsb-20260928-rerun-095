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
	if _, id := store.Select(rehearsal); id != candidate.ID {
		t.Fatalf("rehearsal selected %s", id)
	}
	plain := httptest.NewRequest("GET", "/v1/echo", nil)
	if _, id := store.Select(plain); id != "rel-1" {
		t.Fatalf("normal traffic selected %s", id)
	}
	if got := store.Candidates()[0].RehearsalRequests; got != 1 {
		t.Fatalf("rehearsal requests = %d", got)
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
	if _, id := store.Select(plain); id != rolledBack.ID {
		t.Fatalf("after rollback selected %s", id)
	}
}

func TestRollbackToExplicitRelease(t *testing.T) {
	store := NewStore(testSnapshot("v1"))
	first := store.Stage(testSnapshot("v2"))
	second := store.Stage(testSnapshot("v3"))
	if _, err := store.Confirm(first.ID); err != nil {
		t.Fatal(err)
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
