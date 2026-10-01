package release

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCompareShowsRoutingDifferences(t *testing.T) {
	store := NewStore(testSnapshot("v1"))
	candidate := store.Stage(testSnapshot("v2"))

	requests := []*http.Request{
		httptest.NewRequest(http.MethodGet, "/v1/echo", nil),
	}
	diff, err := store.Compare(candidate.ID, requests)
	if err != nil {
		t.Fatal(err)
	}
	if diff.CandidateID != candidate.ID || diff.ActiveRelease != "rel-1" {
		t.Fatalf("diff header = %+v", diff)
	}
	if len(diff.RoutesRemoved) != 1 || len(diff.RoutesAdded) != 1 {
		t.Fatalf("routes added/removed = %+v / %+v", diff.RoutesAdded, diff.RoutesRemoved)
	}
	if diff.RoutesRemoved[0].UpstreamID != "up-v1" || diff.RoutesAdded[0].UpstreamID != "up-v2" {
		t.Fatalf("route sides = %+v / %+v", diff.RoutesRemoved, diff.RoutesAdded)
	}
	if len(diff.Requests) != 1 || !diff.Requests[0].Changed {
		t.Fatalf("request diff = %+v", diff.Requests)
	}
	if diff.Requests[0].Active.UpstreamID != "up-v1" || diff.Requests[0].Candidate.UpstreamID != "up-v2" {
		t.Fatalf("request sides = %+v", diff.Requests[0])
	}
}

func TestCompareUnknownCandidate(t *testing.T) {
	store := NewStore(testSnapshot("v1"))
	if _, err := store.Compare("cand-99", nil); err == nil {
		t.Fatal("compare unknown candidate succeeded")
	}
}
