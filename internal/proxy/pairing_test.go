package proxy

import (
	"net/http"
	"testing"

	"gateway/internal/release"
)

// The same business request executed on the baseline and on the candidate
// with one pair id forms a single paired evidence sample; the release
// conclusion traces back to that adopted pair.
func TestPairedRehearsalAcrossBaselineAndCandidate(t *testing.T) {
	handler, releases, _ := newReleaseProxy()
	candidate := releases.Stage(hostSnapshot("new.example"))

	// Baseline side: normal traffic carrying the pair correlation id.
	if body := get(handler, "/v1/echo", map[string]string{release.PairHeader: "biz-1"}).Body.String(); body != "old.example" {
		t.Fatalf("baseline body = %q", body)
	}
	report, err := releases.Report(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if report.Pairing == nil || report.Pairing.Overall.SingleSide != 1 {
		t.Fatalf("after baseline only = %+v", report.Pairing)
	}

	// Candidate side: the same business request pinned to the candidate.
	rehearsal := get(handler, "/v1/echo", map[string]string{
		release.RehearsalHeader: candidate.ID,
		release.PairHeader:      "biz-1",
	})
	if body := rehearsal.Body.String(); body != "new.example" {
		t.Fatalf("candidate body = %q", body)
	}
	report, _ = releases.Report(candidate.ID)
	overall := report.Pairing.Overall
	if overall.Paired != 1 || overall.PairedValid != 1 || overall.SingleSide != 0 {
		t.Fatalf("pair counts = %+v", overall)
	}
	pair := report.Pairing.Pairs[0]
	if pair.Status != release.PairStatusPaired || !pair.InBasis || pair.Conclusion != release.ConclusionMatch {
		t.Fatalf("pair = %+v", pair)
	}

	promoted, err := releases.Confirm(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(promoted.Pairs) != 1 || promoted.Pairs[0].PairID != "biz-1" {
		t.Fatalf("release pairs = %+v", promoted.Pairs)
	}
	if body := get(handler, "/v1/echo", nil).Body.String(); body != "new.example" {
		t.Fatalf("post-confirm body = %q", body)
	}
}

// A canceled candidate side is recorded as an adverse pair instead of being
// silently dropped or cherry-picked away.
func TestPairWithCanceledCandidateSideIsAdverse(t *testing.T) {
	handler, releases, _ := newReleaseProxy()
	candidate := releases.Stage(hostSnapshot("new.example"))

	get(handler, "/v1/echo", map[string]string{release.PairHeader: "biz-9"})
	// The candidate side ends as canceled (the proxy path for real client
	// cancels is covered by the generic proxy tests).
	releases.FinishPairObservation(candidate.ID, "biz-9", release.PairSideCandidate, release.RehearsalOutcome{
		RequestID: "c9", Method: http.MethodGet, Path: "/v1/echo", Status: 499, Canceled: true,
	})
	report, _ := releases.Report(candidate.ID)
	if report.Pairing.Overall.Paired != 1 || report.Pairing.Overall.PairedValid != 0 {
		t.Fatalf("canceled pair counts = %+v", report.Pairing.Overall)
	}
	if report.Pairing.Pairs[0].Conclusion != release.ConclusionAdverse {
		t.Fatalf("conclusion = %+v", report.Pairing.Pairs[0])
	}
}
