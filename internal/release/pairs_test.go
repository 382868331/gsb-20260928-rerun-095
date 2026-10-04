package release

import (
	"strings"
	"testing"
)

// pairedOutcome records one business-correlated execution through the
// full RecordExecution path used by FinishRehearsal.
func pairedOutcome(store *Store, candidateID, requestID, businessID string, side Side, attempt int, outcome RehearsalOutcome) {
	outcome.RequestID = requestID
	outcome.BusinessID = businessID
	outcome.Side = side
	outcome.Attempt = attempt
	if outcome.Method == "" {
		outcome.Method = "GET"
	}
	if outcome.Path == "" {
		outcome.Path = "/v1/echo"
	}
	if err := store.RecordExecution(candidateID, outcome); err != nil {
		panic(err)
	}
}

// The baseline (original artifact) returns first; the candidate side then
// completes the pair and the conclusion traces back to both request ids.
func TestBaselineFirstThenCandidateCompletesPair(t *testing.T) {
	store := NewStore(testSnapshot("v1"))
	candidate := store.Stage(testSnapshot("v2"))

	pairedOutcome(store, candidate.ID, "base-1", "biz-1", SideBaseline, 1, RehearsalOutcome{Status: 200})
	listed := store.Candidates()[0]
	if listed.Rehearsal.Undecided != 1 || listed.Rehearsal.PairedTotal != 0 {
		t.Fatalf("after baseline only: %+v", listed.Rehearsal)
	}
	// Baseline outcomes never count as candidate rehearsal success.
	if listed.Rehearsal.Succeeded != 0 {
		t.Fatalf("baseline counted as candidate success: %+v", listed.Rehearsal)
	}

	pairedOutcome(store, candidate.ID, "cand-1", "biz-1", SideCandidate, 1, RehearsalOutcome{Status: 200})
	listed = store.Candidates()[0]
	if listed.Rehearsal.PairedTotal != 1 || listed.Rehearsal.PairedValid != 1 || listed.Rehearsal.Succeeded != 1 {
		t.Fatalf("after candidate side: %+v", listed.Rehearsal)
	}

	promoted, err := store.Confirm(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if promoted.EvidencePolicy != EvidencePolicyFirstTerminal || promoted.ReviewVersion != candidate.ReviewVersion {
		t.Fatalf("release evidence metadata = %+v", promoted)
	}
	if len(promoted.AdoptedPairs) != 1 {
		t.Fatalf("adopted pairs = %+v", promoted.AdoptedPairs)
	}
	ref := promoted.AdoptedPairs[0]
	if ref.BusinessID != "biz-1" || ref.BaselineRequestID != "base-1" || ref.CandidateRequestID != "cand-1" ||
		ref.Round != "round-1" || ref.Conclusion != ConclusionPairedValid {
		t.Fatalf("adopted pair ref = %+v", ref)
	}
}

// A retry after the candidate side failed is kept as evidence but never
// replaces the adopted first terminal result.
func TestCandidateFailureRetryNeverReplacesFirstResult(t *testing.T) {
	store := NewStore(testSnapshot("v1"))
	candidate := store.Stage(testSnapshot("v2"))

	pairedOutcome(store, candidate.ID, "base-1", "biz-1", SideBaseline, 1, RehearsalOutcome{Status: 200})
	pairedOutcome(store, candidate.ID, "cand-1", "biz-1", SideCandidate, 1, RehearsalOutcome{Status: 502, Error: "bad gateway"})
	// Retry of the same side succeeds, but the first terminal result stands.
	pairedOutcome(store, candidate.ID, "cand-1-retry", "biz-1", SideCandidate, 2, RehearsalOutcome{Status: 200})

	listed := store.Candidates()[0]
	if listed.Rehearsal.Failed != 1 || listed.Rehearsal.Succeeded != 0 {
		t.Fatalf("retry changed adopted counts: %+v", listed.Rehearsal)
	}
	if listed.Rehearsal.PairedAdverse != 1 || listed.Rehearsal.PairedValid != 0 {
		t.Fatalf("retry changed pair conclusion: %+v", listed.Rehearsal)
	}

	report, err := store.Report(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Pairs) != 1 || report.Pairs[0].Conclusion != ConclusionPairedAdverse ||
		report.Pairs[0].Candidate.RequestID != "cand-1" {
		t.Fatalf("pair = %+v", report.Pairs)
	}
	var retry *EvidenceEntry
	for i := range report.Evidence {
		if report.Evidence[i].RequestID == "cand-1-retry" {
			retry = &report.Evidence[i]
		}
	}
	if retry == nil || retry.Counted || retry.Adopted || retry.Conclusion != "retry-not-adopted" {
		t.Fatalf("retry evidence = %+v", retry)
	}
	if _, err := store.Confirm(candidate.ID); err == nil {
		t.Fatal("candidate with adverse pair was confirmed")
	}
}

// A paired request matching several categories is counted once overall and
// once per category; a repeated request id never inflates the counts.
func TestPairedCategoryDedup(t *testing.T) {
	store := NewStore(coverageSnapshot("v1", nil))
	candidate := store.Stage(coverageSnapshot("v2", testCategories()))
	if _, err := store.ConfirmPlan(candidate.ID); err != nil {
		t.Fatal(err)
	}

	pairedOutcome(store, candidate.ID, "base-1", "biz-1", SideBaseline, 1, RehearsalOutcome{Status: 200})
	pairedOutcome(store, candidate.ID, "cand-1", "biz-1", SideCandidate, 1, RehearsalOutcome{Status: 200})
	// Duplicate request id must be logged but not counted.
	pairedOutcome(store, candidate.ID, "cand-1", "biz-1", SideCandidate, 1, RehearsalOutcome{Status: 200})
	pairedOutcome(store, candidate.ID, "cand-2", "biz-2", SideCandidate, 1, RehearsalOutcome{Status: 200})

	report, err := store.Report(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if report.Overall.Succeeded != 2 {
		t.Fatalf("overall = %+v", report.Overall)
	}
	reads := report.Categories[0]
	if reads.ID != "reads" || reads.Valid != 2 {
		t.Fatalf("reads = %+v", reads)
	}
	var duplicate *EvidenceEntry
	seen := 0
	for i := range report.Evidence {
		if report.Evidence[i].RequestID == "cand-1" {
			seen++
			if !report.Evidence[i].Counted {
				duplicate = &report.Evidence[i]
			}
		}
	}
	if seen != 2 || duplicate == nil || duplicate.Conclusion != "duplicate-not-counted" {
		t.Fatalf("duplicate handling = %+v (entries %d)", duplicate, seen)
	}
}

// Paired outcomes arriving after the collection round closed are late:
// recorded with a LateDecision, never counted, never changing conclusions.
func TestLateArrivalAfterRoundClosed(t *testing.T) {
	store := NewStore(testSnapshot("v1"))
	candidate := store.Stage(testSnapshot("v2"))

	pairedOutcome(store, candidate.ID, "cand-1", "biz-1", SideCandidate, 1, RehearsalOutcome{Status: 200})
	if _, err := store.Confirm(candidate.ID); err != nil {
		t.Fatal(err)
	}

	// The candidate is confirmed: its round is closed, so the trailing
	// baseline side of biz-1 and any further execution arrive late.
	pairedOutcome(store, candidate.ID, "base-1", "biz-1", SideBaseline, 1, RehearsalOutcome{Status: 200})
	pairedOutcome(store, candidate.ID, "cand-2", "biz-2", SideCandidate, 1, RehearsalOutcome{Status: 200})

	report, err := store.Report(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.LateDecisions) != 2 {
		t.Fatalf("late decisions = %+v", report.LateDecisions)
	}
	for _, decision := range report.LateDecisions {
		if decision.Decision != "recorded-not-counted" || decision.Round != "round-1" ||
			!strings.Contains(decision.Reason, "closed") {
			t.Fatalf("late decision = %+v", decision)
		}
	}
	if report.Overall.Succeeded != 1 || report.Overall.PairedTotal != 0 || report.Overall.SingleSided != 1 {
		t.Fatalf("late arrivals changed counts: %+v", report.Overall)
	}
	if len(report.Rounds) != 1 || !report.Rounds[0].Closed || report.Rounds[0].CloseReason == "" {
		t.Fatalf("rounds = %+v", report.Rounds)
	}
	late := 0
	for _, entry := range report.Evidence {
		if entry.Late {
			late++
			if entry.Counted {
				t.Fatalf("late entry counted: %+v", entry)
			}
		}
	}
	if late != 2 {
		t.Fatalf("late evidence entries = %d", late)
	}
	// The confirmed release still points at exactly the adopted pair.
	history := store.History()
	promoted := history[len(history)-1]
	if len(promoted.AdoptedPairs) != 1 || promoted.AdoptedPairs[0].CandidateRequestID != "cand-1" {
		t.Fatalf("adopted pairs = %+v", promoted.AdoptedPairs)
	}
}
