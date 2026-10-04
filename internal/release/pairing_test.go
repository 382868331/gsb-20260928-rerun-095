package release

import (
	"testing"

	"gateway/internal/config"
)

func recordPairExecution(t *testing.T, store *Store, candidateID, businessID, requestID string, side Side, attempt int, status int, round string) {
	t.Helper()
	outcome := RehearsalOutcome{
		BusinessID: businessID,
		RequestID:  requestID,
		Source:     "paired-drill",
		Method:     "GET",
		Path:       "/v1/echo",
		Status:     status,
		Side:       side,
		Attempt:    attempt,
		Round:      round,
	}
	if status >= 500 {
		outcome.Error = "bad gateway"
	}
	if err := store.RecordExecution(candidateID, outcome); err != nil {
		t.Fatal(err)
	}
}

func TestBaselineFirstCandidateSecondAdoptsTraceablePair(t *testing.T) {
	store := NewStore(testSnapshot("v1"))
	candidate := store.Stage(testSnapshot("v2"))
	round, err := store.StartCollectionRound(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}

	recordPairExecution(t, store, candidate.ID, "biz-1", "base-1", SideBaseline, 1, 200, round.ID)
	recordPairExecution(t, store, candidate.ID, "biz-1", "cand-1", SideCandidate, 1, 200, round.ID)

	listed, err := store.Candidate(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if listed.Rehearsal.PairedTotal != 1 || listed.Rehearsal.PairedValid != 1 || listed.Rehearsal.Succeeded != 1 {
		t.Fatalf("pair summary = %+v", listed.Rehearsal)
	}
	activated, err := store.Confirm(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(activated.AdoptedPairs) != 1 {
		t.Fatalf("adopted pairs = %+v", activated.AdoptedPairs)
	}
	ref := activated.AdoptedPairs[0]
	if ref.BusinessID != "biz-1" || ref.BaselineRequestID != "base-1" || ref.CandidateRequestID != "cand-1" {
		t.Fatalf("release conclusion is not traceable to request pair: %+v", ref)
	}
}

func TestFirstFailedCandidateTerminalIsKeptAndRetryIgnored(t *testing.T) {
	store := NewStore(testSnapshot("v1"))
	candidate := store.Stage(testSnapshot("v2"))
	round, err := store.StartCollectionRound(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}

	recordPairExecution(t, store, candidate.ID, "biz-retry", "base-retry", SideBaseline, 1, 200, round.ID)
	recordPairExecution(t, store, candidate.ID, "biz-retry", "cand-failed", SideCandidate, 1, 502, round.ID)
	recordPairExecution(t, store, candidate.ID, "biz-retry", "cand-success", SideCandidate, 2, 200, round.ID)

	listed, err := store.Candidate(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if listed.Rehearsal.PairedTotal != 1 || listed.Rehearsal.PairedAdverse != 1 || listed.Rehearsal.Failed != 1 || listed.Rehearsal.Succeeded != 0 {
		t.Fatalf("retry replaced the first terminal result: %+v", listed.Rehearsal)
	}
	report, err := store.Report(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	var retry EvidenceEntry
	for _, entry := range report.Evidence {
		if entry.RequestID == "cand-success" {
			retry = entry
		}
	}
	if retry.RequestID == "" || retry.Adopted || retry.Counted || retry.Conclusion != "retry_ignored" {
		t.Fatalf("retry evidence = %+v", retry)
	}
	if _, err := store.Confirm(candidate.ID); err == nil {
		t.Fatal("adverse pair was confirmed")
	}
}

func TestPairedRequestCountsEachCategoryOnce(t *testing.T) {
	categories := []config.CoverageCategory{
		{ID: "api", Methods: []string{"GET"}, Prefixes: []string{"/v1/"}, MinSamples: 1},
		{ID: "echo", Methods: []string{"GET"}, Prefixes: []string{"/v1/echo"}, MinSamples: 1},
	}
	store := NewStore(coverageSnapshot("v1", nil))
	candidate := store.Stage(coverageSnapshot("v2", categories))
	if _, err := store.ConfirmPlan(candidate.ID); err != nil {
		t.Fatal(err)
	}
	round, err := store.StartCollectionRound(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}

	recordPairExecution(t, store, candidate.ID, "biz-categories", "base-category", SideBaseline, 1, 200, round.ID)
	recordPairExecution(t, store, candidate.ID, "biz-categories", "cand-category", SideCandidate, 1, 200, round.ID)

	listed, err := store.Candidate(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if listed.Rehearsal.PairedValid != 1 || listed.Rehearsal.Succeeded != 1 {
		t.Fatalf("overall pair counted more than once: %+v", listed.Rehearsal)
	}
	if listed.Coverage["api"].Valid != 1 || listed.Coverage["echo"].Valid != 1 {
		t.Fatalf("categories = api:%+v echo:%+v", listed.Coverage["api"], listed.Coverage["echo"])
	}
}

func TestCandidateAfterClosedRoundIsLateAndIgnored(t *testing.T) {
	store := NewStore(testSnapshot("v1"))
	candidate := store.Stage(testSnapshot("v2"))
	round, err := store.StartCollectionRound(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	recordPairExecution(t, store, candidate.ID, "biz-late", "base-late", SideBaseline, 1, 200, round.ID)
	if _, err := store.CloseCollectionRound(candidate.ID); err != nil {
		t.Fatal(err)
	}
	recordPairExecution(t, store, candidate.ID, "biz-late", "cand-late", SideCandidate, 1, 200, round.ID)

	report, err := store.Report(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if report.Overall.PairedTotal != 0 || report.Overall.Undecided != 1 || report.Overall.PairedValid != 0 {
		t.Fatalf("late execution changed closed evidence: %+v", report.Overall)
	}
	if len(report.LateDecisions) != 1 {
		t.Fatalf("late decisions = %+v", report.LateDecisions)
	}
	decision := report.LateDecisions[0]
	if decision.RequestID != "cand-late" || decision.BusinessID != "biz-late" || decision.RoundID != round.ID {
		t.Fatalf("late decision does not identify the request pair: %+v", decision)
	}
	var late EvidenceEntry
	for _, entry := range report.Evidence {
		if entry.RequestID == "cand-late" {
			late = entry
		}
	}
	if late.RequestID == "" || !late.Late || late.Adopted || late.Conclusion != "late_ignored" {
		t.Fatalf("late evidence = %+v", late)
	}
}
