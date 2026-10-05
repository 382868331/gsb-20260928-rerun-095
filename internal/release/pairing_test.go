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

func TestConfirmFreezesAdoptedPairsAndLateResultIsSupplementalOnly(t *testing.T) {
	store := NewStore(testSnapshot("v1"))
	candidate := store.Stage(testSnapshot("v2"))
	round, err := store.StartCollectionRound(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}

	// Baseline side returns first; candidate fails, then a retry succeeds.
	recordPairExecution(t, store, candidate.ID, "biz-a", "base-a", SideBaseline, 1, 200, round.ID)
	recordPairExecution(t, store, candidate.ID, "biz-a", "cand-a-failed", SideCandidate, 1, 502, round.ID)
	recordPairExecution(t, store, candidate.ID, "biz-a", "cand-a-retry", SideCandidate, 2, 200, round.ID)
	// A clean pair makes the candidate confirmable despite the adverse one.
	recordPairExecution(t, store, candidate.ID, "biz-b", "base-b", SideBaseline, 1, 200, round.ID)
	recordPairExecution(t, store, candidate.ID, "biz-b", "cand-b", SideCandidate, 1, 200, round.ID)
	// Duplicate submission of the same request must not double count.
	recordPairExecution(t, store, candidate.ID, "biz-b", "cand-b", SideCandidate, 1, 200, round.ID)

	release, err := store.Confirm(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(release.AdoptedPairs) != 2 {
		t.Fatalf("adopted pairs = %+v", release.AdoptedPairs)
	}
	refs := map[string]AdoptedPairRef{}
	for _, ref := range release.AdoptedPairs {
		refs[ref.BusinessID] = ref
	}
	if refs["biz-a"].Conclusion != "adverse" || refs["biz-a"].CandidateRequestID != "cand-a-failed" || refs["biz-a"].CandidateAttempt != 1 {
		t.Fatalf("first terminal result was replaced by retry: %+v", refs["biz-a"])
	}
	if refs["biz-b"].Conclusion != "valid" || refs["biz-b"].BaselineRequestID != "base-b" || refs["biz-b"].BaselineAttempt != 1 ||
		refs["biz-b"].CandidateRequestID != "cand-b" || refs["biz-b"].CandidateAttempt != 1 {
		t.Fatalf("release does not stably identify adopted request ids and attempts: %+v", refs["biz-b"])
	}
	published := *release.Rehearsal
	if published.PairedValid != 1 || published.PairedAdverse != 1 || published.Succeeded != 1 || published.Failed != 1 {
		t.Fatalf("published summary = %+v", published)
	}

	// A late candidate result for the original round arrives after confirm.
	recordPairExecution(t, store, candidate.ID, "biz-b", "cand-b-late", SideCandidate, 2, 200, round.ID)
	// Re-submitting an already adopted request must not re-enter the release.
	recordPairExecution(t, store, candidate.ID, "biz-b", "cand-b", SideCandidate, 1, 200, round.ID)
	// Late results must not reopen a collection round either.
	if _, err := store.StartCollectionRound(candidate.ID); err == nil {
		t.Fatal("reopened a collection round on a confirmed candidate")
	}

	report, err := store.Report(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if report.Overall != published {
		t.Fatalf("late results changed the published basis: %+v != %+v", report.Overall, published)
	}
	if len(report.LateDecisions) != 2 {
		t.Fatalf("late decisions = %+v", report.LateDecisions)
	}
	late := report.LateDecisions[0]
	if late.RequestID != "cand-b-late" || late.BusinessID != "biz-b" || late.RoundID != round.ID || late.Reason == "" || late.At.IsZero() {
		t.Fatalf("late decision misses round, reason or arrival time: %+v", late)
	}
	for _, entry := range report.Evidence {
		if entry.RequestID == "cand-b-late" || (entry.RequestID == "cand-b" && entry.At.After(release.CreatedAt)) {
			if entry.Adopted || entry.Counted || !entry.Late || entry.Conclusion != "late_ignored" {
				t.Fatalf("late evidence adopted after confirm: %+v", entry)
			}
		}
	}
	// The stored release record still points at the originally adopted pair.
	history := store.History()
	stored := history[len(history)-1]
	if len(stored.AdoptedPairs) != 2 || stored.Rehearsal == nil || *stored.Rehearsal != published {
		t.Fatalf("stored release mutated after late arrivals: %+v", stored)
	}
}

func TestDiscardFreezesEvidenceAndLateResultIsSupplementalOnly(t *testing.T) {
	store := NewStore(testSnapshot("v1"))
	candidate := store.Stage(testSnapshot("v2"))
	round, err := store.StartCollectionRound(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	recordPairExecution(t, store, candidate.ID, "biz-d", "base-d", SideBaseline, 1, 200, round.ID)
	recordPairExecution(t, store, candidate.ID, "biz-d", "cand-d", SideCandidate, 1, 200, round.ID)

	if _, err := store.Discard(candidate.ID); err != nil {
		t.Fatal(err)
	}
	before, err := store.Report(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}

	// A late candidate result arrives after the candidate was abandoned.
	recordPairExecution(t, store, candidate.ID, "biz-d", "cand-d-late", SideCandidate, 2, 200, round.ID)
	// Unpaired late traffic must not grow the report basis either.
	if err := store.RecordExecution(candidate.ID, RehearsalOutcome{
		RequestID: "legacy-late", Method: "GET", Path: "/v1/echo", Status: 200, Side: SideCandidate, Round: round.ID,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartCollectionRound(candidate.ID); err == nil {
		t.Fatal("reopened a collection round on a discarded candidate")
	}

	report, err := store.Report(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if report.Overall != before.Overall {
		t.Fatalf("late results changed discarded candidate basis: %+v != %+v", report.Overall, before.Overall)
	}
	if len(report.LateDecisions) != 2 {
		t.Fatalf("late decisions = %+v", report.LateDecisions)
	}
	seen := map[string]LateDecision{}
	for _, decision := range report.LateDecisions {
		seen[decision.RequestID] = decision
	}
	if seen["cand-d-late"].RoundID != round.ID || seen["cand-d-late"].Reason == "" || seen["cand-d-late"].At.IsZero() {
		t.Fatalf("late decision = %+v", seen["cand-d-late"])
	}
	if seen["legacy-late"].Reason == "" || seen["legacy-late"].At.IsZero() {
		t.Fatalf("legacy late decision = %+v", seen["legacy-late"])
	}
	for _, entry := range report.Evidence {
		if entry.RequestID == "cand-d-late" || entry.RequestID == "legacy-late" {
			if entry.Adopted || entry.Counted || !entry.Late || entry.Conclusion != "late_ignored" {
				t.Fatalf("late evidence adopted after discard: %+v", entry)
			}
		}
	}
}
