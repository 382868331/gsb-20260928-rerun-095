package release

import (
	"strings"
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

// TestLateArrivalAfterConfirmCannotChangePublishedRelease demonstrates that
// once a candidate is confirmed the published success/failure conclusions,
// category coverage and adopted request pairs are frozen. A late candidate
// result (including a successful retry of an already decided request pair)
// is retained only as a queryable late fact.
func TestLateArrivalAfterConfirmCannotChangePublishedRelease(t *testing.T) {
	categories := []config.CoverageCategory{
		{ID: "reads", Methods: []string{"GET"}, Prefixes: []string{"/v1/"}, MinSamples: 1},
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
	recordPairExecution(t, store, candidate.ID, "biz-confirm", "base-confirm", SideBaseline, 1, 200, round.ID)
	recordPairExecution(t, store, candidate.ID, "biz-confirm", "cand-confirm", SideCandidate, 1, 502, round.ID)
	recordPairExecution(t, store, candidate.ID, "biz-confirm", "cand-retry", SideCandidate, 2, 200, round.ID)

	listed, err := store.Candidate(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if listed.Rehearsal.PairedAdverse != 1 || listed.Rehearsal.Succeeded != 0 || listed.Rehearsal.Failed != 1 {
		t.Fatalf("successful retry must not replace first terminal failure: %+v", listed.Rehearsal)
	}
	if _, err := store.Confirm(candidate.ID); err == nil {
		t.Fatal("adverse pair must block confirmation")
	}

	// A separate business request reaches valid on its first terminal pair
	// and becomes the published release basis.
	recordPairExecution(t, store, candidate.ID, "biz-publish", "base-publish", SideBaseline, 1, 200, round.ID)
	recordPairExecution(t, store, candidate.ID, "biz-publish", "cand-publish", SideCandidate, 1, 200, round.ID)
	activated, err := store.Confirm(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(activated.AdoptedPairs) != 1 {
		t.Fatalf("published adopted pairs = %+v", activated.AdoptedPairs)
	}
	ref := activated.AdoptedPairs[0]
	if ref.BusinessID != "biz-publish" || ref.BaselineRequestID != "base-publish" || ref.CandidateRequestID != "cand-publish" {
		t.Fatalf("adopted pair refs = %+v", ref)
	}
	if ref.BaselineAttempt != 1 || ref.CandidateAttempt != 1 {
		t.Fatalf("adopted pair must record concrete attempts: %+v", ref)
	}
	publishedValid := activated.Rehearsal.PairedValid
	publishedSucceeded := activated.Rehearsal.Succeeded
	publishedCoverage := append([]CategoryReport(nil), activated.Coverage...)

	// Late results arrive after confirmation: the failed pair's successful
	// retry, a duplicate submission and a late baseline side.
	recordPairExecution(t, store, candidate.ID, "biz-confirm", "cand-retry-late", SideCandidate, 3, 200, round.ID)
	recordPairExecution(t, store, candidate.ID, "biz-publish", "cand-publish-dup", SideCandidate, 1, 200, round.ID)
	recordPairExecution(t, store, candidate.ID, "biz-confirm", "base-late", SideBaseline, 2, 200, round.ID)

	history := store.History()
	promotion := history[len(history)-1]
	if promotion.Rehearsal.PairedValid != publishedValid || promotion.Rehearsal.Succeeded != publishedSucceeded {
		t.Fatalf("published release counters changed after late arrivals: %+v", promotion.Rehearsal)
	}
	if len(promotion.AdoptedPairs) != 1 || promotion.AdoptedPairs[0].CandidateRequestID != "cand-publish" {
		t.Fatalf("published adopted pairs changed: %+v", promotion.AdoptedPairs)
	}
	if len(promotion.Coverage) != 1 || promotion.Coverage[0].Valid != publishedCoverage[0].Valid {
		t.Fatalf("published category coverage changed: %+v", promotion.Coverage)
	}

	report, err := store.Report(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if report.Overall.PairedValid != publishedValid || report.Overall.Succeeded != publishedSucceeded {
		t.Fatalf("report of confirmed candidate must stay published: %+v", report.Overall)
	}
	if len(report.PairRefs) != 1 || report.PairRefs[0].CandidateRequestID != "cand-publish" {
		t.Fatalf("report pair refs must stay published: %+v", report.PairRefs)
	}
	if len(report.LateDecisions) != 3 {
		t.Fatalf("late decisions = %+v", report.LateDecisions)
	}
	for _, decision := range report.LateDecisions {
		if decision.RoundID != round.ID || decision.At.IsZero() {
			t.Fatalf("late decision must name round and arrival time: %+v", decision)
		}
		if decision.Reason == "" {
			t.Fatalf("late decision must explain why it was not adopted: %+v", decision)
		}
	}
}

// TestLateArrivalsAfterDiscardAreQueryableOnly verifies a discarded candidate
// keeps accepting late executions as supplemental facts without reviving any
// counter, pair or coverage conclusion.
func TestLateArrivalsAfterDiscardAreQueryableOnly(t *testing.T) {
	store := NewStore(testSnapshot("v1"))
	candidate := store.Stage(testSnapshot("v2"))
	round, err := store.StartCollectionRound(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	recordPairExecution(t, store, candidate.ID, "biz-discard", "base-discard", SideBaseline, 1, 200, round.ID)
	if _, err := store.Discard(candidate.ID); err != nil {
		t.Fatal(err)
	}
	recordPairExecution(t, store, candidate.ID, "biz-discard", "cand-discard-late", SideCandidate, 1, 200, round.ID)
	recordPairExecution(t, store, candidate.ID, "biz-discard", "cand-discard-dup", SideCandidate, 2, 200, round.ID)

	report, err := store.Report(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if report.Overall.PairedTotal != 0 || report.Overall.PairedValid != 0 || report.Overall.Undecided != 1 {
		t.Fatalf("late arrivals after discard changed evidence: %+v", report.Overall)
	}
	if len(report.LateDecisions) != 2 {
		t.Fatalf("late decisions after discard = %+v", report.LateDecisions)
	}
	for _, decision := range report.LateDecisions {
		if !strings.Contains(decision.Reason, "discarded") {
			t.Fatalf("late decision must mention discard: %+v", decision)
		}
		if decision.RequestID == "" || decision.RoundID != round.ID || decision.At.IsZero() {
			t.Fatalf("late decision missing request/round/arrival time: %+v", decision)
		}
	}
	for _, entry := range report.Evidence {
		if entry.BusinessID == "biz-discard" && entry.Side == SideCandidate {
			if entry.Adopted || entry.Counted || !entry.Late || entry.Conclusion != "late_ignored" {
				t.Fatalf("discarded-candidate evidence must be supplemental only: %+v", entry)
			}
		}
	}
}

// TestReopenedRoundAndDuplicateSubmissionCannotFeedOldRelease verifies that
// reopening collection for the same business request forms an independent
// pair in the new round and can never be folded back into an earlier release,
// not even via repeated submission under the old round id.
func TestReopenedRoundAndDuplicateSubmissionCannotFeedOldRelease(t *testing.T) {
	store := NewStore(testSnapshot("v1"))
	candidate := store.Stage(testSnapshot("v2"))
	first, err := store.StartCollectionRound(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	recordPairExecution(t, store, candidate.ID, "biz-reopen", "base-1", SideBaseline, 1, 200, first.ID)
	recordPairExecution(t, store, candidate.ID, "biz-reopen", "cand-1", SideCandidate, 1, 200, first.ID)
	// Reopening collection for the same business request before the release
	// starts an independent round: the second-round pair is never folded
	// into the first-round conclusion.
	if _, err := store.CloseCollectionRound(candidate.ID); err != nil {
		t.Fatal(err)
	}
	second, err := store.StartCollectionRound(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	recordPairExecution(t, store, candidate.ID, "biz-reopen", "base-2", SideBaseline, 1, 200, second.ID)
	recordPairExecution(t, store, candidate.ID, "biz-reopen", "cand-2", SideCandidate, 1, 200, second.ID)
	activated, err := store.Confirm(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(activated.AdoptedPairs); got != 2 {
		t.Fatalf("expected two independently round-scoped pairs, got %+v", activated.AdoptedPairs)
	}

	// Duplicate submissions referencing either round arrive after the
	// release. None of them can reopen or repopulate the published pairs.
	recordPairExecution(t, store, candidate.ID, "biz-reopen", "base-1-dup", SideBaseline, 1, 200, first.ID)
	recordPairExecution(t, store, candidate.ID, "biz-reopen", "cand-1-dup", SideCandidate, 1, 200, first.ID)

	promotion := store.History()[len(store.History())-1]
	if len(promotion.AdoptedPairs) != 2 {
		t.Fatalf("old release adopted pairs must stay fixed: %+v", promotion.AdoptedPairs)
	}
	report, err := store.Report(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.PairRefs) != 2 {
		t.Fatalf("published report must expose exactly the adopted pairs: %+v", report.PairRefs)
	}
	if len(report.LateDecisions) != 2 {
		t.Fatalf("post-release executions must all be late facts: %+v", report.LateDecisions)
	}
}
