package release

import (
	"strings"
	"testing"

	"gateway/internal/config"
)

func pairingSnapshot(routeID, adoption string, categories []config.CoverageCategory) *config.Snapshot {
	cfg := &config.Config{
		Listen:    "127.0.0.1:0",
		AdminPath: "/__gateway",
		Release: config.ReleasePolicy{
			MinSuccessfulRehearsals: 1,
			CoverageCategories:      categories,
			PairAdoption:            adoption,
		},
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

func pairCategories() []config.CoverageCategory {
	return []config.CoverageCategory{
		{ID: "reads", Methods: []string{"GET"}, Prefixes: []string{"/v1/"}, MinSamples: 1},
		{ID: "payments", Methods: []string{"POST"}, Prefixes: []string{"/v1/pay"}, MinSamples: 1, HighRisk: true},
	}
}

func beginPair(store *Store, candidateID, pairID, side, requestID, method, path string) {
	store.BeginPairObservation(candidateID, pairID, side, requestID, "", method, path)
}

func finishPair(store *Store, candidateID, pairID, side, requestID, method, path string, outcome RehearsalOutcome) {
	outcome.RequestID = requestID
	outcome.Method = method
	outcome.Path = path
	store.FinishPairObservation(candidateID, pairID, side, outcome)
}

func stagedPairCandidate(t *testing.T, store *Store, snapshot *config.Snapshot) Candidate {
	t.Helper()
	candidate := store.Stage(snapshot)
	if _, err := store.ConfirmPlan(candidate.ID); err != nil {
		t.Fatal(err)
	}
	return candidate
}

// The headline scenario: the baseline answers first, the candidate times out
// and its retry succeeds; with the default "latest" adoption the pair is
// valid and the release decision traces back to the adopted request pair.
func TestPairBaselineFirstCandidateTimeoutThenRetry(t *testing.T) {
	store := NewStore(pairingSnapshot("v1", "", nil))
	candidate := stagedPairCandidate(t, store, pairingSnapshot("v2", "", pairCategories()))

	// Baseline side of the business request returns first.
	beginPair(store, candidate.ID, "biz-1", PairSideBaseline, "b1", "GET", "/v1/echo")
	finishPair(store, candidate.ID, "biz-1", PairSideBaseline, "b1", "GET", "/v1/echo", RehearsalOutcome{Status: 200})

	// Candidate side is undetermined while its first attempt is in flight.
	beginPair(store, candidate.ID, "biz-1", PairSideCandidate, "c1", "GET", "/v1/echo")
	report, err := store.Report(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if report.Pairing == nil || report.Pairing.Overall.Undetermined != 1 {
		t.Fatalf("pairing while candidate in flight = %+v", report.Pairing)
	}
	if _, err := store.Confirm(candidate.ID); err == nil || !strings.Contains(err.Error(), "undetermined") {
		t.Fatalf("confirm with undetermined pair: %v", err)
	}

	// The candidate attempt times out; the retry is a new execution of the
	// same business request, kept with its own source but not a new sample.
	finishPair(store, candidate.ID, "biz-1", PairSideCandidate, "c1", "GET", "/v1/echo", RehearsalOutcome{Status: 504, Error: "timeout"})
	beginPair(store, candidate.ID, "biz-1", PairSideCandidate, "c2", "GET", "/v1/echo")
	finishPair(store, candidate.ID, "biz-1", PairSideCandidate, "c2", "GET", "/v1/echo", RehearsalOutcome{Status: 200})

	report, _ = store.Report(candidate.ID)
	overall := report.Pairing.Overall
	if overall.Paired != 1 || overall.PairedValid != 1 || overall.SingleSide != 0 || overall.Undetermined != 0 {
		t.Fatalf("pair counts = %+v", overall)
	}
	reads := report.Pairing.Categories[0]
	if reads.ID != "reads" || reads.PairedValid != 1 || !reads.Satisfied {
		t.Fatalf("reads pair coverage = %+v", reads)
	}
	// The retry is kept as an attempt but the pair is one business sample.
	pair := report.Pairing.Pairs[0]
	if len(pair.Candidate.Attempts) != 2 || pair.Candidate.Adopted != "succeeded" {
		t.Fatalf("candidate side = %+v", pair.Candidate)
	}

	// The high-risk category still lacks its own paired sample.
	if _, err := store.Confirm(candidate.ID); err == nil || !strings.Contains(err.Error(), "payments") {
		t.Fatalf("confirm without payments pair: %v", err)
	}
	beginPair(store, candidate.ID, "biz-2", PairSideBaseline, "b2", "POST", "/v1/pay")
	finishPair(store, candidate.ID, "biz-2", PairSideBaseline, "b2", "POST", "/v1/pay", RehearsalOutcome{Status: 200})
	beginPair(store, candidate.ID, "biz-2", PairSideCandidate, "c3", "POST", "/v1/pay")
	finishPair(store, candidate.ID, "biz-2", PairSideCandidate, "c3", "POST", "/v1/pay", RehearsalOutcome{Status: 200})

	promoted, err := store.Confirm(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	// Every release conclusion traces to the actually adopted request pair.
	if len(promoted.Pairs) != 2 {
		t.Fatalf("release pairs = %+v", promoted.Pairs)
	}
	if promoted.Pairs[0].PairID != "biz-1" || promoted.Pairs[0].Conclusion != ConclusionMatch ||
		promoted.Pairs[0].BaselineID != "b1" || promoted.Pairs[0].CandidateID != "c2" {
		t.Fatalf("adopted pair = %+v", promoted.Pairs[0])
	}
}

// With the "first" adoption rule fixed before rehearsal, the timed-out
// attempt stays the adopted evidence: the retry cannot swap away the
// unfavorable result, and a high-risk adverse pair blocks the release.
func TestPairAdoptionFirstKeepsUnfavorableOutcome(t *testing.T) {
	store := NewStore(pairingSnapshot("v1", "", nil))
	candidate := stagedPairCandidate(t, store, pairingSnapshot("v2", "first", pairCategories()))

	beginPair(store, candidate.ID, "biz-1", PairSideBaseline, "b1", "POST", "/v1/pay")
	finishPair(store, candidate.ID, "biz-1", PairSideBaseline, "b1", "POST", "/v1/pay", RehearsalOutcome{Status: 200})
	beginPair(store, candidate.ID, "biz-1", PairSideCandidate, "c1", "POST", "/v1/pay")
	finishPair(store, candidate.ID, "biz-1", PairSideCandidate, "c1", "POST", "/v1/pay", RehearsalOutcome{Status: 504, Error: "timeout"})
	beginPair(store, candidate.ID, "biz-1", PairSideCandidate, "c2", "POST", "/v1/pay")
	finishPair(store, candidate.ID, "biz-1", PairSideCandidate, "c2", "POST", "/v1/pay", RehearsalOutcome{Status: 200})

	report, _ := store.Report(candidate.ID)
	payments := report.Pairing.Categories[1]
	if payments.Adverse != 1 || payments.Satisfied {
		t.Fatalf("payments pair coverage = %+v", payments)
	}
	if _, err := store.Confirm(candidate.ID); err == nil || !strings.Contains(err.Error(), "adverse") {
		t.Fatalf("confirm with adverse high-risk pair: %v", err)
	}
}

// Closing a round lists unpaired requests; a late arrival only completes the
// round's facts and enters the release basis only after an explicit adopt
// decision. Reopening keeps every conclusion traceable to its pair.
func TestRoundCloseLateArrivalAndExplicitDecision(t *testing.T) {
	store := NewStore(pairingSnapshot("v1", "", nil))
	candidate := stagedPairCandidate(t, store, pairingSnapshot("v2", "", pairCategories()))

	beginPair(store, candidate.ID, "biz-1", PairSideBaseline, "b1", "GET", "/v1/echo")
	finishPair(store, candidate.ID, "biz-1", PairSideBaseline, "b1", "GET", "/v1/echo", RehearsalOutcome{Status: 200})

	// Close the round while the candidate side is still missing.
	closed, err := store.CloseRound(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if closed.Round != 1 || len(closed.Unpaired) != 1 || closed.Unpaired[0].ID != "biz-1" {
		t.Fatalf("closed round = %+v", closed)
	}
	if _, err := store.CloseRound(candidate.ID); err == nil {
		t.Fatal("closing twice without a new round succeeded")
	}

	// The candidate side arrives late: it completes the round's facts but
	// stays out of the release basis until an explicit decision.
	beginPair(store, candidate.ID, "biz-1", PairSideCandidate, "c1", "GET", "/v1/echo")
	finishPair(store, candidate.ID, "biz-1", PairSideCandidate, "c1", "GET", "/v1/echo", RehearsalOutcome{Status: 200})
	report, _ := store.Report(candidate.ID)
	pair := report.Pairing.Pairs[0]
	if pair.Status != PairStatusPaired || pair.InBasis {
		t.Fatalf("late pair = %+v", pair)
	}
	if report.Pairing.Overall.PairedValid != 0 {
		t.Fatalf("late pair entered basis without decision: %+v", report.Pairing.Overall)
	}
	if report.Pairing.Rounds[0].PendingDecision != true {
		t.Fatalf("round pending decision = %+v", report.Pairing.Rounds[0])
	}

	// A new business request after the close opens the next round.
	beginPair(store, candidate.ID, "biz-2", PairSideBaseline, "b2", "POST", "/v1/pay")
	finishPair(store, candidate.ID, "biz-2", PairSideBaseline, "b2", "POST", "/v1/pay", RehearsalOutcome{Status: 200})
	beginPair(store, candidate.ID, "biz-2", PairSideCandidate, "c2", "POST", "/v1/pay")
	finishPair(store, candidate.ID, "biz-2", PairSideCandidate, "c2", "POST", "/v1/pay", RehearsalOutcome{Status: 200})
	report, _ = store.Report(candidate.ID)
	if len(report.Pairing.Rounds) != 2 || !report.Pairing.Rounds[1].Open {
		t.Fatalf("rounds = %+v", report.Pairing.Rounds)
	}

	// The reads category cannot be satisfied by the late pair until the
	// operator adopts the round's late arrivals.
	if _, err := store.Confirm(candidate.ID); err == nil || !strings.Contains(err.Error(), "reads") {
		t.Fatalf("confirm before late decision: %v", err)
	}
	if _, err := store.DecideRound(candidate.ID, 1, true); err != nil {
		t.Fatal(err)
	}
	promoted, err := store.Confirm(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(promoted.Pairs) != 2 || promoted.Pairs[0].PairID != "biz-1" || promoted.Pairs[0].Round != 1 {
		t.Fatalf("release pairs after reopen = %+v", promoted.Pairs)
	}
}

// A rejected decision keeps late arrivals out of the release basis for good.
func TestRoundDecisionRejectedExcludesLateArrivals(t *testing.T) {
	store := NewStore(pairingSnapshot("v1", "", nil))
	candidate := stagedPairCandidate(t, store, pairingSnapshot("v2", "", pairCategories()))

	beginPair(store, candidate.ID, "biz-1", PairSideBaseline, "b1", "GET", "/v1/echo")
	finishPair(store, candidate.ID, "biz-1", PairSideBaseline, "b1", "GET", "/v1/echo", RehearsalOutcome{Status: 200})
	if _, err := store.CloseRound(candidate.ID); err != nil {
		t.Fatal(err)
	}
	beginPair(store, candidate.ID, "biz-1", PairSideCandidate, "c1", "GET", "/v1/echo")
	finishPair(store, candidate.ID, "biz-1", PairSideCandidate, "c1", "GET", "/v1/echo", RehearsalOutcome{Status: 200})
	if _, err := store.DecideRound(candidate.ID, 1, false); err != nil {
		t.Fatal(err)
	}
	report, _ := store.Report(candidate.ID)
	if report.Pairing.Overall.PairedValid != 0 || report.Pairing.Overall.SingleSide != 1 {
		t.Fatalf("rejected late arrival counted: %+v", report.Pairing.Overall)
	}
	if report.Pairing.Rounds[0].Decision != RoundDecisionRejected {
		t.Fatalf("round decision = %+v", report.Pairing.Rounds[0])
	}
}

// Changing the evidence adoption rule forms a new review version: the old
// candidate is superseded and its pairs stop being a valid release basis.
func TestChangingAdoptionRuleSupersedesCandidate(t *testing.T) {
	store := NewStore(pairingSnapshot("v1", "", nil))
	first := stagedPairCandidate(t, store, pairingSnapshot("v2", "latest", pairCategories()))
	beginPair(store, first.ID, "biz-1", PairSideBaseline, "b1", "GET", "/v1/echo")
	finishPair(store, first.ID, "biz-1", PairSideBaseline, "b1", "GET", "/v1/echo", RehearsalOutcome{Status: 200})

	second := store.Stage(pairingSnapshot("v2", "first", pairCategories()))
	if second.ID == first.ID {
		t.Fatal("changed adoption rule reused the old candidate")
	}
	report, err := store.Report(first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if report.EvidenceValid || !strings.Contains(report.InvalidatedReason, second.ID) {
		t.Fatalf("invalidation = %+v", report)
	}
	if _, err := store.Confirm(first.ID); err == nil {
		t.Fatal("superseded candidate confirmed")
	}
}

// One pair in several categories feeds each of them the same adopted
// evidence while the overall counts deduplicate the business request.
func TestPairInMultipleCategoriesCountsOnceOverall(t *testing.T) {
	categories := []config.CoverageCategory{
		{ID: "reads", Methods: []string{"GET"}, Prefixes: []string{"/v1/"}, MinSamples: 1},
		{ID: "all", Prefixes: []string{"/v1/"}, MinSamples: 1},
	}
	store := NewStore(pairingSnapshot("v1", "", nil))
	candidate := stagedPairCandidate(t, store, pairingSnapshot("v2", "", categories))

	beginPair(store, candidate.ID, "biz-1", PairSideBaseline, "b1", "GET", "/v1/echo")
	finishPair(store, candidate.ID, "biz-1", PairSideBaseline, "b1", "GET", "/v1/echo", RehearsalOutcome{Status: 200})
	beginPair(store, candidate.ID, "biz-1", PairSideCandidate, "c1", "GET", "/v1/echo")
	finishPair(store, candidate.ID, "biz-1", PairSideCandidate, "c1", "GET", "/v1/echo", RehearsalOutcome{Status: 200})

	report, _ := store.Report(candidate.ID)
	if report.Pairing.Overall.PairedValid != 1 {
		t.Fatalf("overall dedup = %+v", report.Pairing.Overall)
	}
	for _, category := range report.Pairing.Categories {
		if category.PairedValid != 1 || !category.Satisfied {
			t.Fatalf("category %s = %+v", category.ID, category)
		}
	}
	if len(report.Pairing.Pairs[0].Categories) != 2 {
		t.Fatalf("pair categories = %+v", report.Pairing.Pairs[0])
	}
}
