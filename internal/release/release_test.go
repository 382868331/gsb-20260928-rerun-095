package release_test

import (
	"net/http"
	"testing"
	"time"

	"gateway/internal/config"
	"gateway/internal/release"
)

func validConfig(port string) *config.Config {
	return &config.Config{
		Listen:       "127.0.0.1:" + port,
		AdminPath:    "/__gateway",
		DrainTimeout: config.Duration(time.Second),
		Upstreams:    []config.Upstream{{ID: "up", BaseURL: "http://127.0.0.1:" + port, Timeout: config.Duration(time.Second)}},
		Routes: []config.Route{{
			ID: "r", Version: "v" + port, Prefix: "/api/", UpstreamID: "up", StripPrefix: true,
			Retry: config.RetryPolicy{Methods: []string{http.MethodGet}, MaxAttempts: 1},
		}},
	}
}

func TestCandidateCanaryPromoteAndAuditableRollback(t *testing.T) {
	manager, err := release.NewManager(validConfig("1001"))
	if err != nil {
		t.Fatal(err)
	}
	if got := manager.Current().Revision; got != "rev-1" {
		t.Fatalf("initial revision = %s", got)
	}

	candidate, err := manager.Propose(validConfig("2002"), "move traffic to v2")
	if err != nil {
		t.Fatal(err)
	}
	if manager.Current().Revision != "rev-1" {
		t.Fatal("candidate must not affect serving traffic")
	}
	if _, err := manager.Promote(candidate.ID, ""); err == nil {
		t.Fatal("promotion before canary must be rejected")
	}

	if _, err := manager.BeginCanary(candidate.ID, 2); err != nil {
		t.Fatal(err)
	}
	if _, ok := manager.CanaryAllow("rev-1"); ok {
		t.Fatal("wrong revision must not join canary")
	}
	c1, ok := manager.CanaryAllow("rev-2")
	if !ok {
		t.Fatal("tagged rehearsal request must be admitted")
	}
	manager.CanaryRecord(c1.ID, http.StatusOK, false)
	c2, ok := manager.CanaryAllow("rev-2")
	if !ok {
		t.Fatal("second rehearsal request must be admitted")
	}
	manager.CanaryRecord(c2.ID, http.StatusOK, false)
	if _, ok := manager.CanaryAllow("rev-2"); ok {
		t.Fatal("rehearsal budget must be bounded")
	}
	if _, err := manager.EndCanary(candidate.ID); err != nil {
		t.Fatal(err)
	}
	promoted, err := manager.Promote(candidate.ID, "confirmed after rehearsal")
	if err != nil {
		t.Fatal(err)
	}
	if promoted.Revision != "rev-2" || manager.Current().Revision != "rev-2" {
		t.Fatal("cutover did not take effect")
	}

	// Rollback produces a brand-new release record; failed config remains.
	rolledBack, err := manager.Rollback("r0001", "v2 regressions")
	if err != nil {
		t.Fatal(err)
	}
	if rolledBack.Revision == "rev-2" || rolledBack.ParentID != promoted.ID || rolledBack.Stage != release.StageRolledBack {
		t.Fatalf("unexpected rollback record: %+v", rolledBack)
	}
	history := manager.History()
	if len(history) != 3 {
		t.Fatalf("history len = %d, want 3 (failed config retained)", len(history))
	}
	if history[1].Stage != release.StagePromoted {
		t.Fatal("failed release must not be erased or rewritten")
	}
}

func TestFailingCanaryIsRejectedAndKeptServingOld(t *testing.T) {
	manager, _ := release.NewManager(validConfig("1001"))
	candidate, err := manager.Propose(validConfig("2002"), "bad config")
	if err != nil {
		t.Fatal(err)
	}
	manager.BeginCanary(candidate.ID, 1)
	allowed, _ := manager.CanaryAllow("rev-2")
	manager.CanaryRecord(allowed.ID, http.StatusBadGateway, true)
	if _, err := manager.EndCanary(candidate.ID); err == nil {
		t.Fatal("failing canary must be rejected")
	}
	if manager.Current().Revision != "rev-1" {
		t.Fatal("old revision must keep serving")
	}
	failed, ok := manager.Get(candidate.ID)
	if !ok || failed.Stage != release.StageRejected {
		t.Fatal("rejected candidate must remain auditable")
	}
}

func TestInvalidCandidateRejectedIndependently(t *testing.T) {
	manager, _ := release.NewManager(validConfig("1001"))
	bad := validConfig("2002")
	bad.Routes[0].UpstreamID = "missing"
	if _, err := manager.Propose(bad, ""); err == nil {
		t.Fatal("invalid config must not become candidate")
	}
	if manager.Current().Revision != "rev-1" {
		t.Fatal("validation failure must not disturb service")
	}
}
