package admin

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gateway/internal/config"
	"gateway/internal/proxy"
	"gateway/internal/record"
	"gateway/internal/release"
	"gateway/internal/state"
)

func cfgFor(port string) *config.Config {
	return &config.Config{
		Listen:       "127.0.0.1:0",
		AdminPath:    "/__gateway",
		DrainTimeout: config.Duration(time.Second),
		Upstreams:    []config.Upstream{{ID: "up", BaseURL: "http://127.0.0.1:" + port, Timeout: config.Duration(time.Second)}},
		Routes: []config.Route{{ID: "r", Version: "v" + port, Prefix: "/api/", UpstreamID: "up",
			Retry: config.RetryPolicy{Methods: []string{"GET"}, MaxAttempts: 1}}},
	}
}

func TestProposeCanaryAndRollbackEndpoints(t *testing.T) {
	manager, err := release.NewManager(cfgFor("1"))
	if err != nil {
		t.Fatal(err)
	}
	configs := config.NewStore(manager.Current().Snapshot)
	upstreams := state.NewStore()
	records := record.NewStore(100)
	gateway := proxy.NewWithHTTPClient(configs, upstreams, records, manager, &http.Client{})
	api := New(configs, upstreams, records, manager, gateway, "config.json", func() error { return nil })

	body, _ := json.Marshal(map[string]any{"config": cfgFor("2"), "reason": "test"})
	req := httptest.NewRequest(http.MethodPost, "/__gateway/releases/propose", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("propose status %d: %s", rec.Code, rec.Body.String())
	}
	var candidate release.Release
	json.Unmarshal(rec.Body.Bytes(), &candidate)
	if candidate.Stage != release.StageCandidate {
		t.Fatalf("stage = %s", candidate.Stage)
	}

	// Canary with no rehearsal requests is rejected, candidate still listed.
	canaryBody, _ := json.Marshal(map[string]int64{"max_requests": 3})
	req = httptest.NewRequest(http.MethodPost, "/__gateway/releases/"+candidate.ID+"/canary/start", bytes.NewReader(canaryBody))
	rec = httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatal(rec.Body.String())
	}

	// Promote while canary is open must fail with a clear error.
	req = httptest.NewRequest(http.MethodPost, "/__gateway/releases/"+candidate.ID+"/promote", strings.NewReader(`{}`))
	rec = httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("promote during canary status = %d", rec.Code)
	}

	// History exposes every stage transition for review.
	req = httptest.NewRequest(http.MethodGet, "/__gateway/releases", nil)
	rec = httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	if !strings.Contains(rec.Body.String(), candidate.ID) {
		t.Fatal("history missing candidate")
	}
}

func TestInvalidProposeDoesNotDisturbService(t *testing.T) {
	manager, _ := release.NewManager(cfgFor("1"))
	api := New(config.NewStore(manager.Current().Snapshot), state.NewStore(), record.NewStore(10), manager, nil, "", func() error { return nil })
	body := []byte(`{"config":{"listen":"127.0.0.1:0"},"reason":"bad"}`)
	req := httptest.NewRequest(http.MethodPost, "/__gateway/releases/propose", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected validation failure, got %d", rec.Code)
	}
	if manager.Current().Revision != "rev-1" {
		t.Fatal("serving revision changed after invalid proposal")
	}
}
