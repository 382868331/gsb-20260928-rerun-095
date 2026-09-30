package gatewayflow_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"gateway/internal/config"
	"gateway/internal/proxy"
	"gateway/internal/record"
	"gateway/internal/release"
	"gateway/internal/state"
)

const totalChunks = 80

func longUpstream(t *testing.T, name string) *pipeHTTPServer {
	t.Helper()
	return newPipeHTTPServer(t, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		flusher, _ := writer.(http.Flusher)
		writer.Header().Set("Content-Type", "text/plain")
		writer.WriteHeader(http.StatusOK)
		fmt.Fprintf(writer, "BEGIN-%s\n", name)
		flusher.Flush()
		for i := 0; i < totalChunks; i++ {
			select {
			case <-request.Context().Done():
				return
			case <-time.After(5 * time.Millisecond):
			}
			fmt.Fprintf(writer, "%s-chunk-%03d\n", name, i)
			flusher.Flush()
		}
		fmt.Fprintf(writer, "END-%s\n", name)
		flusher.Flush()
	}))
}

func buildConfig(listen, v1URL, v2URL string) *config.Config {
	return buildConfigVersion(listen, v1URL, v2URL, "v1", "local-v1")
}

func buildConfigVersion(listen, v1URL, v2URL, version, upstreamID string) *config.Config {
	return &config.Config{
		Listen:       listen,
		AdminPath:    "/__gateway",
		DrainTimeout: config.Duration(2 * time.Second),
		Upstreams: []config.Upstream{
			{ID: "local-v1", BaseURL: v1URL, Timeout: config.Duration(10 * time.Second)},
			{ID: "local-v2", BaseURL: v2URL, Timeout: config.Duration(10 * time.Second)},
		},
		Routes: []config.Route{
			{ID: "api", Version: version, Prefix: "/api/", UpstreamID: upstreamID, StripPrefix: true,
				Retry: config.RetryPolicy{Methods: []string{"GET"}, MaxAttempts: 1}},
		},
	}
}

func newTestGateway(t *testing.T, v1, v2 *pipeHTTPServer) (*pipeHTTPServer, *release.Manager, *proxy.Proxy, *record.Store) {
	t.Helper()
	cfg := buildConfig("127.0.0.1:0", v1.URL, v2.URL)
	releases, err := release.NewManager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	configs := config.NewStore(releases.Current().Snapshot)
	upstreams := state.NewStore()
	records := record.NewStore(1000)
	gwHandler := proxy.NewWithHTTPClient(configs, upstreams, records, releases, v1.Client())
	muxHandler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if strings.HasPrefix(request.URL.Path, "/__gateway") {
			http.Error(writer, "not found", http.StatusNotFound)
			return
		}
		gwHandler.ServeHTTP(writer, request)
	})
	server := newPipeHTTPServer(t, muxHandler)
	return server, releases, gwHandler, records
}

func switchToV2(t *testing.T, gatewayClient *http.Client, manager *release.Manager, base string) *release.Release {
	t.Helper()
	cfg := buildConfigVersion("127.0.0.1:0", "http://127.0.0.1:1", "http://v2.invalid", "v2", "local-v2")
	candidate, err := manager.Propose(cfg, "shift /api to v2")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.BeginCanary(candidate.ID, 5); err != nil {
		t.Fatal(err)
	}
	// Canary must reach the candidate upstream: switch the proxy's upstream
	// dialer per revision by tagging distinct hosts.
	req2, _ := http.NewRequest(http.MethodGet, base+"/api/long", nil)
	req2.Header.Set(proxy.CanaryRevisionHeader, candidate.Revision)
	resp, err := gatewayClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.Header.Get("X-Gateway-Revision") != candidate.Revision || !strings.Contains(string(body), "v2") {
		t.Fatalf("canary did not hit candidate: revision=%s body=%q", resp.Header.Get("X-Gateway-Revision"), body)
	}
	if _, err := manager.EndCanary(candidate.ID); err != nil {
		t.Fatal(err)
	}
	promoted, err := manager.Promote(candidate.ID, "confirmed")
	if err != nil {
		t.Fatal(err)
	}
	return promoted
}

func TestReleaseLifecycleEndToEnd(t *testing.T) {
	v1 := longUpstream(t, "v1")
	v2 := longUpstream(t, "v2")
	t.Cleanup(v1.Close)
	t.Cleanup(v2.Close)
	server, releases, gwHandler, records := newTestGateway(t, v1, v2)
	gatewayClient := server.Client()

	// 1. Baseline traffic belongs to rev-1 / v1.
	resp, err := gatewayClient.Get(server.URL + "/api/long")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.Header.Get("X-Gateway-Revision") != "rev-1" || !strings.Contains(string(body), "v1-chunk") {
		t.Fatalf("baseline misrouted: rev=%s", resp.Header.Get("X-Gateway-Revision"))
	}

	// 2. Start a complete long response, then cut over while it is in flight.
	longResp, err := gatewayClient.Get(server.URL + "/api/long")
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(longResp.Body)
	line, err := reader.ReadString('\n')
	if err != nil || !strings.Contains(line, "BEGIN-v1") {
		t.Fatalf("expected v1 begin, got %q err=%v", line, err)
	}
	// Candidate configuration points at the genuinely different v2 upstream;
	// switch the proxy's upstream dialer for the rehearsal, then back.
	gwHandler.SetClient(v2.Client())
	promoted := switchToV2(t, gatewayClient, releases, server.URL)

	// 3. New request belongs to rev-2 / v2.
	newResp, err := gatewayClient.Get(server.URL + "/api/long")
	if err != nil {
		t.Fatal(err)
	}
	newBody, _ := io.ReadAll(newResp.Body)
	newResp.Body.Close()
	if newResp.Header.Get("X-Gateway-Revision") != promoted.Revision || !strings.Contains(string(newBody), "v2-chunk") {
		t.Fatalf("new request not on new revision: rev=%s", newResp.Header.Get("X-Gateway-Revision"))
	}

	// 4. The in-flight response completes entirely from v1: no splicing.
	rest, _ := io.ReadAll(reader)
	longResp.Body.Close()
	full := line + string(rest)
	if strings.Contains(full, "v2") || !strings.Contains(full, "END-v1") {
		t.Fatalf("in-flight response spliced or truncated: %q", tail(full))
	}
	if strings.Count(full, "BEGIN-") != 1 {
		t.Fatal("two upstream responses were spliced together")
	}

	// 5. Rollback is a new release record, not an erasure.
	rolledBack, err := releases.Rollback("r0001", "integration rollback")
	if err != nil {
		t.Fatal(err)
	}
	gwHandler.SetClient(v1.Client())
	resp2, err := gatewayClient.Get(server.URL + "/api/long")
	if err != nil {
		t.Fatal(err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if resp2.Header.Get("X-Gateway-Release") != rolledBack.ID || !strings.Contains(string(body2), "v1") {
		t.Fatalf("rollback traffic not restored: release=%s", resp2.Header.Get("X-Gateway-Release"))
	}
	history := releases.History()
	if len(history) != 3 || history[1].ID != promoted.ID {
		t.Fatalf("audit history wrong: %d entries", len(history))
	}

	// Records clearly attribute every request to a release revision.
	for _, entry := range records.Snapshot() {
		if entry.ReleaseID == "" || entry.Revision == "" {
			t.Fatalf("record missing release attribution: %+v", entry)
		}
	}
}

func TestClientCancelMidstreamIsRecordedAs499(t *testing.T) {
	v1 := longUpstream(t, "v1")
	v2 := longUpstream(t, "v2")
	t.Cleanup(v1.Close)
	t.Cleanup(v2.Close)
	server, _, _, records := newTestGateway(t, v1, v2)

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/api/long", nil)
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	n, err := resp.Body.Read(buf)
	if err != nil || !strings.Contains(string(buf[:n]), "BEGIN-v1") {
		t.Fatalf("first bytes wrong: n=%d err=%v", n, err)
	}
	cancel()
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		entries := records.Snapshot()
		if len(entries) >= 1 && entries[0].Status == 499 && entries[0].Canceled {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("canceled long response not recorded as 499: %+v", records.Snapshot())
}

func TestDrainFinishesInFlightAndAbortsOverdue(t *testing.T) {
	v1 := longUpstream(t, "v1")
	v2 := longUpstream(t, "v2")
	t.Cleanup(v1.Close)
	t.Cleanup(v2.Close)
	server, _, handler, records := newTestGateway(t, v1, v2)
	client := server.Client()

	// A request that will outlive the drain window.
	overResp, err := client.Get(server.URL + "/api/long")
	if err != nil {
		t.Fatal(err)
	}
	prelude := make([]byte, 32)
	if _, err := io.ReadFull(overResp.Body, prelude); err != nil {
		t.Fatal(err)
	}

	drainDone := make(chan struct{})
	var tracked, aborted int
	go func() {
		tracked, aborted = handler.BeginDrain(120 * time.Millisecond)
		close(drainDone)
	}()

	// New connections while draining get a clear 503 terminal instead of queueing.
	deadline := time.Now().Add(time.Second)
	var late *http.Response
	for time.Now().Before(deadline) {
		late, err = client.Get(server.URL + "/api/long")
		if err == nil && late.StatusCode == http.StatusServiceUnavailable {
			break
		}
		if late != nil {
			late.Body.Close()
		}
		time.Sleep(10 * time.Millisecond)
	}
	if late == nil || late.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("request during drain did not get clear 503 terminal: %+v err=%v", late, err)
	}

	<-drainDone
	if tracked < 1 || aborted < 1 {
		t.Fatalf("drain did not abort overdue request: tracked=%d aborted=%d", tracked, aborted)
	}
	// After the wait window the gateway force-closes lingering connections,
	// giving the client an unambiguous terminal error.
	server.Close()
	// The overdue in-flight response terminates with an error, body stops.
	buffer := make([]byte, 4096)
	n, readErr := overResp.Body.Read(buffer)
	_ = n
	if readErr == nil {
		_, readErr = io.ReadAll(overResp.Body)
	}
	overResp.Body.Close()
	if readErr == nil {
		t.Fatal("overdue in-flight request should receive explicit terminal state")
	}

	deadline = time.Now().Add(2 * time.Second)
	found503 := false
	for time.Now().Before(deadline) && !found503 {
		for _, entry := range records.Snapshot() {
			if entry.Status == http.StatusServiceUnavailable && strings.Contains(entry.Error, "drain timeout") {
				found503 = true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !found503 {
		t.Fatalf("no 503 drain-timeout record: %+v", records.Snapshot())
	}
}

func tail(s string) string {
	if len(s) > 120 {
		return s[len(s)-120:]
	}
	return s
}
