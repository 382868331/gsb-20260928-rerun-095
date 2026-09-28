package gateway_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"gateway/internal/config"
	"gateway/internal/gateway"
)

type fakeTransport struct {
	handlers map[string]http.Handler
	holding  *holdingTransport
}

func (t *fakeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Host == "old.local" && t.holding != nil {
		return t.holding.RoundTrip(req)
	}
	handler := t.handlers[req.URL.Host]
	if handler == nil {
		return nil, fmt.Errorf("dial tcp %s: connect: connection refused", req.URL.Host)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	result := recorder.Result()
	delete(result.Header, "Content-Length")
	result.ContentLength = int64(recorder.Body.Len())
	result.Request = req
	return result, nil
}

type holdingTransport struct {
	startRequests chan chan struct{}
	releases      chan chan struct{}
}

func newHoldingTransport() *holdingTransport {
	return &holdingTransport{
		startRequests: make(chan chan struct{}),
		releases:      make(chan chan struct{}),
	}
}

func (t *holdingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	reader, writer := io.Pipe()
	started := make(chan struct{})
	release := make(chan struct{})
	t.startRequests <- started
	t.releases <- release
	go func() {
		close(started)
		<-release
		_, _ = io.WriteString(writer, "-old-finished")
		_ = writer.Close()
	}()
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: reader, Request: req}, nil
}

func newConfig(maxAttempts int) *config.Config {
	upstreamIDs := []string{"demo"}
	upstreams := []config.Upstream{{ID: "demo", URL: "http://demo.local"}}
	if maxAttempts > 1 {
		upstreamIDs = []string{"closed", "demo"}
		upstreams = append([]config.Upstream{{ID: "closed", URL: "http://closed.invalid"}}, upstreams...)
	}
	return &config.Config{
		ListenAddr: "127.0.0.1:0",
		AdminPath:  "/__gateway__",
		Routes: []config.Route{{
			ID:           "demo",
			Version:      "v1",
			Prefix:       "/v1/demo",
			UpstreamIDs:  upstreamIDs,
			RetryEnabled: true,
			RetryMethods: []string{http.MethodGet},
			MaxAttempts:  maxAttempts,
		}},
		Upstreams: upstreams,
	}
}

func newGateway(t *testing.T, cfg *config.Config, transport http.RoundTripper) (*gateway.Gateway, http.Handler) {
	t.Helper()
	g, err := gateway.NewWithRoundTripper(cfg, transport)
	if err != nil {
		t.Fatal(err)
	}
	return g, g.Handler()
}

func doRequest(t *testing.T, handler http.Handler, method, target string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, body)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	return recorder
}

func TestRetryOnlyConfiguredMethod(t *testing.T) {
	var mu sync.Mutex
	var methods []string
	transport := &fakeTransport{handlers: map[string]http.Handler{
		"demo.local": http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			mu.Lock()
			methods = append(methods, req.Method)
			mu.Unlock()
			_, _ = io.WriteString(w, "ok")
		}),
	}}
	_, handler := newGateway(t, newConfig(2), transport)

	getResponse := doRequest(t, handler, http.MethodGet, "/v1/demo", nil)
	if getResponse.Code != http.StatusOK || getResponse.Body.String() != "ok" {
		t.Fatalf("unexpected GET result: %d %q", getResponse.Code, getResponse.Body.String())
	}
	postResponse := doRequest(t, handler, http.MethodPost, "/v1/demo", strings.NewReader("body"))
	if postResponse.Code != http.StatusBadGateway {
		t.Fatalf("unexpected POST status: %d", postResponse.Code)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(methods) != 1 || methods[0] != http.MethodGet {
		t.Fatalf("expected only one GET upstream call, got %v", methods)
	}
}

func TestLongResponseIsNotDuplicated(t *testing.T) {
	const chunkSize = 4096
	const chunkCount = 256
	transport := &fakeTransport{handlers: map[string]http.Handler{
		"demo.local": http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusOK)
			chunk := bytes.Repeat([]byte("abcdefgh"), chunkSize/8)
			for range chunkCount {
				_, _ = w.Write(chunk)
			}
		}),
	}}
	_, handler := newGateway(t, newConfig(1), transport)

	response := doRequest(t, handler, http.MethodGet, "/v1/demo/stream?x=1", nil)
	expectedLength := chunkSize * chunkCount
	body := response.Body.Bytes()
	if response.Code != http.StatusOK || len(body) != expectedLength {
		t.Fatalf("unexpected response: status=%d length=%d", response.Code, len(body))
	}
	if bytes.Count(body, []byte("abcdefgh")) != expectedLength/8 {
		t.Fatal("response content was duplicated or corrupted")
	}
}

type contextAwareTransport struct {
	started  chan struct{}
	canceled chan struct{}
}

func (t *contextAwareTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	reader, writer := io.Pipe()
	go func() {
		close(t.started)
		_, _ = writer.Write([]byte("x"))
		<-req.Context().Done()
		close(t.canceled)
		_ = writer.Close()
	}()
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       reader,
		Request:    req,
	}, nil
}

func TestClientCancellationClosesUpstreamRequest(t *testing.T) {
	transport := &contextAwareTransport{started: make(chan struct{}), canceled: make(chan struct{})}
	_, handler := newGateway(t, newConfig(1), transport)

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/v1/demo/cancel", nil).WithContext(ctx)
	finished := make(chan struct{})
	recorder := streamRecorder{buf: &safeBuffer{}}
	go func() {
		handler.ServeHTTP(recorder, req)
		close(finished)
	}()
	<-transport.started
	requireEventual(t, func() bool { return recorder.buf.Len() > 0 }, "expected response to start")
	cancel()
	select {
	case <-transport.canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream request remained open after client cancellation")
	}
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("gateway request did not finish after cancellation")
	}
}

func TestConfigReplacementAndRollbackKeepsInFlightSnapshot(t *testing.T) {
	holding := newHoldingTransport()
	transport := &fakeTransport{handlers: map[string]http.Handler{
		"old.local": http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		}),
		"new.local": http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Header().Set("X-Snapshot", "new")
			_, _ = io.WriteString(w, "new")
		}),
		"rollback.local": http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Header().Set("X-Snapshot", "rollback")
			_, _ = io.WriteString(w, "rollback")
		}),
	}, holding: holding}
	cfg := newConfig(1)
	cfg.Upstreams[0].URL = "http://old.local"
	g, handler := newGateway(t, cfg, transport)

	inFlightBody := &safeBuffer{}
	inFlightDone := make(chan struct{})
	go func() {
		req := httptest.NewRequest(http.MethodGet, "/v1/demo/flight", nil)
		handler.ServeHTTP(streamRecorder{buf: inFlightBody}, req)
		close(inFlightDone)
	}()
	started := <-holding.startRequests
	release := <-holding.releases
	<-started

	nextConfig := newConfig(1)
	nextConfig.Upstreams[0].URL = "http://new.local"
	nextConfig.Routes[0].Version = "v2"
	replaceConfig(t, handler, nextConfig)

	newResponse := doRequest(t, handler, http.MethodGet, "/v1/demo/new", nil)
	if newResponse.Header().Get("X-Snapshot") != "new" || newResponse.Body.String() != "new" {
		t.Fatalf("new snapshot not used: %q %q", newResponse.Header().Get("X-Snapshot"), newResponse.Body.String())
	}

	rollbackConfig := newConfig(1)
	rollbackConfig.Upstreams[0].URL = "http://rollback.local"
	replaceConfig(t, handler, rollbackConfig)

	rollbackResponse := doRequest(t, handler, http.MethodGet, "/v1/demo/rollback", nil)
	if rollbackResponse.Header().Get("X-Snapshot") != "rollback" || rollbackResponse.Body.String() != "rollback" {
		t.Fatalf("rollback snapshot not used: %q %q", rollbackResponse.Header().Get("X-Snapshot"), rollbackResponse.Body.String())
	}

	invalidConfig := newConfig(1)
	invalidConfig.Upstreams = nil
	replaceConfigExpectError(t, handler, invalidConfig)

	close(release)
	select {
	case <-inFlightDone:
	case <-time.After(2 * time.Second):
		t.Fatal("old connection did not finish")
	}
	if inFlightBody.String() != "-old-finished" {
		t.Fatalf("in-flight response was corrupted: %q", inFlightBody.String())
	}
	_ = g
}

type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(data)
}

func (b *safeBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type streamRecorder struct {
	buf *safeBuffer
}

func (r streamRecorder) Header() http.Header            { return make(http.Header) }
func (r streamRecorder) Write(data []byte) (int, error) { return r.buf.Write(data) }
func (r streamRecorder) WriteHeader(int)                {}
func (r streamRecorder) Flush()                         {}

func replaceConfig(t *testing.T, handler http.Handler, cfg *config.Config) {
	t.Helper()
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	response := doRequest(t, handler, http.MethodPut, "/__gateway__/config", bytes.NewReader(data))
	if response.Code != http.StatusOK {
		t.Fatalf("replace config failed: %d %s", response.Code, response.Body.String())
	}
}

func replaceConfigExpectError(t *testing.T, handler http.Handler, cfg *config.Config) {
	t.Helper()
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	response := doRequest(t, handler, http.MethodPut, "/__gateway__/config", bytes.NewReader(data))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected invalid config rejection, got %d %s", response.Code, response.Body.String())
	}
}

func TestVersionedRoutingAndRecords(t *testing.T) {
	transport := &fakeTransport{handlers: map[string]http.Handler{
		"demo.local": http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Header().Set("X-Path", req.URL.Path)
			fmt.Fprintf(w, "path=%s query=%s", req.URL.Path, req.URL.RawQuery)
		}),
	}}
	_, handler := newGateway(t, newConfig(1), transport)
	response := doRequest(t, handler, http.MethodGet, "/v1/demo/orders/42?foo=bar", nil)
	if response.Header().Get("X-Path") != "/orders/42" || response.Body.String() != "path=/orders/42 query=foo=bar" {
		t.Fatalf("unexpected forwarded path/body: %q %q", response.Header().Get("X-Path"), response.Body.String())
	}

	recordsResponse := doRequest(t, handler, http.MethodGet, "/__gateway__/records", nil)
	var payload struct {
		Records []struct {
			RouteID      string   `json:"route_id"`
			RouteVersion string   `json:"route_version"`
			StatusCode   int      `json:"status_code"`
			UpstreamIDs  []string `json:"upstream_ids"`
		} `json:"records"`
	}
	if err := json.Unmarshal(recordsResponse.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Records) != 1 || payload.Records[0].RouteVersion != "v1" || payload.Records[0].StatusCode != 200 {
		t.Fatalf("unexpected records: %+v", payload.Records)
	}
}

func requireEventual(t *testing.T, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(message)
}
