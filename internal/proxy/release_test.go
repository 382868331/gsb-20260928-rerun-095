package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gateway/internal/config"
	"gateway/internal/record"
	"gateway/internal/release"
	"gateway/internal/state"
)

// hostTransport answers each request with the upstream host name, so tests
// can tell exactly which upstream (and therefore which release) served it.
func hostTransport(extra map[string]http.RoundTripper) http.RoundTripper {
	return roundTripFunc(func(request *http.Request) (*http.Response, error) {
		host := request.URL.Host
		if rt, ok := extra[host]; ok {
			return rt.RoundTrip(request)
		}
		return textResponse(http.StatusOK, host), nil
	})
}

func hostSnapshot(host string) *config.Snapshot {
	cfg := &config.Config{
		Listen:    "127.0.0.1:0",
		AdminPath: "/__gateway",
		Upstreams: []config.Upstream{{ID: "up-" + host, BaseURL: "http://" + host, Timeout: config.Duration(5 * time.Second)}},
		Routes: []config.Route{{
			ID:          "api",
			Version:     host,
			Prefix:      "/v1/",
			UpstreamID:  "up-" + host,
			StripPrefix: true,
		}},
	}
	snapshot, err := cfg.Snapshot()
	if err != nil {
		panic(err)
	}
	return snapshot
}

func newReleaseProxy() (*Proxy, *release.Store, *record.Store) {
	releases := release.NewStore(hostSnapshot("old.example"))
	records := record.NewStore(100)
	handler := NewWithTransport(releases, state.NewStore(), records, hostTransport(nil))
	return handler, releases, records
}

func get(handler http.Handler, path string, headers map[string]string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodGet, path, nil)
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func TestCandidateRehearsalConfirmAndRollbackAttribution(t *testing.T) {
	handler, releases, records := newReleaseProxy()

	candidate := releases.Stage(hostSnapshot("new.example"))

	// Confirmation before any rehearsal is rejected by the release gate.
	if _, err := releases.Confirm(candidate.ID); err == nil {
		t.Fatal("fresh candidate was confirmed without rehearsal")
	}

	// Normal traffic keeps using the active release.
	if body := get(handler, "/v1/echo", nil).Body.String(); body != "old.example" {
		t.Fatalf("active body = %q", body)
	}
	// Rehearsal traffic pinned to the candidate uses the candidate snapshot.
	rehearsal := get(handler, "/v1/echo", map[string]string{release.RehearsalHeader: candidate.ID})
	if body := rehearsal.Body.String(); body != "new.example" {
		t.Fatalf("rehearsal body = %q", body)
	}
	entries := records.Snapshot()
	if entries[0].ReleaseID != "rel-1" || entries[1].ReleaseID != candidate.ID {
		t.Fatalf("release attribution = %s, %s", entries[0].ReleaseID, entries[1].ReleaseID)
	}
	if state := releases.Candidates()[0].Readiness; !state.Ready || state.State != release.StateReady {
		t.Fatalf("rehearsal readiness = %+v", state)
	}

	// Confirm switches all traffic to the candidate configuration.
	if _, err := releases.Confirm(candidate.ID); err != nil {
		t.Fatal(err)
	}
	if body := get(handler, "/v1/echo", nil).Body.String(); body != "new.example" {
		t.Fatalf("post-confirm body = %q", body)
	}

	// Rollback appends a new release record and restores the old config.
	rolledBack, err := releases.Rollback("")
	if err != nil {
		t.Fatal(err)
	}
	if rolledBack.Kind != release.KindRollback {
		t.Fatalf("rollback kind = %s", rolledBack.Kind)
	}
	if body := get(handler, "/v1/echo", nil).Body.String(); body != "old.example" {
		t.Fatalf("post-rollback body = %q", body)
	}
	entries = records.Snapshot()
	if entries[3].ReleaseID != rolledBack.ID {
		t.Fatalf("post-rollback attribution = %s, want %s", entries[3].ReleaseID, rolledBack.ID)
	}
	if len(releases.History()) != 3 {
		t.Fatal("release history lost the failed release")
	}
}

func TestInFlightLongResponseCompletesAcrossConfirm(t *testing.T) {
	streamReader, streamWriter := io.Pipe()
	oldTransport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		response := textResponse(http.StatusOK, "")
		response.Body = streamReader
		response.ContentLength = -1
		return response, nil
	})
	releases := release.NewStore(hostSnapshot("old.example"))
	records := record.NewStore(100)
	handler := NewWithTransport(releases, state.NewStore(), records,
		hostTransport(map[string]http.RoundTripper{"old.example": oldTransport}))

	clientReader, clientWriter := io.Pipe()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(pipeResponseWriter{writer: clientWriter}, httptest.NewRequest(http.MethodGet, "/v1/long", nil))
		close(done)
	}()

	go func() {
		io.WriteString(streamWriter, "old-chunk-1\n")
	}()
	buffer := make([]byte, 64)
	n, err := clientReader.Read(buffer)
	if err != nil || !strings.Contains(string(buffer[:n]), "old-chunk-1") {
		t.Fatalf("first read n=%d err=%v data=%q", n, err, buffer[:n])
	}

	// Confirm a new release while the long response is in flight.
	candidate := releases.Stage(hostSnapshot("new.example"))
	// A successful rehearsal must exist before the release is allowed.
	if _, err := releases.Confirm(candidate.ID); err == nil {
		t.Fatal("candidate confirmed without rehearsal")
	}
	rehearsal := get(handler, "/v1/echo", map[string]string{release.RehearsalHeader: candidate.ID})
	if body := rehearsal.Body.String(); body != "new.example" {
		t.Fatalf("rehearsal body = %q", body)
	}
	if _, err := releases.Confirm(candidate.ID); err != nil {
		t.Fatalf("confirm after rehearsal: %v", err)
	}

	// The in-flight response keeps streaming from the old upstream only.
	go io.WriteString(streamWriter, "old-chunk-2\n")
	n, err = clientReader.Read(buffer)
	if err != nil || !strings.Contains(string(buffer[:n]), "old-chunk-2") {
		t.Fatalf("second read n=%d err=%v data=%q", n, err, buffer[:n])
	}
	streamWriter.Close()
	<-done

	var entry record.Record
	for _, recorded := range records.Snapshot() {
		if recorded.Path == "/v1/long" {
			entry = recorded
		}
	}
	if entry.ReleaseID != "rel-1" || entry.Error != "" || entry.Canceled || entry.Aborted {
		t.Fatalf("in-flight entry = %+v", entry)
	}
	if body := get(handler, "/v1/echo", nil).Body.String(); body != "new.example" {
		t.Fatalf("post-confirm body = %q", body)
	}
}

// A request in flight when a rollback happens stays bound to the release
// snapshot it entered with; the rollback only affects later requests. Both
// releases stay present in history.
func TestInFlightRequestPinnedAcrossRollback(t *testing.T) {
	streamReader, streamWriter := io.Pipe()
	newTransport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/echo" {
			return textResponse(http.StatusOK, "new.example"), nil
		}
		response := textResponse(http.StatusOK, "")
		response.Body = streamReader
		response.ContentLength = -1
		return response, nil
	})
	releases := release.NewStore(hostSnapshot("old.example"))
	records := record.NewStore(100)
	handler := NewWithTransport(releases, state.NewStore(), records,
		hostTransport(map[string]http.RoundTripper{"new.example": newTransport}))

	candidate := releases.Stage(hostSnapshot("new.example"))
	if body := get(handler, "/v1/echo", map[string]string{release.RehearsalHeader: candidate.ID}).Body.String(); body != "new.example" {
		t.Fatalf("rehearsal body = %q", body)
	}
	promoted, err := releases.Confirm(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}

	clientReader, clientWriter := io.Pipe()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(pipeResponseWriter{writer: clientWriter}, httptest.NewRequest(http.MethodGet, "/v1/long", nil))
		close(done)
	}()
	buffer := make([]byte, 64)
	go func() { io.WriteString(streamWriter, "new-chunk\n") }()
	if n, err := clientReader.Read(buffer); err != nil || !strings.Contains(string(buffer[:n]), "new-chunk") {
		t.Fatalf("first read n=%d err=%v", n, err)
	}

	rolledBack, err := releases.Rollback("")
	if err != nil {
		t.Fatal(err)
	}
	go io.WriteString(streamWriter, "still-new\n")
	if n, err := clientReader.Read(buffer); err != nil || !strings.Contains(string(buffer[:n]), "still-new") {
		t.Fatalf("rollback switched in-flight upstream: n=%d err=%v data=%q", n, err, buffer[:n])
	}
	streamWriter.Close()
	<-done

	if body := get(handler, "/v1/echo", nil).Body.String(); body != "old.example" {
		t.Fatalf("post-rollback body = %q", body)
	}
	var long record.Record
	for _, recorded := range records.Snapshot() {
		if recorded.Path == "/v1/long" {
			long = recorded
		}
	}
	if long.ReleaseID != promoted.ID {
		t.Fatalf("in-flight release = %s, want %s", long.ReleaseID, promoted.ID)
	}
	if rolledBack.SourceID != "rel-1" {
		t.Fatalf("rollback source = %s", rolledBack.SourceID)
	}
	if history := releases.History(); len(history) != 3 {
		t.Fatalf("history length = %d", len(history))
	}
}

func TestFailedRehearsalBlocksConfirm(t *testing.T) {
	failTransport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusBadGateway,
			Body:       io.NopCloser(strings.NewReader("boom")),
			Header:     make(http.Header),
		}, nil
	})
	releases := release.NewStore(hostSnapshot("old.example"))
	records := record.NewStore(100)
	failHandler := NewWithTransport(releases, state.NewStore(), records,
		hostTransport(map[string]http.RoundTripper{"new.example": failTransport}))
	candidate := releases.Stage(hostSnapshot("new.example"))
	get(failHandler, "/v1/echo", map[string]string{release.RehearsalHeader: candidate.ID})
	if readiness := releases.Candidates()[0].Readiness; readiness.Ready || readiness.Summary.Failed != 1 {
		t.Fatalf("failed rehearsal readiness = %+v", readiness)
	}
	if _, err := releases.Confirm(candidate.ID); err == nil {
		t.Fatal("candidate confirmed after failed rehearsal")
	}
}

func TestClientCancelDuringCandidateRehearsal(t *testing.T) {
	streamStarted := make(chan struct{})
	oldTransport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		reader, writer := io.Pipe()
		go func() {
			close(streamStarted)
			io.WriteString(writer, "rehearsal-chunk\n")
			<-request.Context().Done()
			writer.CloseWithError(context.Canceled)
		}()
		response := textResponse(http.StatusOK, "")
		response.Body = reader
		response.ContentLength = -1
		return response, nil
	})
	releases := release.NewStore(hostSnapshot("old.example"))
	records := record.NewStore(100)
	handler := NewWithTransport(releases, state.NewStore(), records,
		hostTransport(map[string]http.RoundTripper{"new.example": oldTransport}))
	candidate := releases.Stage(hostSnapshot("new.example"))

	ctx, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodGet, "/v1/long", nil).WithContext(ctx)
	request.Header.Set(release.RehearsalHeader, candidate.ID)
	clientReader, clientWriter := io.Pipe()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(pipeResponseWriter{writer: clientWriter}, request)
		close(done)
	}()
	<-streamStarted
	buffer := make([]byte, 64)
	if _, err := clientReader.Read(buffer); err != nil {
		t.Fatal(err)
	}
	cancel()
	<-done

	entry := records.Snapshot()[0]
	if !entry.Canceled || entry.ReleaseID != candidate.ID || entry.Status != 499 {
		t.Fatalf("canceled rehearsal entry = %+v", entry)
	}
}

func TestDrainFinishesInFlightAndAbortsOvertime(t *testing.T) {
	releaseStream := make(chan struct{})
	streamTransport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		reader, writer := io.Pipe()
		go func() {
			io.WriteString(writer, "chunk\n")
			select {
			case <-releaseStream:
				writer.Close()
			case <-request.Context().Done():
				writer.CloseWithError(request.Context().Err())
			}
		}()
		response := textResponse(http.StatusOK, "")
		response.Body = reader
		response.ContentLength = -1
		return response, nil
	})
	releases := release.NewStore(hostSnapshot("old.example"))
	records := record.NewStore(100)
	handler := NewWithTransport(releases, state.NewStore(), records,
		hostTransport(map[string]http.RoundTripper{"old.example": streamTransport}))

	startLong := func() (chan struct{}, *io.PipeReader) {
		clientReader, clientWriter := io.Pipe()
		done := make(chan struct{})
		go func() {
			handler.ServeHTTP(pipeResponseWriter{writer: clientWriter}, httptest.NewRequest(http.MethodGet, "/v1/long", nil))
			close(done)
		}()
		buffer := make([]byte, 16)
		if _, err := clientReader.Read(buffer); err != nil {
			t.Fatal(err)
		}
		return done, clientReader
	}

	// In-flight request finishes normally inside the drain window.
	firstDone, _ := startLong()
	handler.BeginDrain()
	close(releaseStream)
	if !handler.WaitInflight(2 * time.Second) {
		t.Fatal("in-flight request did not finish inside the drain window")
	}
	<-firstDone
	entry := records.Snapshot()[0]
	if entry.Aborted || entry.Canceled || entry.Error != "" {
		t.Fatalf("drained entry = %+v", entry)
	}

	// New requests during drain get a clear terminal state.
	rejected := get(handler, "/v1/echo", nil)
	if rejected.Code != http.StatusServiceUnavailable || !strings.Contains(rejected.Body.String(), "draining") {
		t.Fatalf("drain rejection = %d %q", rejected.Code, rejected.Body.String())
	}
}

func TestDrainAbortBeyondDeadlineRecordsTerminalState(t *testing.T) {
	streamTransport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		reader, writer := io.Pipe()
		go func() {
			io.WriteString(writer, "chunk\n")
			<-request.Context().Done()
			writer.CloseWithError(request.Context().Err())
		}()
		response := textResponse(http.StatusOK, "")
		response.Body = reader
		response.ContentLength = -1
		return response, nil
	})
	releases := release.NewStore(hostSnapshot("old.example"))
	records := record.NewStore(100)
	handler := NewWithTransport(releases, state.NewStore(), records,
		hostTransport(map[string]http.RoundTripper{"old.example": streamTransport}))

	clientReader, clientWriter := io.Pipe()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(pipeResponseWriter{writer: clientWriter}, httptest.NewRequest(http.MethodGet, "/v1/long", nil))
		close(done)
	}()
	buffer := make([]byte, 16)
	if _, err := clientReader.Read(buffer); err != nil {
		t.Fatal(err)
	}

	handler.BeginDrain()
	if handler.WaitInflight(50 * time.Millisecond) {
		t.Fatal("long request finished before the drain deadline")
	}
	handler.AbortInflight()
	if !handler.WaitInflight(2 * time.Second) {
		t.Fatal("aborted request did not terminate")
	}
	<-done

	entry := records.Snapshot()[0]
	if !entry.Aborted || entry.Error != ErrShutdownDeadline.Error() || entry.Canceled {
		t.Fatalf("aborted entry = %+v", entry)
	}
}
