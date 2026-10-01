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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestRetryOnlyBeforeResponseAndWithoutDuplicateBody(t *testing.T) {
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Header.Get("X-Gateway-Attempt") == "1" {
			return textResponse(http.StatusServiceUnavailable, "temporary failure\n"), nil
		}
		return textResponse(http.StatusOK, "recovered"), nil
	})
	proxyHandler, records := newTestProxy(transport)
	recorder := httptest.NewRecorder()
	proxyHandler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/flaky", nil))

	if recorder.Code != http.StatusOK || recorder.Body.String() != "recovered" {
		t.Fatalf("code=%d body=%q", recorder.Code, recorder.Body.String())
	}
	if records.Snapshot()[0].Attempts != 2 {
		t.Fatal("request was not retried")
	}
}

func TestNonRetryableMethodReceivesOneFailure(t *testing.T) {
	requests := 0
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		return textResponse(http.StatusServiceUnavailable, "temporary failure\n"), nil
	})
	proxyHandler, records := newTestProxy(transport)
	recorder := httptest.NewRecorder()
	proxyHandler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/flaky", strings.NewReader("payload")))

	if recorder.Code != http.StatusServiceUnavailable || requests != 1 || recorder.Body.String() != "temporary failure\n" {
		t.Fatalf("code=%d requests=%d body=%q", recorder.Code, requests, recorder.Body.String())
	}
	if records.Snapshot()[0].Attempts != 1 {
		t.Fatal("POST was retried")
	}
}

func TestCancelAfterFirstByteDoesNotSpliceAnotherResponse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		reader, writer := io.Pipe()
		go func() {
			io.WriteString(writer, "first-chunk\n")
			for {
				select {
				case <-request.Context().Done():
					writer.CloseWithError(context.Canceled)
					return
				case <-time.After(time.Millisecond):
					io.WriteString(writer, "more\n")
				}
			}
		}()
		response := textResponse(http.StatusOK, "")
		response.Body = reader
		response.ContentLength = -1
		return response, nil
	})
	proxyHandler, records := newTestProxy(transport)
	clientReader, clientWriter := io.Pipe()
	request := httptest.NewRequest(http.MethodGet, "/v1/long", nil).WithContext(ctx)
	done := make(chan struct{})
	go func() {
		proxyHandler.ServeHTTP(pipeResponseWriter{writer: clientWriter}, request)
		close(done)
	}()

	buffer := make([]byte, 64)
	n, err := clientReader.Read(buffer)
	if err != nil || !strings.Contains(string(buffer[:n]), "first-chunk") {
		t.Fatalf("n=%d err=%v data=%q", n, err, buffer[:n])
	}
	cancel()
	<-done

	entry := records.Snapshot()[0]
	if !entry.Canceled || entry.Attempts != 1 || strings.Count(string(buffer[:n]), "first-chunk") != 1 {
		t.Fatalf("unexpected canceled entry: %+v data=%q", entry, buffer[:n])
	}
}

func TestInFlightRequestKeepsOldSnapshot(t *testing.T) {
	reachedOld := make(chan struct{})
	releaseOld := make(chan struct{})
	oldTransport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		close(reachedOld)
		<-releaseOld
		return textResponse(http.StatusOK, "old-upstream"), nil
	})
	newTransport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return textResponse(http.StatusOK, "new-upstream"), nil
	})
	releases := release.NewStore(mustSnapshot(t))
	upstreams := state.NewStore()
	records := record.NewStore(100)
	handler := NewWithTransport(releases, upstreams, records, oldTransport)

	firstDone := make(chan httptest.ResponseRecorder)
	go func() {
		destination := httptest.NewRecorder()
		handler.ServeHTTP(destination, httptest.NewRequest(http.MethodGet, "/v1/echo/ping", nil))
		firstDone <- *destination
	}()
	<-reachedOld
	candidate := releases.Stage(mustSnapshot(t))
	handler.client = &http.Client{Transport: newTransport}
	rehearsal := httptest.NewRequest(http.MethodGet, "/v1/echo/ping", nil)
	rehearsal.Header.Set(release.RehearsalHeader, candidate.ID)
	rehearsalRecorder := httptest.NewRecorder()
	handler.ServeHTTP(rehearsalRecorder, rehearsal)
	if rehearsalRecorder.Body.String() != "new-upstream" {
		t.Fatalf("rehearsal body = %q", rehearsalRecorder.Body.String())
	}
	if _, err := releases.Confirm(candidate.ID); err != nil {
		t.Fatal(err)
	}
	close(releaseOld)
	oldResult := <-firstDone
	if oldResult.Body.String() != "old-upstream" {
		t.Fatalf("in-flight body = %q", oldResult.Body.String())
	}

	next := httptest.NewRecorder()
	handler.ServeHTTP(next, httptest.NewRequest(http.MethodGet, "/v1/echo/ping", nil))
	if next.Body.String() != "new-upstream" {
		t.Fatalf("new request body = %q", next.Body.String())
	}
}

type pipeResponseWriter struct {
	writer *io.PipeWriter
}

func (w pipeResponseWriter) Header() http.Header { return http.Header{} }
func (w pipeResponseWriter) WriteHeader(int)     {}
func (w pipeResponseWriter) Write(data []byte) (int, error) {
	return w.writer.Write(data)
}
func (w pipeResponseWriter) Flush() {}

func textResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode:    status,
		Header:        make(http.Header),
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

func newTestProxy(transport http.RoundTripper) (http.Handler, *record.Store) {
	releases := release.NewStore(mustSnapshot(nil))
	upstreams := state.NewStore()
	records := record.NewStore(100)
	return NewWithTransport(releases, upstreams, records, transport), records
}

func mustSnapshot(t testing.TB) *config.Snapshot {
	if t != nil {
		t.Helper()
	}
	cfg := &config.Config{
		Listen:    "127.0.0.1:0",
		AdminPath: "/__gateway",
		Upstreams: []config.Upstream{{ID: "local", BaseURL: "http://local.example", Timeout: config.Duration(5 * time.Second)}},
		Routes: []config.Route{{
			ID:          "v1",
			Version:     "v1",
			Prefix:      "/v1/",
			UpstreamID:  "local",
			StripPrefix: true,
			Retry: config.RetryPolicy{
				Methods:        []string{http.MethodGet, http.MethodHead},
				MaxAttempts:    2,
				RetryOnStatus:  []int{http.StatusServiceUnavailable, http.StatusBadGateway, http.StatusGatewayTimeout},
				RetryOnConnect: true,
			},
		}},
	}
	snapshot, err := cfg.Snapshot()
	if err != nil {
		if t != nil {
			t.Fatal(err)
		}
		panic(err)
	}
	return snapshot
}
