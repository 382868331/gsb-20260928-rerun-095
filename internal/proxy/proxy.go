package proxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gateway/internal/config"
	"gateway/internal/record"
	"gateway/internal/release"
	"gateway/internal/state"
)

// ErrShutdownDeadline cancels in-flight requests that outlast the drain
// window so they reach a clear terminal state instead of being cut silently.
var ErrShutdownDeadline = errors.New("shutdown deadline exceeded")

var hopHeaders = []string{
	"Connection",
	"Proxy-Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
}

type Proxy struct {
	releases  *release.Store
	upstreams *state.Store
	records   *record.Store
	client    *http.Client

	draining atomic.Bool
	inflight sync.WaitGroup
	abortMu  sync.Mutex
	aborts   map[string]context.CancelCauseFunc
}

func New(releases *release.Store, upstreams *state.Store, records *record.Store) *Proxy {
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	return NewWithTransport(releases, upstreams, records, transport)
}

func NewWithTransport(releases *release.Store, upstreams *state.Store, records *record.Store, transport http.RoundTripper) *Proxy {
	return &Proxy{
		releases:  releases,
		upstreams: upstreams,
		records:   records,
		client:    &http.Client{Transport: transport},
		aborts:    make(map[string]context.CancelCauseFunc),
	}
}

// BeginDrain stops new requests from being accepted; in-flight requests keep
// their snapshots and finish normally within the drain window.
func (p *Proxy) BeginDrain() {
	p.draining.Store(true)
}

// WaitInflight reports whether all in-flight requests finished within timeout.
func (p *Proxy) WaitInflight(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		p.inflight.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// AbortInflight cancels every remaining in-flight request with
// ErrShutdownDeadline so each one is recorded with a clear terminal state.
func (p *Proxy) AbortInflight() {
	p.abortMu.Lock()
	defer p.abortMu.Unlock()
	for _, cancel := range p.aborts {
		cancel(ErrShutdownDeadline)
	}
}

func (p *Proxy) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	started := time.Now()
	entry := record.Record{
		ID:            newID(),
		StartedAt:     started,
		Method:        request.Method,
		Path:          request.URL.Path,
		Query:         request.URL.RawQuery,
		ClientAddress: request.RemoteAddr,
	}

	p.inflight.Add(1)
	defer p.inflight.Done()
	if p.draining.Load() {
		entry.Status = http.StatusServiceUnavailable
		entry.Duration = time.Since(started)
		entry.Error = "gateway draining"
		p.records.Add(entry)
		http.Error(writer, "gateway draining", http.StatusServiceUnavailable)
		return
	}

	currentSnapshot := p.releases.Current()
	if strings.HasPrefix(request.URL.Path, currentSnapshot.AdminPath) {
		http.NotFound(writer, request)
		return
	}

	ctx, cancel := context.WithCancelCause(request.Context())
	defer cancel(nil)
	p.abortMu.Lock()
	p.aborts[entry.ID] = cancel
	p.abortMu.Unlock()
	defer func() {
		p.abortMu.Lock()
		delete(p.aborts, entry.ID)
		p.abortMu.Unlock()
	}()
	request = request.WithContext(ctx)

	selection := p.releases.Select(request)
	if selection.PlanPending {
		entry.Status = http.StatusConflict
		entry.Duration = time.Since(started)
		entry.Error = release.ErrPlanPending.Error()
		entry.ReleaseID = selection.CandidateID
		p.records.Add(entry)
		http.Error(writer, release.ErrPlanPending.Error(), http.StatusConflict)
		return
	}
	snapshot := selection.Snapshot
	releaseID := selection.ID
	entry.ReleaseID = releaseID
	request.Header.Set(release.RehearsalHeader, releaseID)

	matched, ok := snapshot.Match(request)
	if !ok {
		entry.Status = http.StatusNotFound
		entry.Duration = time.Since(started)
		entry.Error = "no route matched"
		p.records.Add(entry)
		p.finishRehearsal(selection.CandidateID, request, entry)
		http.Error(writer, "no route matched", http.StatusNotFound)
		return
	}

	entry.RouteID = matched.Route.ID
	entry.Version = matched.Route.Version
	entry.UpstreamID = matched.Upstream.ID
	finish := p.upstreams.Begin(matched.Upstream.ID)

	status, attempts, bytesOut, errText, canceled, aborted := p.forward(writer, request, matched)
	entry.Status = status
	entry.Attempts = attempts
	entry.BytesIn = request.ContentLength
	entry.BytesOut = bytesOut
	entry.Error = errText
	entry.Canceled = canceled
	entry.Aborted = aborted
	entry.Duration = time.Since(started)
	p.records.Add(entry)
	p.finishRehearsal(selection.CandidateID, request, entry)
	finish(status < 500 && errText == "", errText)
}

// finishRehearsal feeds the terminal state of a rehearsal request back into
// the candidate's evidence. Only requests pinned to a staged candidate at
// entry are counted; normal traffic is ignored.
func (p *Proxy) finishRehearsal(candidateID string, request *http.Request, entry record.Record) {
	if candidateID == "" {
		return
	}
	p.releases.FinishRehearsal(candidateID, release.RehearsalOutcome{
		Status:     entry.Status,
		Error:      entry.Error,
		Canceled:   entry.Canceled,
		Aborted:    entry.Aborted,
		Method:     entry.Method,
		Path:       entry.Path,
		Header:     categoryHeaders(request.Header),
		RequestID:  entry.ID,
		SampleID:   request.Header.Get(release.SampleIDHeader),
		Supplement: request.Header.Get(release.SupplementHeader),
	})
}

// categoryHeaders forwards only request headers that category rules may
// inspect; gateway control headers must not classify traffic.
func categoryHeaders(header http.Header) http.Header {
	copied := make(http.Header)
	for name, values := range header {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "x-gateway-") {
			continue
		}
		copied[name] = append([]string(nil), values...)
	}
	return copied
}

func (p *Proxy) forward(writer http.ResponseWriter, request *http.Request, matched *config.Match) (int, int, int64, string, bool, bool) {
	requestBody, buffered := p.requestBody(request, matched.Route.Retry)
	maxAttempts := 1
	if canRetry(request.Method, matched.Route.Retry.Methods) {
		maxAttempts = matched.Route.Retry.MaxAttempts
	}

	var lastStatus int
	var bytesOut int64
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			p.upstreams.AddRetry(matched.Upstream.ID)
		}
		var attemptBody io.Reader
		if buffered {
			attemptBody = bytes.NewReader(requestBody.([]byte))
		} else if requestBody != nil {
			attemptBody = requestBody.(io.Reader)
		}

		upstreamRequest, cancel, err := p.buildRequest(request, matched, attemptBody, attempt)
		if err != nil {
			writeClientError(writer, http.StatusBadGateway, "bad gateway")
			return http.StatusBadGateway, attempt, bytesOut, err.Error(), false, false
		}
		response, err := p.client.Do(upstreamRequest)
		if err != nil {
			cancel()
			if errors.Is(context.Cause(request.Context()), ErrShutdownDeadline) {
				return http.StatusServiceUnavailable, attempt, bytesOut, ErrShutdownDeadline.Error(), false, true
			}
			if attempt < maxAttempts && matched.Route.Retry.RetryOnConnect && isRetryableRequestError(request.Context(), err) {
				lastStatus = http.StatusBadGateway
				continue
			}
			if errors.Is(request.Context().Err(), context.Canceled) {
				writeClientError(writer, 499, "client canceled")
				return 499, attempt, bytesOut, "client canceled", true, false
			}
			writeClientError(writer, http.StatusBadGateway, "bad gateway")
			return http.StatusBadGateway, attempt, bytesOut, err.Error(), false, false
		}

		if attempt < maxAttempts && containsStatus(matched.Route.Retry.RetryOnStatus, response.StatusCode) {
			io.CopyN(io.Discard, response.Body, 1<<20)
			response.Body.Close()
			cancel()
			lastStatus = response.StatusCode
			continue
		}

		copyHeaders(writer.Header(), response.Header)
		writer.WriteHeader(response.StatusCode)
		written, err := copyBody(newFlusher(writer, response), response.Body)
		bytesOut += written
		response.Body.Close()
		cancel()
		if err != nil {
			if errors.Is(context.Cause(request.Context()), ErrShutdownDeadline) {
				return response.StatusCode, attempt, bytesOut, ErrShutdownDeadline.Error(), false, true
			}
			if errors.Is(request.Context().Err(), context.Canceled) {
				return 499, attempt, bytesOut, "client canceled during response", true, false
			}
			return response.StatusCode, attempt, bytesOut, err.Error(), false, false
		}
		return response.StatusCode, attempt, bytesOut, "", false, false
	}

	writeClientError(writer, lastStatus, "bad gateway")
	return lastStatus, maxAttempts, bytesOut, "retry attempts exhausted", false, false
}

func writeClientError(writer http.ResponseWriter, status int, message string) {
	if status == 499 {
		return
	}
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	writer.WriteHeader(status)
	_, _ = io.WriteString(writer, message+"\n")
}

func (p *Proxy) buildRequest(request *http.Request, matched *config.Match, body io.Reader, attempt int) (*http.Request, context.CancelFunc, error) {
	base, err := url.Parse(matched.Upstream.BaseURL)
	if err != nil {
		return nil, nil, err
	}
	requestPath := request.URL.Path
	if matched.Route.StripPrefix {
		prefix := strings.TrimSuffix(matched.Route.Prefix, "/")
		remaining := strings.TrimPrefix(requestPath, prefix)
		if remaining == "" {
			remaining = "/"
		}
		requestPath = remaining
	}
	upstreamURL := *base
	upstreamURL.Path = path.Join(base.Path, requestPath)
	upstreamURL.RawQuery = request.URL.RawQuery

	upstreamRequest, err := http.NewRequestWithContext(request.Context(), request.Method, upstreamURL.String(), body)
	if err != nil {
		return nil, nil, err
	}
	copyHeaders(upstreamRequest.Header, request.Header)
	removeHopHeaders(upstreamRequest.Header)
	upstreamRequest.Header.Set("X-Forwarded-For", request.RemoteAddr)
	upstreamRequest.Header.Set("X-Forwarded-Proto", scheme(request))
	upstreamRequest.Header.Set("X-Gateway-Route", matched.Route.ID)
	upstreamRequest.Header.Set("X-Gateway-Version", matched.Route.Version)
	upstreamRequest.Header.Set("X-Gateway-Attempt", strconv.Itoa(attempt))

	timeout := time.Duration(matched.Upstream.Timeout)
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithCancelCause(upstreamRequest.Context())
	timer := time.AfterFunc(timeout, func() {
		cancel(context.DeadlineExceeded)
	})
	upstreamRequest = upstreamRequest.WithContext(ctx)
	return upstreamRequest, func() {
		timer.Stop()
		cancel(nil)
	}, nil
}

func (p *Proxy) requestBody(request *http.Request, policy config.RetryPolicy) (any, bool) {
	if request.Body == nil || request.Body == http.NoBody || !canRetry(request.Method, policy.Methods) {
		return io.Reader(request.Body), false
	}
	data, err := io.ReadAll(request.Body)
	if err != nil {
		return request.Body, false
	}
	request.Body.Close()
	return data, true
}

func canRetry(method string, methods []string) bool {
	for _, allowed := range methods {
		if strings.EqualFold(method, allowed) {
			return true
		}
	}
	return false
}

func containsStatus(statuses []int, status int) bool {
	for _, candidate := range statuses {
		if candidate == status {
			return true
		}
	}
	return false
}

func isRetryableRequestError(ctx context.Context, err error) bool {
	return ctx.Err() == nil
}

func copyHeaders(destination, source http.Header) {
	for name, values := range source {
		for _, value := range values {
			destination.Add(name, value)
		}
	}
	removeHopHeaders(destination)
}

func removeHopHeaders(headers http.Header) {
	for _, header := range hopHeaders {
		headers.Del(header)
	}
}

type flusher struct {
	writer    http.ResponseWriter
	flusher   http.Flusher
	streaming bool
}

func newFlusher(writer http.ResponseWriter, response *http.Response) *flusher {
	f := &flusher{writer: writer}
	if response.ContentLength != 0 {
		f.flusher, _ = writer.(http.Flusher)
	}
	return f
}

func (f *flusher) Write(data []byte) (int, error) {
	n, err := f.writer.Write(data)
	if err == nil && f.flusher != nil {
		f.flusher.Flush()
	}
	return n, err
}

func copyBody(destination io.Writer, source io.Reader) (int64, error) {
	return io.Copy(destination, source)
}

func scheme(request *http.Request) string {
	if request.TLS != nil {
		return "https"
	}
	return "http"
}

func newID() string {
	data := make([]byte, 8)
	_, _ = rand.Read(data)
	return strconv.FormatInt(time.Now().UnixNano(), 16) + "-" + hex.EncodeToString(data)
}
