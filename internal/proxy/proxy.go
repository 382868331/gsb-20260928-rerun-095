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
	"time"

	"gateway/internal/config"
	"gateway/internal/record"
	"gateway/internal/release"
	"gateway/internal/state"
)

const CanaryRevisionHeader = "X-Gateway-Canary-Revision"

var errDrainTimeout = errors.New("gateway drain timeout exceeded")

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
	configs   *config.Store
	upstreams *state.Store
	records   *record.Store
	releases  *release.Manager
	client    *http.Client
	activeMu  sync.Mutex
	active    map[*inFlight]struct{}
	stopping  bool
}

type inFlight struct {
	releaseID string
	revision  string
	canary    bool
	cancel    context.CancelCauseFunc
}

func New(configs *config.Store, upstreams *state.Store, records *record.Store, releases *release.Manager) *Proxy {
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	return &Proxy{
		configs:   configs,
		upstreams: upstreams,
		records:   records,
		releases:  releases,
		client:    &http.Client{Transport: transport},
		active:    make(map[*inFlight]struct{}),
	}
}

func NewWithTransport(configs *config.Store, upstreams *state.Store, records *record.Store, releases *release.Manager, transport http.RoundTripper) *Proxy {
	proxy := &Proxy{
		configs:   configs,
		upstreams: upstreams,
		records:   records,
		releases:  releases,
		client:    &http.Client{Transport: transport},
		active:    make(map[*inFlight]struct{}),
	}
	return proxy
}

// NewWithHTTPClient injects a client, used to route upstream traffic over
// in-memory connections in tests while preserving the full proxy path.
func NewWithHTTPClient(configs *config.Store, upstreams *state.Store, records *record.Store, releases *release.Manager, client *http.Client) *Proxy {
	return &Proxy{
		configs:   configs,
		upstreams: upstreams,
		records:   records,
		releases:  releases,
		client:    client,
		active:    make(map[*inFlight]struct{}),
	}
}

// SetClient replaces the upstream HTTP client used by subsequent requests.
func (p *Proxy) SetClient(client *http.Client) {
	p.client = client
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

	current := p.releases.Current()
	snapshot := current.Snapshot
	flight := &inFlight{releaseID: current.ID, revision: current.Revision}
	if canaryRevision := request.Header.Get(CanaryRevisionHeader); canaryRevision != "" {
		if canary, ok := p.releases.CanaryAllow(canaryRevision); ok {
			snapshot = canary.Snapshot
			flight.releaseID = canary.ID
			flight.revision = canary.Revision
			flight.canary = true
		} else {
			entry.Duration = time.Since(started)
			entry.Error = "canary revision not found or rehearsal budget exhausted"
			p.records.Add(entry)
			http.Error(writer, entry.Error, http.StatusConflict)
			return
		}
	}

	if strings.HasPrefix(request.URL.Path, snapshot.AdminPath) {
		http.NotFound(writer, request)
		return
	}

	requestCtx, cancel := context.WithCancelCause(request.Context())
	flight.cancel = cancel
	request = request.WithContext(requestCtx)
	if !p.begin(flight) {
		cancel(nil)
		http.Error(writer, "gateway is shutting down", http.StatusServiceUnavailable)
		return
	}
	defer p.end(flight)

	entry.ReleaseID = flight.releaseID
	entry.Revision = flight.revision
	entry.Canary = flight.canary
	writer.Header().Set("X-Gateway-Release", flight.releaseID)
	writer.Header().Set("X-Gateway-Revision", flight.revision)
	if flight.canary {
		writer.Header().Set("X-Gateway-Canary", "true")
	}

	matched, ok := snapshot.Match(request)
	if !ok {
		entry.Status = http.StatusNotFound
		entry.Duration = time.Since(started)
		entry.Error = "no route matched"
		p.records.Add(entry)
		http.Error(writer, "no route matched", http.StatusNotFound)
		return
	}

	entry.RouteID = matched.Route.ID
	entry.Version = matched.Route.Version
	entry.UpstreamID = matched.Upstream.ID
	finish := p.upstreams.Begin(matched.Upstream.ID)

	status, attempts, bytesOut, errText, canceled := p.forward(writer, request, snapshot, matched)
	entry.Status = status
	entry.Attempts = attempts
	entry.BytesIn = request.ContentLength
	entry.BytesOut = bytesOut
	entry.Error = errText
	entry.Canceled = canceled
	entry.Duration = time.Since(started)
	p.records.Add(entry)
	finish(status < 500 && errText == "", errText)
	if flight.canary {
		p.releases.CanaryRecord(flight.releaseID, status, status >= 500 || errText != "")
	}
}

func (p *Proxy) begin(flight *inFlight) bool {
	p.activeMu.Lock()
	defer p.activeMu.Unlock()
	if p.stopping {
		return false
	}
	p.active[flight] = struct{}{}
	return true
}

func (p *Proxy) end(flight *inFlight) {
	p.activeMu.Lock()
	delete(p.active, flight)
	p.activeMu.Unlock()
}

// ActiveRequests reports which release revision every in-flight request is
// pinned to. The snapshot is taken at entry and never changes mid-request.
func (p *Proxy) ActiveRequests() []map[string]string {
	p.activeMu.Lock()
	defer p.activeMu.Unlock()
	result := make([]map[string]string, 0, len(p.active))
	for flight := range p.active {
		result = append(result, map[string]string{
			"release_id": flight.releaseID,
			"revision":   flight.revision,
			"canary":     strconv.FormatBool(flight.canary),
		})
	}
	return result
}

// BeginDrain stops accepting new requests. Requests that do not finish within
// the timeout receive an explicit terminal cause rather than hanging forever.
func (p *Proxy) BeginDrain(timeout time.Duration) (int, int) {
	p.activeMu.Lock()
	p.stopping = true
	flights := make([]*inFlight, 0, len(p.active))
	for flight := range p.active {
		flights = append(flights, flight)
	}
	p.activeMu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	aborted := 0
	for {
		if p.activeCount() == 0 {
			return len(flights), 0
		}
		select {
		case <-timer.C:
			p.activeMu.Lock()
			for flight := range p.active {
				flight.cancel(errDrainTimeout)
				aborted++
			}
			p.activeMu.Unlock()
			drainWait := time.NewTimer(2 * time.Second)
			for p.activeCount() > 0 {
				select {
				case <-drainWait.C:
					return len(flights), aborted
				case <-ticker.C:
				}
			}
			drainWait.Stop()
			return len(flights), aborted
		case <-ticker.C:
		}
	}
}

func (p *Proxy) activeCount() int {
	p.activeMu.Lock()
	defer p.activeMu.Unlock()
	return len(p.active)
}

func (p *Proxy) forward(writer http.ResponseWriter, request *http.Request, snapshot *config.Snapshot, matched *config.Match) (int, int, int64, string, bool) {
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
			return http.StatusBadGateway, attempt, bytesOut, err.Error(), false
		}
		response, err := p.client.Do(upstreamRequest)
		if err != nil {
			cancel()
			if errors.Is(context.Cause(request.Context()), errDrainTimeout) {
				writeClientError(writer, http.StatusServiceUnavailable, "gateway shutdown: drain timeout exceeded")
				return http.StatusServiceUnavailable, attempt, bytesOut, errDrainTimeout.Error(), false
			}
			if attempt < maxAttempts && matched.Route.Retry.RetryOnConnect && isRetryableRequestError(request.Context(), err) {
				lastStatus = http.StatusBadGateway
				continue
			}
			if errors.Is(request.Context().Err(), context.Canceled) {
				writeClientError(writer, 499, "client canceled")
				return 499, attempt, bytesOut, "client canceled", true
			}
			writeClientError(writer, http.StatusBadGateway, "bad gateway")
			return http.StatusBadGateway, attempt, bytesOut, err.Error(), false
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
			if errors.Is(context.Cause(request.Context()), errDrainTimeout) {
				return http.StatusServiceUnavailable, attempt, bytesOut, "gateway shutdown: drain timeout exceeded", false
			}
			if errors.Is(request.Context().Err(), context.Canceled) {
				return 499, attempt, bytesOut, "client canceled during response", true
			}
			return response.StatusCode, attempt, bytesOut, err.Error(), false
		}
		return response.StatusCode, attempt, bytesOut, "", false
	}

	writeClientError(writer, lastStatus, "bad gateway")
	return lastStatus, maxAttempts, bytesOut, "retry attempts exhausted", false
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
