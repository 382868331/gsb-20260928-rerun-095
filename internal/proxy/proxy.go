package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"gateway/internal/router"
)

type UpstreamStatusRecorder interface {
	Success(upstreamID string)
	Failure(upstreamID string)
}

type Proxy struct {
	client *http.Client
	status UpstreamStatusRecorder
}

type Result struct {
	StatusCode int
	Attempts   int
	UpstreamID string
	BytesSent  int64
	Canceled   bool
	Error      string
}

type flushWriter struct {
	responseWriter http.ResponseWriter
	flusher        http.Flusher
	bytesSent      int64
}

func (w *flushWriter) Write(data []byte) (int, error) {
	n, err := w.responseWriter.Write(data)
	if n > 0 {
		w.bytesSent += int64(n)
	}
	if err == nil {
		w.flusher.Flush()
	}
	return n, err
}

type noopFlusher struct{}

func (noopFlusher) Flush() {}

func New(status UpstreamStatusRecorder) *Proxy {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 15 * time.Second
	return &Proxy{
		client: &http.Client{Transport: transport},
		status: status,
	}
}

func NewWithRoundTripper(status UpstreamStatusRecorder, roundTripper http.RoundTripper) *Proxy {
	proxy := New(status)
	if roundTripper != nil {
		proxy.client.Transport = roundTripper
	}
	return proxy
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, req *http.Request, route router.Route, remainingPath string, rawQuery string) Result {
	maxAttempts := 1
	if route.RetryAllowed(req) {
		maxAttempts = route.MaxAttempts
	}

	result := Result{}
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		upstream := route.Upstreams[(attempt-1)%len(route.Upstreams)]
		attemptResult, retryable := p.attempt(w, req, upstream, remainingPath, rawQuery)
		result.Attempts = attempt
		result.UpstreamID = upstream.ID
		result.StatusCode = attemptResult.StatusCode
		result.BytesSent = attemptResult.BytesSent
		result.Canceled = attemptResult.Canceled
		result.Error = attemptResult.Error
		if !retryable || attempt == maxAttempts {
			if attemptResult.StatusCode == http.StatusBadGateway && attemptResult.BytesSent == 0 && !attemptResult.Canceled {
				w.WriteHeader(http.StatusBadGateway)
			}
			return result
		}
	}
	return result
}

func (p *Proxy) attempt(w http.ResponseWriter, original *http.Request, upstream router.Upstream, remainingPath, rawQuery string) (Result, bool) {
	upstreamURL := *upstream.URL
	upstreamURL.Path = remainingPath
	upstreamURL.RawQuery = rawQuery

	upstreamReq, err := http.NewRequestWithContext(original.Context(), original.Method, upstreamURL.String(), original.Body)
	if err != nil {
		p.status.Failure(upstream.ID)
		return Result{StatusCode: http.StatusBadGateway, Error: err.Error()}, false
	}
	copyHeaders(original.Header, upstreamReq.Header)
	if original.Host != "" {
		upstreamReq.Host = original.Host
	}

	response, err := p.client.Do(upstreamReq)
	if err != nil {
		p.status.Failure(upstream.ID)
		canceled := errors.Is(err, context.Canceled) || original.Context().Err() != nil
		return Result{
			StatusCode: http.StatusBadGateway,
			Canceled:   canceled,
			Error:      err.Error(),
		}, !canceled
	}
	defer response.Body.Close()

	removeHopHeaders(response.Header)
	writeHeaders(w, response.Header)
	w.WriteHeader(response.StatusCode)

	flusher, _ := w.(http.Flusher)
	writer := &flushWriter{responseWriter: w, flusher: flusher}
	if flusher == nil {
		writer.flusher = noopFlusher{}
	}
	_, copyErr := io.Copy(writer, response.Body)
	if copyErr == nil {
		copyErr = response.Body.Close()
	}

	canceled := copyErr != nil && (errors.Is(copyErr, context.Canceled) || original.Context().Err() != nil)
	if copyErr != nil {
		p.status.Failure(upstream.ID)
	} else {
		p.status.Success(upstream.ID)
	}

	return Result{
		StatusCode: response.StatusCode,
		BytesSent:  writer.bytesSent,
		Canceled:   canceled,
		Error:      errorString(copyErr),
	}, false
}

func copyHeaders(source, destination http.Header) {
	for name, values := range source {
		if isHopHeader(name) {
			continue
		}
		for _, value := range values {
			destination.Add(name, value)
		}
	}
}

func writeHeaders(w http.ResponseWriter, headers http.Header) {
	for name, values := range headers {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}
}

func removeHopHeaders(headers http.Header) {
	for name := range headers {
		if isHopHeader(name) {
			headers.Del(name)
		}
	}
}

func isHopHeader(name string) bool {
	switch strings.ToLower(name) {
	case "connection", "proxy-connection", "keep-alive", "te", "trailer", "transfer-encoding", "upgrade":
		return true
	default:
		return false
	}
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
