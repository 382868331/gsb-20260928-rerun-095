package gatewayflow_test

import (
	"context"
	"net"
	"net/http"
	"sync"
	"testing"
)

// pipeHTTPServer serves a real HTTP stack over in-memory pipes, exercising
// genuine routing, framing, streaming and connection teardown behavior
// without needing permission to bind TCP ports in the sandbox.
type pipeHTTPServer struct {
	listener *pipeListener
	URL      string
	server   *http.Server
}

func newPipeHTTPServer(t *testing.T, handler http.Handler) *pipeHTTPServer {
	t.Helper()
	listener := newPipeListener()
	httpServer := &http.Server{Handler: handler}
	go func() { _ = httpServer.Serve(listener) }()
	t.Cleanup(func() { _ = httpServer.Close() })
	return &pipeHTTPServer{listener: listener, URL: "http://pipe.invalid", server: httpServer}
}

func (s *pipeHTTPServer) Close() { _ = s.server.Close() }

func (s *pipeHTTPServer) Client() *http.Client {
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return s.listener.dial(ctx)
		},
	}}
}

type pipeListener struct {
	mu     sync.Mutex
	ch     chan net.Conn
	closed bool
}

func newPipeListener() *pipeListener {
	return &pipeListener{ch: make(chan net.Conn, 8)}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	conn, ok := <-l.ch
	if !ok {
		return nil, net.ErrClosed
	}
	return conn, nil
}

func (l *pipeListener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.closed {
		l.closed = true
		close(l.ch)
	}
	return nil
}

func (l *pipeListener) Addr() net.Addr { return pipeAddr{} }

func (l *pipeListener) dial(ctx context.Context) (net.Conn, error) {
	server, client := net.Pipe()
	l.mu.Lock()
	if l.closed {
		server.Close()
		client.Close()
		l.mu.Unlock()
		return nil, net.ErrClosed
	}
	l.ch <- server
	l.mu.Unlock()
	return client, nil
}

type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "pipe.invalid:80" }
