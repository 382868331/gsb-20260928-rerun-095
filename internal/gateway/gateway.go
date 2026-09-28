package gateway

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"gateway/internal/config"
	"gateway/internal/proxy"
	"gateway/internal/records"
	"gateway/internal/router"
	"gateway/internal/state"
)

type statusAdapter struct {
	registry *state.Registry
}

func (a statusAdapter) Success(upstreamID string) {
	if upstream := a.registry.Get(upstreamID); upstream != nil {
		upstream.Record(state.ResultSuccess)
	}
}

func (a statusAdapter) Failure(upstreamID string) {
	if upstream := a.registry.Get(upstreamID); upstream != nil {
		upstream.Record(state.ResultFailure)
	}
}

type Gateway struct {
	configs  *config.Store
	records  *records.Recorder
	registry *state.Registry
	proxy    *proxy.Proxy
	snapshot atomic.Pointer[router.Snapshot]
	version  atomic.Int64
}

type adminResponse struct {
	Version int64            `json:"version"`
	Config  *config.Config   `json:"config,omitempty"`
	Status  any              `json:"status,omitempty"`
	Records []records.Record `json:"records,omitempty"`
	Error   string           `json:"error,omitempty"`
}

func New(cfg *config.Config) (*Gateway, error) {
	return NewWithRoundTripper(cfg, nil)
}

func NewWithRoundTripper(cfg *config.Config, roundTripper http.RoundTripper) (*Gateway, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg.ProgramDefaults()
	store := config.NewStore(cfg)
	registry := state.NewRegistry(upstreamIDs(cfg))
	g := &Gateway{
		configs:  store,
		records:  records.NewRecorder(2000),
		registry: registry,
		proxy:    proxy.NewWithRoundTripper(statusAdapter{registry: registry}, roundTripper),
	}
	snapshot, err := router.BuildSnapshot(cfg)
	if err != nil {
		return nil, err
	}
	g.snapshot.Store(snapshot)
	g.version.Store(1)
	return g, nil
}

func (g *Gateway) Handler() http.Handler {
	return http.HandlerFunc(g.serveHTTP)
}

func (g *Gateway) CurrentConfig() *config.Config {
	return g.configs.Current()
}

func (g *Gateway) serveHTTP(w http.ResponseWriter, req *http.Request) {
	cfg := g.configs.Current()
	if req.URL.Path == cfg.AdminPath || strings.HasPrefix(req.URL.Path, cfg.AdminPath+"/") {
		g.serveAdmin(w, req, cfg)
		return
	}

	record := g.records.Start(req.Method, req.URL.RequestURI())
	snapshot := g.snapshot.Load()
	route, remainingPath, ok := snapshot.Match(req)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		record.StatusCode = http.StatusNotFound
		record.Error = "no route matched"
		g.records.Finish(record)
		return
	}

	record.RouteID = route.ID
	record.RouteVersion = route.Version
	record.UpstreamIDs = upstreamIDsFromRoute(route)
	result := g.proxy.ServeHTTP(w, req, route, remainingPath, req.URL.RawQuery)
	record.Attempts = result.Attempts
	record.StatusCode = result.StatusCode
	record.BytesSent = result.BytesSent
	record.Canceled = result.Canceled
	record.Error = result.Error
	g.records.Finish(record)
}

func (g *Gateway) serveAdmin(w http.ResponseWriter, req *http.Request, cfg *config.Config) {
	suffix := strings.TrimPrefix(req.URL.Path, cfg.AdminPath)
	suffix = strings.TrimPrefix(suffix, "/")
	if suffix == "" {
		writeJSON(w, http.StatusOK, adminResponse{Version: g.version.Load(), Config: g.configs.Current()})
		return
	}
	if suffix == "status" {
		writeJSON(w, http.StatusOK, adminResponse{Version: g.version.Load(), Status: g.registry.Snapshot()})
		return
	}
	if suffix == "records" {
		writeJSON(w, http.StatusOK, adminResponse{Version: g.version.Load(), Records: g.records.Snapshot()})
		return
	}
	if suffix == "config" {
		switch req.Method {
		case http.MethodGet:
			writeJSON(w, http.StatusOK, adminResponse{Version: g.version.Load(), Config: g.configs.Current()})
		case http.MethodPut, http.MethodPost:
			data, err := io.ReadAll(io.LimitReader(req.Body, 1<<20))
			if err != nil {
				writeJSON(w, http.StatusBadRequest, adminResponse{Error: err.Error()})
				return
			}
			next, err := g.configs.ReplaceJSON(data)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, adminResponse{Version: g.version.Load(), Config: next, Error: err.Error()})
				return
			}
			snapshot, err := router.BuildSnapshot(next)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, adminResponse{Version: g.version.Load(), Config: g.configs.Current(), Error: err.Error()})
				return
			}
			g.registry.EnsureIDs(upstreamIDs(next))
			g.snapshot.Store(snapshot)
			g.version.Add(1)
			writeJSON(w, http.StatusOK, adminResponse{Version: g.version.Load(), Config: next})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

func writeJSON(w http.ResponseWriter, statusCode int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(value)
}

func upstreamIDs(cfg *config.Config) []string {
	ids := make([]string, 0, len(cfg.Upstreams))
	for _, upstream := range cfg.Upstreams {
		ids = append(ids, upstream.ID)
	}
	return ids
}

func upstreamIDsFromRoute(route router.Route) []string {
	ids := make([]string, 0, len(route.Upstreams))
	for _, upstream := range route.Upstreams {
		ids = append(ids, upstream.ID)
	}
	return ids
}

func ListenAndServe(cfg *config.Config) (*http.Server, error) {
	g, err := New(cfg)
	if err != nil {
		return nil, err
	}
	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           g.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() { _ = server.ListenAndServe() }()
	return server, nil
}
