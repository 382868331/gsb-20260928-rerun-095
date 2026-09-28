package router

import (
	"net/http"
	"net/url"
	"strings"

	"gateway/internal/config"
)

type Upstream struct {
	ID  string
	URL *url.URL
}

type Route struct {
	ID           string
	Version      string
	Prefix       string
	Upstreams    []Upstream
	RetryEnabled bool
	RetryMethods []string
	MaxAttempts  int
}

type Snapshot struct {
	Routes []Route
}

func BuildSnapshot(cfg *config.Config) (*Snapshot, error) {
	upstreams := make(map[string]Upstream, len(cfg.Upstreams))
	for _, item := range cfg.Upstreams {
		parsed, err := url.Parse(item.URL)
		if err != nil {
			return nil, err
		}
		upstreams[item.ID] = Upstream{ID: item.ID, URL: parsed}
	}

	snapshot := &Snapshot{Routes: make([]Route, 0, len(cfg.Routes))}
	for _, item := range cfg.Routes {
		route := Route{
			ID:           item.ID,
			Version:      item.Version,
			Prefix:       item.Prefix,
			RetryEnabled: item.RetryEnabled,
			RetryMethods: append([]string(nil), item.RetryMethods...),
			MaxAttempts:  item.MaxAttempts,
		}
		for _, id := range item.UpstreamIDs {
			route.Upstreams = append(route.Upstreams, upstreams[id])
		}
		snapshot.Routes = append(snapshot.Routes, route)
	}
	return snapshot, nil
}

func (s *Snapshot) Match(req *http.Request) (Route, string, bool) {
	path := req.URL.EscapedPath()
	for _, route := range s.Routes {
		if path == route.Prefix || strings.HasPrefix(path, strings.TrimSuffix(route.Prefix, "/")+"/") {
			remaining := strings.TrimPrefix(path, route.Prefix)
			if remaining == "" {
				remaining = "/"
			}
			if !strings.HasPrefix(remaining, "/") {
				remaining = "/" + remaining
			}
			return route, remaining, true
		}
	}
	return Route{}, "", false
}

func (r Route) RetryAllowed(req *http.Request) bool {
	if !r.RetryEnabled || r.MaxAttempts < 2 {
		return false
	}
	for _, method := range r.RetryMethods {
		if strings.EqualFold(method, req.Method) {
			return true
		}
	}
	return false
}
