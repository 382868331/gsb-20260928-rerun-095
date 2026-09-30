package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"time"
)

type Config struct {
	Listen       string     `json:"listen"`
	AdminPath    string     `json:"admin_path"`
	Upstreams    []Upstream `json:"upstreams"`
	Routes       []Route    `json:"routes"`
	DrainTimeout Duration   `json:"drain_timeout"`
}

type Duration time.Duration

func (d *Duration) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

type Upstream struct {
	ID      string   `json:"id"`
	BaseURL string   `json:"base_url"`
	Timeout Duration `json:"timeout"`
}

type Route struct {
	ID          string      `json:"id"`
	Version     string      `json:"version"`
	Prefix      string      `json:"prefix"`
	UpstreamID  string      `json:"upstream_id"`
	StripPrefix bool        `json:"strip_prefix"`
	Retry       RetryPolicy `json:"retry"`
}

type RetryPolicy struct {
	Methods        []string `json:"methods"`
	MaxAttempts    int      `json:"max_attempts"`
	RetryOnStatus  []int    `json:"retry_on_status"`
	RetryOnConnect bool     `json:"retry_on_connect"`
}

type Snapshot struct {
	Listen       string
	AdminPath    string
	DrainTimeout time.Duration
	upstreams    map[string]*Upstream
	routes       []matchedRoute
}

type matchedRoute struct {
	route    Route
	upstream *Upstream
}

type Match struct {
	Route    Route
	Upstream *Upstream
}

func (s *Snapshot) Match(request *http.Request) (*Match, bool) {
	for _, candidate := range s.routes {
		if hasPrefix(request.URL.Path, candidate.route.Prefix) {
			return &Match{Route: candidate.route, Upstream: candidate.upstream}, true
		}
	}
	return nil, false
}

func hasPrefix(requestPath, prefix string) bool {
	if requestPath == prefix {
		return true
	}
	return strings.HasPrefix(requestPath, strings.TrimSuffix(prefix, "/")+"/")
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) Snapshot() (*Snapshot, error) {
	if c.Listen == "" {
		return nil, errors.New("listen is required")
	}
	if c.AdminPath == "" {
		c.AdminPath = "/__gateway"
	}
	if c.DrainTimeout == 0 {
		c.DrainTimeout = Duration(30 * time.Second)
	}
	if !strings.HasPrefix(c.AdminPath, "/") {
		return nil, errors.New("admin_path must start with /")
	}
	if len(c.Routes) == 0 {
		return nil, errors.New("at least one route is required")
	}

	upstreams := make(map[string]*Upstream, len(c.Upstreams))
	for i := range c.Upstreams {
		upstream := c.Upstreams[i]
		if upstream.ID == "" {
			return nil, errors.New("upstream id is required")
		}
		if _, exists := upstreams[upstream.ID]; exists {
			return nil, fmt.Errorf("duplicate upstream id %q", upstream.ID)
		}
		parsed, err := url.Parse(upstream.BaseURL)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return nil, fmt.Errorf("upstream %q has invalid base_url", upstream.ID)
		}
		if upstream.Timeout == 0 {
			upstream.Timeout = Duration(30 * time.Second)
		}
		upstreams[upstream.ID] = &upstream
	}

	routes := make([]matchedRoute, 0, len(c.Routes))
	seenPrefixes := make(map[string]struct{}, len(c.Routes))
	for _, route := range c.Routes {
		if route.ID == "" {
			return nil, errors.New("route id is required")
		}
		if route.Version == "" {
			return nil, fmt.Errorf("route %q version is required", route.ID)
		}
		if !strings.HasPrefix(route.Prefix, "/") {
			return nil, fmt.Errorf("route %q prefix must start with /", route.ID)
		}
		if _, exists := seenPrefixes[route.Prefix]; exists {
			return nil, fmt.Errorf("duplicate route prefix %q", route.Prefix)
		}
		seenPrefixes[route.Prefix] = struct{}{}
		upstream, ok := upstreams[route.UpstreamID]
		if !ok {
			return nil, fmt.Errorf("route %q references unknown upstream %q", route.ID, route.UpstreamID)
		}
		if route.Retry.MaxAttempts == 0 {
			route.Retry.MaxAttempts = 1
		}
		if route.Retry.MaxAttempts < 1 {
			return nil, fmt.Errorf("route %q retry max_attempts must be positive", route.ID)
		}
		routes = append(routes, matchedRoute{route: route, upstream: upstream})
	}

	return &Snapshot{
		Listen:       c.Listen,
		AdminPath:    path.Clean(c.AdminPath),
		DrainTimeout: time.Duration(c.DrainTimeout),
		upstreams:    upstreams,
		routes:       routes,
	}, nil
}

type Store struct {
	current atomic.Pointer[Snapshot]
}

func NewStore(snapshot *Snapshot) *Store {
	store := &Store{}
	store.current.Store(snapshot)
	return store
}

func (s *Store) Current() *Snapshot {
	return s.current.Load()
}

func (s *Store) Replace(snapshot *Snapshot) {
	s.current.Store(snapshot)
}
