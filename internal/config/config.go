package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

type Config struct {
	Listen       string        `json:"listen"`
	AdminPath    string        `json:"admin_path"`
	Upstreams    []Upstream    `json:"upstreams"`
	Routes       []Route       `json:"routes"`
	DrainTimeout Duration      `json:"drain_timeout"`
	Release      ReleasePolicy `json:"release"`
}

// ReleasePolicy controls the evidence a candidate must gather through
// limited rehearsal traffic before it is allowed to become the release.
type ReleasePolicy struct {
	MinSuccessfulRehearsals int `json:"min_successful_rehearsals"`
	// Coverage, when present, defines the per-business-category rehearsal
	// coverage plan that must be acknowledged before rehearsal traffic is
	// accepted and satisfied before a candidate may be released.
	Coverage *CoveragePlan `json:"coverage,omitempty"`
}

// CoveragePlan groups rehearsal requests into user-defined business
// categories. One request may belong to several categories at once.
type CoveragePlan struct {
	Categories []CoverageCategory `json:"categories"`
}

// CoverageCategory is one required request class. A request belongs to the
// category when its method is allowed, its path matches at least one prefix,
// and the optional header carries the required value. An empty
// header_value matches on header presence only. Every criterion that is
// left empty is ignored, so at least one criterion must be set.
type CoverageCategory struct {
	ID           string   `json:"id"`
	Description  string   `json:"description,omitempty"`
	Methods      []string `json:"methods,omitempty"`
	PathPrefixes []string `json:"path_prefixes,omitempty"`
	HeaderName   string   `json:"header_name,omitempty"`
	HeaderValue  string   `json:"header_value,omitempty"`
	// HighRisk categories are judged independently: the overall average can
	// never compensate for missing or invalid high-risk evidence.
	HighRisk bool `json:"high_risk"`
	// MinValidSamples is the number of distinct valid (successful, matched)
	// rehearsal requests the category must contain. Zero means the category
	// is tracked and reported but not required by the release gate.
	MinValidSamples int `json:"min_valid_samples"`
}

// Matches reports whether a request described by method/path/headers belongs
// to this category. Empty criteria are wildcards.
func (c CoverageCategory) Matches(method, path string, headers http.Header) bool {
	if len(c.Methods) > 0 {
		allowed := false
		for _, candidate := range c.Methods {
			if strings.EqualFold(candidate, method) {
				allowed = true
				break
			}
		}
		if !allowed {
			return false
		}
	}
	if len(c.PathPrefixes) > 0 {
		matched := false
		for _, prefix := range c.PathPrefixes {
			if hasPrefix(path, prefix) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	if c.HeaderName != "" {
		value, present := headers[http.CanonicalHeaderKey(c.HeaderName)]
		if !present {
			return false
		}
		if c.HeaderValue != "" {
			matchedValue := false
			for _, candidate := range value {
				if candidate == c.HeaderValue {
					matchedValue = true
				}
			}
			if !matchedValue {
				return false
			}
		}
	}
	return true
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
	Listen                  string
	AdminPath               string
	DrainTimeout            time.Duration
	minSuccessfulRehearsals int
	coverage                *CoveragePlan
	upstreams               map[string]*Upstream
	routes                  []matchedRoute
}

type matchedRoute struct {
	route    Route
	upstream *Upstream
}

type Match struct {
	Route    Route
	Upstream *Upstream
}

// DescribeRoutes returns stable route summaries used in release audit records.
func (s *Snapshot) DescribeRoutes() []string {
	result := make([]string, 0, len(s.routes))
	for _, candidate := range s.routes {
		result = append(result, candidate.route.ID+"@"+candidate.route.Version)
	}
	return result
}

// MinSuccessfulRehearsals is the number of successful rehearsal requests a
// candidate built from this snapshot needs before it may be confirmed.
func (s *Snapshot) MinSuccessfulRehearsals() int {
	if s.minSuccessfulRehearsals <= 0 {
		return 1
	}
	return s.minSuccessfulRehearsals
}

// CoveragePlan returns the configured per-category coverage plan, or nil when
// no category requirements were defined.
func (s *Snapshot) CoveragePlan() *CoveragePlan {
	return s.coverage
}

// Routes returns the configured routes in declaration order.
func (s *Snapshot) Routes() []Route {
	routes := make([]Route, len(s.routes))
	for i := range s.routes {
		routes[i] = s.routes[i].route
	}
	return routes
}

// Upstreams returns the configured upstreams sorted by ID.
func (s *Snapshot) Upstreams() []Upstream {
	upstreams := make([]Upstream, 0, len(s.upstreams))
	for _, upstream := range s.upstreams {
		upstreams = append(upstreams, *upstream)
	}
	sort.Slice(upstreams, func(i, j int) bool { return upstreams[i].ID < upstreams[j].ID })
	return upstreams
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
	if c.Release.MinSuccessfulRehearsals == 0 {
		c.Release.MinSuccessfulRehearsals = 1
	}
	if c.Release.MinSuccessfulRehearsals < 0 {
		return nil, errors.New("release.min_successful_rehearsals must not be negative")
	}
	coverage, err := validateCoverage(c.Release.Coverage)
	if err != nil {
		return nil, err
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
		Listen:                  c.Listen,
		AdminPath:               path.Clean(c.AdminPath),
		DrainTimeout:            time.Duration(c.DrainTimeout),
		minSuccessfulRehearsals: c.Release.MinSuccessfulRehearsals,
		coverage:                coverage,
		upstreams:               upstreams,
		routes:                  routes,
	}, nil
}

func validateCoverage(plan *CoveragePlan) (*CoveragePlan, error) {
	if plan == nil || len(plan.Categories) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(plan.Categories))
	for i := range plan.Categories {
		category := plan.Categories[i]
		if category.ID == "" {
			return nil, errors.New("release.coverage category id is required")
		}
		if _, exists := seen[category.ID]; exists {
			return nil, fmt.Errorf("release.coverage duplicate category id %q", category.ID)
		}
		seen[category.ID] = struct{}{}
		if category.MinValidSamples < 0 {
			return nil, fmt.Errorf("release.coverage category %q min_valid_samples must not be negative", category.ID)
		}
		if len(category.Methods) == 0 && len(category.PathPrefixes) == 0 && category.HeaderName == "" {
			return nil, fmt.Errorf("release.coverage category %q must define methods, path_prefixes or header_name", category.ID)
		}
		for _, method := range category.Methods {
			if method == "" {
				return nil, fmt.Errorf("release.coverage category %q has an empty method", category.ID)
			}
		}
		for _, prefix := range category.PathPrefixes {
			if !strings.HasPrefix(prefix, "/") {
				return nil, fmt.Errorf("release.coverage category %q prefix %q must start with /", category.ID, prefix)
			}
		}
		if category.HeaderName == "" && category.HeaderValue != "" {
			return nil, fmt.Errorf("release.coverage category %q sets header_value without header_name", category.ID)
		}
		plan.Categories[i] = category
	}
	return plan, nil
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
