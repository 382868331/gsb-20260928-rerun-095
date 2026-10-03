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
	MinSuccessfulRehearsals int                `json:"min_successful_rehearsals"`
	CoverageCategories      []CoverageCategory `json:"coverage_categories"`
	// EvidencePolicy fixes how repeated executions of one interrupted
	// business request are adopted before rehearsal starts. The only
	// supported policy is "first_terminal" (default): the first terminal
	// execution of each side is the adopted evidence and a later retry can
	// never replace an unfavorable result. Changing the policy changes the
	// candidate fingerprint and opens a new review version.
	EvidencePolicy string `json:"evidence_policy"`
}

const (
	// EvidencePolicyFirstTerminal adopts the first terminal execution of
	// each side (baseline/candidate) of a business request.
	EvidencePolicyFirstTerminal = "first_terminal"
)

// CoverageCategory defines one business category of requests that a
// candidate must cover with valid rehearsal samples before release. A
// request belongs to a category when its method matches Methods (empty means
// any method) and its path is under one of Prefixes (empty means any path).
// One request may belong to several categories at once.
type CoverageCategory struct {
	ID         string   `json:"id"`
	Methods    []string `json:"methods"`
	Prefixes   []string `json:"prefixes"`
	MinSamples int      `json:"min_samples"`
	// HighRisk categories must individually pass (minimum samples met and
	// no failed, canceled or aborted samples); their result can never be
	// masked by a healthy overall average.
	HighRisk bool `json:"high_risk"`
}

// Matches reports whether one request belongs to the category.
func (c CoverageCategory) Matches(method, requestPath string) bool {
	if len(c.Methods) > 0 {
		found := false
		for _, allowed := range c.Methods {
			if strings.EqualFold(allowed, method) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if len(c.Prefixes) == 0 {
		return true
	}
	for _, prefix := range c.Prefixes {
		if requestPath == prefix || strings.HasPrefix(requestPath, strings.TrimSuffix(prefix, "/")+"/") {
			return true
		}
	}
	return false
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
	coverageCategories      []CoverageCategory
	evidencePolicy          string
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

// CoverageCategories returns the rehearsal coverage plan in declaration
// order. An empty plan means no per-category gating applies.
func (s *Snapshot) CoverageCategories() []CoverageCategory {
	result := make([]CoverageCategory, len(s.coverageCategories))
	copy(result, s.coverageCategories)
	return result
}

// EvidencePolicy returns the evidence adoption policy fixed before the
// rehearsal of a candidate built from this snapshot starts.
func (s *Snapshot) EvidencePolicy() string {
	if s.evidencePolicy == "" {
		return EvidencePolicyFirstTerminal
	}
	return s.evidencePolicy
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
	if c.Release.EvidencePolicy == "" {
		c.Release.EvidencePolicy = EvidencePolicyFirstTerminal
	}
	if c.Release.EvidencePolicy != EvidencePolicyFirstTerminal {
		return nil, fmt.Errorf("release.evidence_policy %q is not supported", c.Release.EvidencePolicy)
	}
	seenCategories := make(map[string]struct{}, len(c.Release.CoverageCategories))
	for _, category := range c.Release.CoverageCategories {
		if category.ID == "" {
			return nil, errors.New("release coverage category id is required")
		}
		if _, exists := seenCategories[category.ID]; exists {
			return nil, fmt.Errorf("duplicate release coverage category %q", category.ID)
		}
		seenCategories[category.ID] = struct{}{}
		if category.MinSamples <= 0 {
			return nil, fmt.Errorf("release coverage category %q min_samples must be positive", category.ID)
		}
		for _, prefix := range category.Prefixes {
			if !strings.HasPrefix(prefix, "/") {
				return nil, fmt.Errorf("release coverage category %q prefix %q must start with /", category.ID, prefix)
			}
		}
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
		coverageCategories:      append([]CoverageCategory(nil), c.Release.CoverageCategories...),
		evidencePolicy:          c.Release.EvidencePolicy,
		upstreams:               upstreams,
		routes:                  routes,
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
