package release

import (
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"time"

	"gateway/internal/config"
)

// RouteView is the user-facing, stable description of one route entry.
type RouteView struct {
	ID          string             `json:"id"`
	Version     string             `json:"version"`
	Prefix      string             `json:"prefix"`
	UpstreamID  string             `json:"upstream_id"`
	StripPrefix bool               `json:"strip_prefix"`
	Retry       config.RetryPolicy `json:"retry"`
}

// UpstreamView is the user-facing, stable description of one upstream.
type UpstreamView struct {
	ID      string `json:"id"`
	BaseURL string `json:"base_url"`
	Timeout string `json:"timeout"`
}

// RequestDiff shows how one representative request is routed by the active
// release and by the candidate on the very same path and method.
type RequestDiff struct {
	Method    string     `json:"method"`
	Path      string     `json:"path"`
	Active    *MatchView `json:"active"`
	Candidate *MatchView `json:"candidate"`
	Changed   bool       `json:"changed"`
}

// MatchView describes which route/version/upstream a request resolves to.
type MatchView struct {
	Matched    bool   `json:"matched"`
	RouteID    string `json:"route_id,omitempty"`
	Version    string `json:"version,omitempty"`
	UpstreamID string `json:"upstream_id,omitempty"`
}

// Diff compares the active release with a candidate over route/limit
// definitions and a fixed set of representative requests.
type Diff struct {
	ActiveRelease    string           `json:"active_release"`
	CandidateID      string           `json:"candidate_id"`
	RoutesAdded      []RouteView      `json:"routes_added"`
	RoutesRemoved    []RouteView      `json:"routes_removed"`
	RoutesChanged    []RouteChange    `json:"routes_changed"`
	UpstreamsAdded   []UpstreamView   `json:"upstreams_added"`
	UpstreamsRemoved []UpstreamView   `json:"upstreams_removed"`
	UpstreamsChanged []UpstreamChange `json:"upstreams_changed"`
	Requests         []RequestDiff    `json:"requests"`
}

// RouteChange presents the before/after definition of one route id.
type RouteChange struct {
	Before RouteView `json:"before"`
	After  RouteView `json:"after"`
}

// UpstreamChange presents the before/after definition of one upstream id.
type UpstreamChange struct {
	Before UpstreamView `json:"before"`
	After  UpstreamView `json:"after"`
}

// CompareDiffs builds a diff over the given representative requests. When no
// requests are supplied, route prefixes from both sides are used instead so
// the comparison still covers the configured paths.
func (s *Store) Compare(candidateID string, requests []*http.Request) (Diff, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate, ok := s.candidates[candidateID]
	if !ok {
		return Diff{}, unknownCandidateError(candidateID)
	}
	return diffLocked(s.activeID, s.active, candidate, requests), nil
}

func unknownCandidateError(id string) error {
	return &CandidateError{ID: id}
}

// CandidateError carries an unknown candidate id; callers map it to 404.
type CandidateError struct{ ID string }

func (e *CandidateError) Error() string { return ErrUnknownCandidate.Error() + " " + e.ID }

func (e *CandidateError) Unwrap() error { return ErrUnknownCandidate }

func diffLocked(activeID string, active *config.Snapshot, candidate *Candidate, requests []*http.Request) Diff {
	diff := Diff{
		ActiveRelease:    activeID,
		CandidateID:      candidate.ID,
		RoutesAdded:      []RouteView{},
		RoutesRemoved:    []RouteView{},
		RoutesChanged:    []RouteChange{},
		UpstreamsAdded:   []UpstreamView{},
		UpstreamsRemoved: []UpstreamView{},
		UpstreamsChanged: []UpstreamChange{},
		Requests:         []RequestDiff{},
	}

	activeRoutes := make(map[string]RouteView)
	for _, route := range active.Routes() {
		activeRoutes[route.ID] = routeView(route)
	}
	candidateRoutes := make(map[string]RouteView)
	for _, route := range candidate.snapshot.Routes() {
		candidateRoutes[route.ID] = routeView(route)
	}
	ids := mapKeys(activeRoutes, candidateRoutes)
	for _, id := range ids {
		before, beforeOK := activeRoutes[id]
		after, afterOK := candidateRoutes[id]
		switch {
		case beforeOK && !afterOK:
			diff.RoutesRemoved = append(diff.RoutesRemoved, before)
		case !beforeOK && afterOK:
			diff.RoutesAdded = append(diff.RoutesAdded, after)
		case beforeOK && afterOK && !equalJSON(before, after):
			diff.RoutesChanged = append(diff.RoutesChanged, RouteChange{Before: before, After: after})
		}
	}

	activeUpstreams := make(map[string]UpstreamView)
	for _, upstream := range active.Upstreams() {
		activeUpstreams[upstream.ID] = upstreamView(upstream)
	}
	candidateUpstreams := make(map[string]UpstreamView)
	for _, upstream := range candidate.snapshot.Upstreams() {
		candidateUpstreams[upstream.ID] = upstreamView(upstream)
	}
	upstreamIDs := mapKeys(activeUpstreams, candidateUpstreams)
	for _, id := range upstreamIDs {
		before, beforeOK := activeUpstreams[id]
		after, afterOK := candidateUpstreams[id]
		switch {
		case beforeOK && !afterOK:
			diff.UpstreamsRemoved = append(diff.UpstreamsRemoved, before)
		case !beforeOK && afterOK:
			diff.UpstreamsAdded = append(diff.UpstreamsAdded, after)
		case beforeOK && afterOK && !equalJSON(before, after):
			diff.UpstreamsChanged = append(diff.UpstreamsChanged, UpstreamChange{Before: before, After: after})
		}
	}

	if len(requests) == 0 {
		seen := map[string]struct{}{}
		for _, view := range diff.RoutesAdded {
			requests = appendUniquePath(requests, seen, view.Prefix)
		}
		for _, view := range activeRoutes {
			requests = appendUniquePath(requests, seen, view.Prefix)
		}
	}
	for _, request := range requests {
		activeMatch := matchView(active, request)
		candidateMatch := matchView(candidate.snapshot, request)
		diff.Requests = append(diff.Requests, RequestDiff{
			Method:    request.Method,
			Path:      request.URL.Path,
			Active:    &activeMatch,
			Candidate: &candidateMatch,
			Changed:   !equalJSON(activeMatch, candidateMatch),
		})
	}
	return diff
}

func appendUniquePath(requests []*http.Request, seen map[string]struct{}, prefix string) []*http.Request {
	path := prefix
	if path == "" || path == "/" {
		return requests
	}
	if _, ok := seen[path]; ok {
		return requests
	}
	seen[path] = struct{}{}
	request := newSimpleGet(path)
	return append(requests, request)
}

func routeView(route config.Route) RouteView {
	return RouteView{
		ID:          route.ID,
		Version:     route.Version,
		Prefix:      route.Prefix,
		UpstreamID:  route.UpstreamID,
		StripPrefix: route.StripPrefix,
		Retry:       route.Retry,
	}
}

func upstreamView(upstream config.Upstream) UpstreamView {
	return UpstreamView{
		ID:      upstream.ID,
		BaseURL: upstream.BaseURL,
		Timeout: time.Duration(upstream.Timeout).String(),
	}
}

func matchView(snapshot *config.Snapshot, request *http.Request) MatchView {
	matched, ok := snapshot.Match(request)
	if !ok {
		return MatchView{Matched: false}
	}
	return MatchView{
		Matched:    true,
		RouteID:    matched.Route.ID,
		Version:    matched.Route.Version,
		UpstreamID: matched.Upstream.ID,
	}
}

func mapKeys[T any](a, b map[string]T) []string {
	keys := make(map[string]struct{})
	for id := range a {
		keys[id] = struct{}{}
	}
	for id := range b {
		keys[id] = struct{}{}
	}
	result := make([]string, 0, len(keys))
	for id := range keys {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}

func newSimpleGet(path string) *http.Request {
	return &http.Request{Method: http.MethodGet, URL: &url.URL{Path: path}}
}

func equalJSON(a, b any) bool {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return string(left) == string(right)
}
