package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
)

type Config struct {
	ListenAddr string     `json:"listen_addr"`
	AdminPath  string     `json:"admin_path"`
	Routes     []Route    `json:"routes"`
	Upstreams  []Upstream `json:"upstreams"`
}

type Route struct {
	ID           string   `json:"id"`
	Version      string   `json:"version"`
	Prefix       string   `json:"prefix"`
	UpstreamIDs  []string `json:"upstream_ids"`
	RetryEnabled bool     `json:"retry_enabled"`
	RetryMethods []string `json:"retry_methods"`
	MaxAttempts  int      `json:"max_attempts"`
}

type Upstream struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

func LoadFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(data)
}

func Parse(data []byte) (*Config, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var cfg Config
	if err := decoder.Decode(&cfg); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("configuration must contain exactly one JSON object")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg.ProgramDefaults()
	return &cfg, nil
}

func (c *Config) Validate() error {
	if c.ListenAddr == "" {
		c.ListenAddr = ":http"
	}
	if c.AdminPath == "" {
		c.AdminPath = "/__gateway__"
	}
	if !strings.HasPrefix(c.AdminPath, "/") {
		return errors.New("admin_path must start with /")
	}
	if len(c.Routes) == 0 {
		return errors.New("at least one route is required")
	}
	if len(c.Upstreams) == 0 {
		return errors.New("at least one upstream is required")
	}

	upstreams := make(map[string]struct{}, len(c.Upstreams))
	routes := make(map[string]struct{}, len(c.Routes))
	prefixes := make(map[string]struct{}, len(c.Routes))
	for i := range c.Upstreams {
		u := &c.Upstreams[i]
		if u.ID == "" {
			return fmt.Errorf("upstream at index %d has no id", i)
		}
		if _, exists := upstreams[u.ID]; exists {
			return fmt.Errorf("duplicate upstream id %q", u.ID)
		}
		parsed, err := url.Parse(u.URL)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return fmt.Errorf("upstream %q has an invalid url", u.ID)
		}
		upstreams[u.ID] = struct{}{}
	}

	for i := range c.Routes {
		r := &c.Routes[i]
		if r.ID == "" {
			return fmt.Errorf("route at index %d has no id", i)
		}
		if _, exists := routes[r.ID]; exists {
			return fmt.Errorf("duplicate route id %q", r.ID)
		}
		if r.Version == "" || r.Prefix == "" {
			return fmt.Errorf("route %q requires version and prefix", r.ID)
		}
		if !strings.HasPrefix(r.Prefix, "/") {
			return fmt.Errorf("route %q prefix must start with /", r.ID)
		}
		if _, exists := prefixes[r.Prefix]; exists {
			return fmt.Errorf("duplicate route prefix %q", r.Prefix)
		}
		if len(r.UpstreamIDs) == 0 {
			return fmt.Errorf("route %q requires at least one upstream", r.ID)
		}
		for _, id := range r.UpstreamIDs {
			if _, exists := upstreams[id]; !exists {
				return fmt.Errorf("route %q references unknown upstream %q", r.ID, id)
			}
		}
		if r.MaxAttempts < 0 {
			return fmt.Errorf("route %q has negative max_attempts", r.ID)
		}
		routes[r.ID] = struct{}{}
		prefixes[r.Prefix] = struct{}{}
	}
	return nil
}

func (c *Config) ProgramDefaults() {
	for i := range c.Routes {
		if c.Routes[i].MaxAttempts == 0 {
			if c.Routes[i].RetryEnabled {
				c.Routes[i].MaxAttempts = 2
			} else {
				c.Routes[i].MaxAttempts = 1
			}
		}
	}
}

type Store struct {
	current atomic.Pointer[Config]
}

func NewStore(cfg *Config) *Store {
	var store Store
	store.current.Store(cfg.clone())
	return &store
}

func (s *Store) Current() *Config {
	return s.current.Load()
}

func (s *Store) Replace(cfg *Config) (*Config, error) {
	if cfg == nil {
		return nil, errors.New("configuration is nil")
	}
	if err := cfg.Validate(); err != nil {
		return s.Current(), err
	}
	cfg.ProgramDefaults()
	next := cfg.clone()
	s.current.Store(next)
	return next, nil
}

func (s *Store) ReplaceJSON(data []byte) (*Config, error) {
	cfg, err := Parse(data)
	if err != nil {
		return s.Current(), err
	}
	return s.Replace(cfg)
}

func (c *Config) clone() *Config {
	clone := *c
	clone.Routes = append([]Route(nil), c.Routes...)
	for i := range clone.Routes {
		clone.Routes[i].UpstreamIDs = append([]string(nil), c.Routes[i].UpstreamIDs...)
		clone.Routes[i].RetryMethods = append([]string(nil), c.Routes[i].RetryMethods...)
	}
	clone.Upstreams = append([]Upstream(nil), c.Upstreams...)
	return &clone
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
