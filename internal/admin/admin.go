package admin

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"gateway/internal/config"
	"gateway/internal/proxy"
	"gateway/internal/record"
	"gateway/internal/release"
	"gateway/internal/state"
)

type Admin struct {
	configs    *config.Store
	upstreams  *state.Store
	records    *record.Store
	releases   *release.Manager
	gateway    *proxy.Proxy
	configPath string
	reload     func() error
}

func New(configs *config.Store, upstreams *state.Store, records *record.Store, releases *release.Manager, gateway *proxy.Proxy, configPath string, reload func() error) *Admin {
	return &Admin{
		configs:    configs,
		upstreams:  upstreams,
		records:    records,
		releases:   releases,
		gateway:    gateway,
		configPath: configPath,
		reload:     reload,
	}
}

func (a *Admin) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	adminPath := a.releases.Current().Snapshot.AdminPath
	rel := strings.TrimPrefix(request.URL.Path, adminPath)
	switch {
	case rel == "/upstreams":
		a.methodGuard(writer, request, http.MethodGet)
		json.NewEncoder(writer).Encode(a.upstreams.Snapshot())
	case rel == "/requests":
		a.methodGuard(writer, request, http.MethodGet)
		json.NewEncoder(writer).Encode(a.records.Snapshot())
	case rel == "/config":
		a.methodGuard(writer, request, http.MethodGet)
		current := a.releases.Current()
		snapshot := current.Snapshot
		json.NewEncoder(writer).Encode(map[string]any{
			"release_id":    current.ID,
			"revision":      current.Revision,
			"stage":         current.Stage,
			"listen":        snapshot.Listen,
			"admin_path":    snapshot.AdminPath,
			"drain_timeout": snapshot.DrainTimeout.String(),
		})
	case rel == "/inflight":
		a.methodGuard(writer, request, http.MethodGet)
		json.NewEncoder(writer).Encode(a.gateway.ActiveRequests())
	case rel == "/releases":
		a.methodGuard(writer, request, http.MethodGet)
		json.NewEncoder(writer).Encode(a.releases.History())
	case rel == "/releases/propose":
		a.Propose(writer, request)
		return
	case rel == "/releases/current":
		a.methodGuard(writer, request, http.MethodGet)
		json.NewEncoder(writer).Encode(a.releases.Current())
	case strings.HasPrefix(rel, "/releases/"):
		a.serveReleaseAction(writer, request, strings.TrimPrefix(rel, "/releases/"))
	default:
		writer.WriteHeader(http.StatusNotFound)
		json.NewEncoder(writer).Encode(map[string]string{"error": "unknown admin endpoint"})
	}
}

type proposeRequest struct {
	Config     json.RawMessage `json:"config"`
	ConfigPath string          `json:"config_path"`
	Reason     string          `json:"reason"`
}

type canaryRequest struct {
	MaxRequests int64 `json:"max_requests"`
}

type actionRequest struct {
	Reason string `json:"reason"`
}

func (a *Admin) serveReleaseAction(writer http.ResponseWriter, request *http.Request, action string) {
	parts := strings.Split(strings.Trim(action, "/"), "/")
	if len(parts) < 2 || len(parts) > 3 {
		writeError(writer, http.StatusNotFound, errNotFound)
		return
	}
	id := parts[0]
	actionName := ""
	if len(parts) > 1 {
		actionName = strings.Join(parts[1:], "/")
	}
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodPost)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	switch actionName {
	case "canary/start":
		var body canaryRequest
		if err := decodeBody(request, &body); err != nil {
			writeError(writer, http.StatusBadRequest, err)
			return
		}
		r, err := a.releases.BeginCanary(id, body.MaxRequests)
		writeResult(writer, r, err)
	case "canary/end":
		r, err := a.releases.EndCanary(id)
		writeResult(writer, r, err)
	case "promote":
		var body actionRequest
		_ = decodeBody(request, &body)
		r, err := a.releases.Promote(id, body.Reason)
		writeResult(writer, r, err)
	case "rollback":
		var body actionRequest
		if err := decodeBody(request, &body); err != nil {
			writeError(writer, http.StatusBadRequest, err)
			return
		}
		r, err := a.releases.Rollback(id, body.Reason)
		writeResult(writer, r, err)
	case "reject":
		var body actionRequest
		_ = decodeBody(request, &body)
		r, err := a.releases.Reject(id, body.Reason)
		writeResult(writer, r, err)
	default:
		writeError(writer, http.StatusNotFound, &simpleError{message: "unknown release action"})
	}
}

// Propose handles POST /__gateway/releases/propose with either an inline
// config document or a config_path referencing a file on disk.
func (a *Admin) Propose(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodPost)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body proposeRequest
	if err := decodeBody(request, &body); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	var cfg *config.Config
	if len(body.Config) > 0 {
		cfg = &config.Config{}
		if err := json.Unmarshal(body.Config, cfg); err != nil {
			writeError(writer, http.StatusBadRequest, err)
			return
		}
	} else if body.ConfigPath != "" {
		loaded, err := config.Load(body.ConfigPath)
		if err != nil {
			writeError(writer, http.StatusBadRequest, err)
			return
		}
		cfg = loaded
	} else {
		writeError(writer, http.StatusBadRequest, errMissingProbe)
		return
	}
	r, err := a.releases.Propose(cfg, body.Reason)
	writeResult(writer, r, err)
}

func (a *Admin) Reload(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodPost)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := a.reload(); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	json.NewEncoder(writer).Encode(a.releases.Current())
}

func (a *Admin) methodGuard(writer http.ResponseWriter, request *http.Request, allowed ...string) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
}

func decodeBody(request *http.Request, target any) error {
	defer request.Body.Close()
	decoder := json.NewDecoder(request.Body)
	if err := decoder.Decode(target); err != nil && err != io.EOF {
		return err
	}
	return nil
}

func writeResult(writer http.ResponseWriter, value any, err error) {
	writer.Header().Set("Content-Type", "application/json")
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(writer).Encode(map[string]string{"error": err.Error()})
		return
	}
	json.NewEncoder(writer).Encode(value)
}

func writeError(writer http.ResponseWriter, status int, err error) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	json.NewEncoder(writer).Encode(map[string]string{"error": err.Error()})
}

var errNotFound = &simpleError{message: "release not found or unknown action"}
var errMissingProbe = &simpleError{message: "either config or config_path is required"}

type simpleError struct{ message string }

func (e *simpleError) Error() string { return e.message }
