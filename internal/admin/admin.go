package admin

import (
	"encoding/json"
	"net/http"
	"strings"

	"gateway/internal/record"
	"gateway/internal/release"
	"gateway/internal/state"
)

type Admin struct {
	releases  *release.Store
	upstreams *state.Store
	records   *record.Store
	stage     func() (release.Candidate, error)
	confirm   func(string) (release.Release, error)
	rollback  func(string) (release.Release, error)
}

func New(
	releases *release.Store,
	upstreams *state.Store,
	records *record.Store,
	stage func() (release.Candidate, error),
	confirm func(string) (release.Release, error),
	rollback func(string) (release.Release, error),
) *Admin {
	return &Admin{
		releases:  releases,
		upstreams: upstreams,
		records:   records,
		stage:     stage,
		confirm:   confirm,
		rollback:  rollback,
	}
}

func (a *Admin) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	relative := strings.TrimPrefix(request.URL.Path, a.releases.Current().AdminPath)
	if relative == "" {
		relative = "/"
	}
	switch {
	case request.Method == http.MethodGet && relative == "/upstreams":
		a.writeJSON(writer, http.StatusOK, a.upstreams.Snapshot())
	case request.Method == http.MethodGet && relative == "/requests":
		a.writeJSON(writer, http.StatusOK, a.records.Snapshot())
	case request.Method == http.MethodGet && relative == "/config":
		current := a.releases.Current()
		a.writeJSON(writer, http.StatusOK, map[string]any{
			"release_id":    a.releases.CurrentID(),
			"listen":        current.Listen,
			"admin_path":    current.AdminPath,
			"drain_timeout": current.DrainTimeout.String(),
		})
	case request.Method == http.MethodGet && relative == "/releases":
		a.writeJSON(writer, http.StatusOK, map[string]any{
			"active":   a.releases.CurrentID(),
			"releases": a.releases.History(),
		})
	case request.Method == http.MethodGet && relative == "/candidates":
		a.writeJSON(writer, http.StatusOK, a.releases.Candidates())
	case request.Method == http.MethodPost && relative == "/candidates":
		a.handleStage(writer)
	case request.Method == http.MethodPost && strings.HasPrefix(relative, "/candidates/") && strings.HasSuffix(relative, "/confirm"):
		id := strings.TrimSuffix(strings.TrimPrefix(relative, "/candidates/"), "/confirm")
		a.handleConfirm(writer, id)
	case request.Method == http.MethodPost && relative == "/rollback":
		a.handleRollback(writer, request)
	default:
		a.writeJSON(writer, http.StatusNotFound, map[string]string{"error": "unknown admin endpoint"})
	}
}

// handleStage validates the on-disk configuration and registers it as a
// candidate. Invalid configurations are rejected without touching the active
// release.
func (a *Admin) handleStage(writer http.ResponseWriter) {
	candidate, err := a.stage()
	if err != nil {
		a.writeJSON(writer, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	a.writeJSON(writer, http.StatusCreated, candidate)
}

func (a *Admin) handleConfirm(writer http.ResponseWriter, candidateID string) {
	activated, err := a.confirm(candidateID)
	if err != nil {
		a.writeJSON(writer, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	a.writeJSON(writer, http.StatusOK, activated)
}

func (a *Admin) handleRollback(writer http.ResponseWriter, request *http.Request) {
	target := request.URL.Query().Get("release")
	if target == "" && request.Body != nil {
		var body struct {
			ReleaseID string `json:"release_id"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err == nil {
			target = body.ReleaseID
		}
	}
	activated, err := a.rollback(target)
	if err != nil {
		a.writeJSON(writer, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	a.writeJSON(writer, http.StatusOK, activated)
}

func (a *Admin) writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
