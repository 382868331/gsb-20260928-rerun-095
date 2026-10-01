package admin

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"gateway/internal/record"
	"gateway/internal/release"
	"gateway/internal/state"
)

type Admin struct {
	releases    *release.Store
	upstreams   *state.Store
	records     *record.Store
	stage       func() (release.Candidate, error)
	revise      func(string) (release.Candidate, bool, error)
	confirm     func(string) (release.Release, error)
	discard     func(string) (release.CandidateRecord, error)
	rollback    func(string) (release.Release, error)
	diffConfig  func(string) (release.SnapshotDiff, error)
	diffRequest func(string, string, string) (release.RequestDiff, error)
}

func New(
	releases *release.Store,
	upstreams *state.Store,
	records *record.Store,
	stage func() (release.Candidate, error),
	revise func(string) (release.Candidate, bool, error),
	confirm func(string) (release.Release, error),
	discard func(string) (release.CandidateRecord, error),
	rollback func(string) (release.Release, error),
	diffConfig func(string) (release.SnapshotDiff, error),
	diffRequest func(string, string, string) (release.RequestDiff, error),
) *Admin {
	return &Admin{
		releases:    releases,
		upstreams:   upstreams,
		records:     records,
		stage:       stage,
		revise:      revise,
		confirm:     confirm,
		discard:     discard,
		rollback:    rollback,
		diffConfig:  diffConfig,
		diffRequest: diffRequest,
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
		a.writeJSON(writer, http.StatusOK, map[string]any{
			"candidates": a.releases.Candidates(),
			"archive":    a.releases.ArchivedCandidates(),
		})
	case request.Method == http.MethodPost && relative == "/candidates":
		a.handleStage(writer)
	case isCandidateSubroute(request.Method, relative, "/revise"):
		a.handleRevise(writer, candidateIDFromPath(relative, "/revise"))
	case isCandidateSubroute(request.Method, relative, "/confirm"):
		a.handleConfirm(writer, candidateIDFromPath(relative, "/confirm"))
	case isCandidateSubroute(request.Method, relative, "/discard"):
		a.handleDiscard(writer, candidateIDFromPath(relative, "/discard"))
	case isCandidateSubroute(request.Method, relative, "/diff"):
		a.handleDiff(writer, request, candidateIDFromPath(relative, "/diff"))
	case request.Method == http.MethodPost && relative == "/rollback":
		a.handleRollback(writer, request)
	default:
		a.writeJSON(writer, http.StatusNotFound, map[string]string{"error": "unknown admin endpoint"})
	}
}

func isCandidateSubroute(method, relative, suffix string) bool {
	return method == http.MethodPost &&
		strings.HasPrefix(relative, "/candidates/") &&
		strings.HasSuffix(relative, suffix)
}

func candidateIDFromPath(relative, suffix string) string {
	return strings.TrimSuffix(strings.TrimPrefix(relative, "/candidates/"), suffix)
}

func (a *Admin) handleStage(writer http.ResponseWriter) {
	candidate, err := a.stage()
	if err != nil {
		a.writeJSON(writer, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	a.writeJSON(writer, http.StatusCreated, candidate)
}

func (a *Admin) handleRevise(writer http.ResponseWriter, candidateID string) {
	candidate, invalidated, err := a.revise(candidateID)
	if err != nil {
		a.writeError(writer, err)
		return
	}
	status := http.StatusOK
	if invalidated {
		status = http.StatusCreated
	}
	a.writeJSON(writer, status, map[string]any{
		"candidate":            candidate,
		"evidence_invalidated": invalidated,
	})
}

func (a *Admin) handleConfirm(writer http.ResponseWriter, candidateID string) {
	activated, err := a.confirm(candidateID)
	if err != nil {
		a.writeError(writer, err)
		return
	}
	a.writeJSON(writer, http.StatusOK, activated)
}

func (a *Admin) handleDiscard(writer http.ResponseWriter, candidateID string) {
	record, err := a.discard(candidateID)
	if err != nil {
		a.writeError(writer, err)
		return
	}
	a.writeJSON(writer, http.StatusOK, record)
}

func (a *Admin) handleDiff(writer http.ResponseWriter, request *http.Request, candidateID string) {
	requestPath := request.URL.Query().Get("path")
	if requestPath == "" {
		diff, err := a.diffConfig(candidateID)
		if err != nil {
			a.writeError(writer, err)
			return
		}
		a.writeJSON(writer, http.StatusOK, diff)
		return
	}
	method := request.URL.Query().Get("method")
	diff, err := a.diffRequest(candidateID, method, requestPath)
	if err != nil {
		a.writeError(writer, err)
		return
	}
	a.writeJSON(writer, http.StatusOK, diff)
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
		a.writeError(writer, err)
		return
	}
	a.writeJSON(writer, http.StatusOK, activated)
}

func (a *Admin) writeError(writer http.ResponseWriter, err error) {
	var gateError *release.GateError
	if errors.As(err, &gateError) {
		a.writeJSON(writer, http.StatusConflict, map[string]any{
			"error":     gateError.Error(),
			"status":    gateError.Status,
			"rehearsal": gateError.Summary,
		})
		return
	}
	if strings.Contains(err.Error(), "unknown candidate") || strings.Contains(err.Error(), "unknown release") {
		a.writeJSON(writer, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	a.writeJSON(writer, http.StatusConflict, map[string]string{"error": err.Error()})
}

func (a *Admin) writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
