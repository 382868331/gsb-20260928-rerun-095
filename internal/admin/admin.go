package admin

import (
	"encoding/json"
	"net/http"

	"gateway/internal/config"
	"gateway/internal/record"
	"gateway/internal/state"
)

type Admin struct {
	configs    *config.Store
	upstreams  *state.Store
	records    *record.Store
	configPath string
	reload     func() error
}

func New(configs *config.Store, upstreams *state.Store, records *record.Store, configPath string, reload func() error) *Admin {
	return &Admin{
		configs:    configs,
		upstreams:  upstreams,
		records:    records,
		configPath: configPath,
		reload:     reload,
	}
}

func (a *Admin) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	switch request.URL.Path {
	case a.configs.Current().AdminPath + "/upstreams":
		json.NewEncoder(writer).Encode(a.upstreams.Snapshot())
	case a.configs.Current().AdminPath + "/requests":
		json.NewEncoder(writer).Encode(a.records.Snapshot())
	case a.configs.Current().AdminPath + "/config":
		current := a.configs.Current()
		json.NewEncoder(writer).Encode(map[string]any{
			"listen":        current.Listen,
			"admin_path":    current.AdminPath,
			"drain_timeout": current.DrainTimeout.String(),
		})
	default:
		writer.WriteHeader(http.StatusNotFound)
		json.NewEncoder(writer).Encode(map[string]string{"error": "unknown admin endpoint"})
	}
}

func (a *Admin) Reload(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodPost)
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := a.reload(); err != nil {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(writer).Encode(map[string]string{"error": err.Error()})
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	json.NewEncoder(writer).Encode(map[string]string{"status": "reloaded"})
}
