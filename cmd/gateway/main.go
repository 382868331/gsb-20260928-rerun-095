package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"gateway/internal/admin"
	"gateway/internal/config"
	"gateway/internal/proxy"
	"gateway/internal/record"
	"gateway/internal/release"
	"gateway/internal/state"
)

func main() {
	configPath := flag.String("config", "configs/gateway.json", "gateway configuration file")
	flag.Parse()

	initialCfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	releases, err := release.NewManager(initialCfg)
	if err != nil {
		log.Fatalf("validate config: %v", err)
	}
	initial := releases.Current().Snapshot
	configs := config.NewStore(initial)
	upstreams := state.NewStore()
	records := record.NewStore(2000)
	gateway := proxy.New(configs, upstreams, records, releases)

	var serverMu sync.Mutex
	var currentServer *http.Server

	// When the serving revision changes (promotion or rollback), swap the
	// atomic routing snapshot and, if the listen address changed, start the
	// new listener and drain the old one. In-flight requests hold their own
	// snapshot and finish on the old listener.
	releases.OnSwap = func(previous, next *release.Release) {
		serverMu.Lock()
		defer serverMu.Unlock()
		configs.Replace(next.Snapshot)
		log.Printf("release %s (%s) now serving; previous %s retained for in-flight requests",
			next.ID, next.Revision, previous.Revision)
		if next.Snapshot.Listen == previous.Snapshot.Listen {
			return
		}
		old := currentServer
		server := startServer(next.Snapshot.Listen, nil)
		currentServer = server
		go stopServer(old, previous.Snapshot.DrainTimeout)
	}

	// Legacy file reload: independently validate the file, then record and
	// promote a brand-new release. Invalid files leave service untouched.
	reload := func() error {
		nextFile, err := config.Load(*configPath)
		if err != nil {
			return err
		}
		if _, err := releases.DirectPromote(nextFile, "file reload from "+*configPath); err != nil {
			return err
		}
		return nil
	}
	adminAPI := admin.New(configs, upstreams, records, releases, gateway, *configPath, reload)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		current := releases.Current().Snapshot
		adminPath := strings.TrimSuffix(current.AdminPath, "/")
		if request.URL.Path == adminPath+"/reload" {
			adminAPI.Reload(writer, request)
			return
		}
		if strings.HasPrefix(request.URL.Path, current.AdminPath+"/") || request.URL.Path == current.AdminPath {
			adminAPI.ServeHTTP(writer, request)
			return
		}
		gateway.ServeHTTP(writer, request)
	})
	currentServer = startServer(initial.Listen, mux)

	signals := make(chan os.Signal, 2)
	shutdownDone := make(chan struct{})
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for event := range signals {
			if event == syscall.SIGHUP {
				if err := reload(); err != nil {
					log.Printf("reload failed: %v", err)
				}
				continue
			}
			log.Printf("shutting down; in-flight requests must finish within %s", initial.DrainTimeout)
			serverMu.Lock()
			server := currentServer
			serverMu.Unlock()
			go func() {
				shutdownCtx, cancel := context.WithTimeout(context.Background(), initial.DrainTimeout)
				defer cancel()
				if err := server.Shutdown(shutdownCtx); err != nil {
					log.Printf("listener shutdown: %v", err)
				}
			}()
			tracked, aborted := gateway.BeginDrain(initial.DrainTimeout)
			if aborted > 0 {
				log.Printf("drain: %d in-flight request(s) exceeded wait range and received 503 terminal", aborted)
				_ = server.Close()
			} else {
				log.Printf("drain: %d in-flight request(s) finished cleanly", tracked)
			}
			close(shutdownDone)
			return
		}
	}()
	<-shutdownDone
}

func startServer(addr string, handler http.Handler) *http.Server {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen on %s: %v", addr, err)
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("server error: %v", err)
		}
	}()
	log.Printf("gateway listening on %s", addr)
	return server
}

func stopServer(server *http.Server, drainTimeout time.Duration) {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("old listener graceful shutdown failed: %v", err)
	}
}
