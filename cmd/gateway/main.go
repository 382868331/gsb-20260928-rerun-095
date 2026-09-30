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

func loadSnapshot(path string) (*config.Snapshot, error) {
	loaded, err := config.Load(path)
	if err != nil {
		return nil, err
	}
	return loaded.Snapshot()
}

func main() {
	configPath := flag.String("config", "configs/gateway.json", "gateway configuration file")
	flag.Parse()

	initial, err := loadSnapshot(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	releases := release.NewStore(initial)
	upstreams := state.NewStore()
	records := record.NewStore(2000)
	gateway := proxy.New(releases, upstreams, records)

	var adminAPI *admin.Admin
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		current := releases.Current()
		if request.URL.Path == current.AdminPath || strings.HasPrefix(request.URL.Path, current.AdminPath+"/") {
			adminAPI.ServeHTTP(writer, request)
			return
		}
		gateway.ServeHTTP(writer, request)
	})

	var serverMu sync.Mutex
	servers := map[string]*http.Server{}
	var draining sync.WaitGroup
	stopServer := func(server *http.Server) {
		draining.Add(1)
		defer draining.Done()
		if err := server.Shutdown(context.Background()); err != nil {
			log.Printf("server shutdown failed: %v", err)
		}
	}
	// ensureListener starts a listener for the active release address and
	// drains any listeners the active release no longer uses, so in-flight
	// requests on old listeners finish on their original connection.
	ensureListener := func() error {
		want := releases.Current().Listen
		serverMu.Lock()
		defer serverMu.Unlock()
		if _, ok := servers[want]; !ok {
			listener, err := net.Listen("tcp", want)
			if err != nil {
				return err
			}
			server := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
			servers[want] = server
			go func() {
				if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
					log.Printf("server error on %s: %v", want, err)
				}
			}()
			log.Printf("gateway listening on %s", want)
		}
		for addr, server := range servers {
			if addr != want {
				delete(servers, addr)
				go stopServer(server)
			}
		}
		return nil
	}

	stage := func() (release.Candidate, error) {
		snapshot, err := loadSnapshot(*configPath)
		if err != nil {
			return release.Candidate{}, err
		}
		candidate := releases.Stage(snapshot)
		log.Printf("staged candidate %s routes=%v", candidate.ID, candidate.Routes)
		return candidate, nil
	}
	confirm := func(candidateID string) (release.Release, error) {
		activated, err := releases.Confirm(candidateID)
		if err != nil {
			return release.Release{}, err
		}
		if err := ensureListener(); err != nil {
			log.Printf("release %s active but listener update failed: %v", activated.ID, err)
		}
		log.Printf("confirmed release %s (from %s)", activated.ID, activated.SourceID)
		return activated, nil
	}
	rollback := func(target string) (release.Release, error) {
		activated, err := releases.Rollback(target)
		if err != nil {
			return release.Release{}, err
		}
		if err := ensureListener(); err != nil {
			log.Printf("release %s active but listener update failed: %v", activated.ID, err)
		}
		log.Printf("rolled back to %s as new release %s", activated.SourceID, activated.ID)
		return activated, nil
	}
	adminAPI = admin.New(releases, upstreams, records, stage, confirm, rollback)

	if err := ensureListener(); err != nil {
		log.Fatal(err)
	}

	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	for event := range signals {
		if event == syscall.SIGHUP {
			if _, err := stage(); err != nil {
				log.Printf("stage failed: %v", err)
			}
			continue
		}
		log.Printf("shutting down")
		gateway.BeginDrain()
		drainTimeout := releases.Current().DrainTimeout
		if !gateway.WaitInflight(drainTimeout) {
			log.Printf("drain deadline %s exceeded; aborting remaining in-flight requests", drainTimeout)
			gateway.AbortInflight()
			gateway.WaitInflight(10 * time.Second)
		}
		serverMu.Lock()
		serversToStop := make([]*http.Server, 0, len(servers))
		for _, server := range servers {
			serversToStop = append(serversToStop, server)
		}
		serverMu.Unlock()
		for _, server := range serversToStop {
			go stopServer(server)
		}
		draining.Wait()
		return
	}
}
