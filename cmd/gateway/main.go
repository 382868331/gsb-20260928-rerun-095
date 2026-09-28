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
	"gateway/internal/state"
)

func main() {
	configPath := flag.String("config", "configs/gateway.json", "gateway configuration file")
	flag.Parse()

	loaded, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	initial, err := loaded.Snapshot()
	if err != nil {
		log.Fatalf("validate config: %v", err)
	}
	configs := config.NewStore(initial)
	upstreams := state.NewStore()
	records := record.NewStore(2000)
	gateway := proxy.New(configs, upstreams, records)

	reload := func() error {
		nextFile, err := config.Load(*configPath)
		if err != nil {
			return err
		}
		next, err := nextFile.Snapshot()
		if err != nil {
			return err
		}
		configs.Replace(next)
		log.Printf("configuration reloaded")
		return nil
	}
	adminAPI := admin.New(configs, upstreams, records, *configPath, reload)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		current := configs.Current()
		if request.URL.Path == strings.TrimSuffix(current.AdminPath, "/")+"/reload" {
			adminAPI.Reload(writer, request)
			return
		}
		if strings.HasPrefix(request.URL.Path, current.AdminPath+"/") || request.URL.Path == current.AdminPath {
			adminAPI.ServeHTTP(writer, request)
			return
		}
		gateway.ServeHTTP(writer, request)
	})

	var serverMu sync.Mutex
	var currentServer *http.Server
	activeServers := map[*http.Server]struct{}{}
	var draining sync.WaitGroup
	startServer := func(addr string) (*http.Server, error) {
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			return nil, err
		}
		server := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
		go func() {
			if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("server error: %v", err)
			}
		}()
		log.Printf("gateway listening on %s", addr)
		return server, nil
	}
	stopServer := func(server *http.Server) {
		draining.Add(1)
		defer draining.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), configs.Current().DrainTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("graceful shutdown failed: %v", err)
		}
	}
	currentServer, err = startServer(initial.Listen)
	if err != nil {
		log.Fatal(err)
	}
	activeServers[currentServer] = struct{}{}

	reload = func() error {
		nextFile, err := config.Load(*configPath)
		if err != nil {
			return err
		}
		next, err := nextFile.Snapshot()
		if err != nil {
			return err
		}
		serverMu.Lock()
		defer serverMu.Unlock()
		var replacement *http.Server
		if next.Listen != configs.Current().Listen {
			replacement, err = startServer(next.Listen)
			if err != nil {
				return err
			}
		}
		old := currentServer
		configs.Replace(next)
		if replacement != nil {
			currentServer = replacement
			activeServers[replacement] = struct{}{}
			delete(activeServers, old)
			go stopServer(old)
		}
		log.Printf("configuration reloaded")
		return nil
	}

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
			log.Printf("shutting down")
			serverMu.Lock()
			servers := make([]*http.Server, 0, len(activeServers))
			for server := range activeServers {
				servers = append(servers, server)
			}
			serverMu.Unlock()
			for _, server := range servers {
				go stopServer(server)
			}
			draining.Wait()
			close(shutdownDone)
		}
	}()
	<-shutdownDone
}
