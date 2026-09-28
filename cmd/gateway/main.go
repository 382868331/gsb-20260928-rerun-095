package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"gateway/internal/config"
	"gateway/internal/gateway"
)

func main() {
	configPath := flag.String("config", "configs/gateway.json", "gateway configuration file")
	flag.Parse()

	cfg, err := config.LoadFile(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	server, err := gateway.ListenAndServe(cfg)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("gateway listening on %s", cfg.ListenAddr)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	_ = server.Close()
}
