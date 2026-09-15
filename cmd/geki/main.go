package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/abhiraj-ku/geki/internals/config"
	"github.com/abhiraj-ku/geki/internals/proxy"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	if err := godotenv.Load(); err != nil {
		panic("env variable init failed")
	}
	cfg := config.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("\nRecieved shutdown signal. stopping new conn...")
		cancel()
	}()

	// setup the prometheus collector endpoint (port: 9090)
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("[Metric] exposing prom ednpoint on :9090/metrics")
		if err := http.ListenAndServe(":9090", nil); err != nil {
			log.Fatalf("[metrics] fasiled to start prom server: %v", err)
		}
	}()

	// instansiate the server
	// For local development, we pass the same local target address for both
	// primary and replica. In production, we will have two different
	// addresses.
	srv := proxy.NewServer(cfg.ListnrAddr, cfg.TargetAddr, cfg.TargetAddr)

	// Run this server
	if err := srv.Start(ctx); err != nil {
		log.Fatalf("server error: %v", err)
	}
	srv.Wait()
	log.Println("shutdown complete...")
}
