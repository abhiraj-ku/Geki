package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/abhiraj-ku/geki/internals/config"
	"github.com/abhiraj-ku/geki/internals/proxy"
	"github.com/joho/godotenv"
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

	// instansiate the server
	srv := proxy.NewServer(cfg.ListnrAddr, cfg.TargetAddr)

	// Run this server
	if err := srv.Start(ctx); err != nil {
		log.Fatalf("server error: %v", err)
	}
	srv.Wait()
	log.Println("shutdown complete...")
}
