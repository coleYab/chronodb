package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/coleYab/chronodb/internal/agent"
	"github.com/coleYab/chronodb/internal/config"
)

func main() {
	configPath := flag.String("config", "chrono-agent.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("failed to load config", "err", err)
		os.Exit(1)
	}

	a := agent.New(cfg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("chrono-agent starting", "output", cfg.Output.URL)
	if err := a.Run(ctx); err != nil {
		slog.Error("agent error", "err", err)
		os.Exit(1)
	}
}
