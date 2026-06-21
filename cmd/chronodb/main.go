package main

import (
	"context"
	"log/slog"
	"os"

	httpapi "github.com/coleYab/chronodb/api/http"
	"github.com/coleYab/chronodb/internal/engine"
	"github.com/coleYab/chronodb/internal/index"
	"github.com/coleYab/chronodb/internal/series"
)

func main() {
	addr := ":8080"
	if v := os.Getenv("CHRONO_HTTP_ADDR"); v != "" {
		addr = v
	}
	walPath := os.Getenv("CHRONO_WAL_PATH")
	if walPath == "" {
		walPath = "data/chronodb.wal"
	}
	manifestPath := os.Getenv("CHRONO_MANIFEST_PATH")
	if manifestPath == "" {
		manifestPath = "data/manifest.json"
	}
	dataDir := os.Getenv("CHRONO_DATA_DIR")
	if dataDir == "" {
		dataDir = "data"
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		slog.Error("failed to create data dir", "err", err)
		os.Exit(1)
	}

	cfg := engine.DefaultConfig()
	cfg.WALPath = walPath
	cfg.ManifestPath = manifestPath
	cfg.DataDir = dataDir

	e, err := engine.New(cfg)
	if err != nil {
		slog.Error("failed to create engine", "err", err)
		os.Exit(1)
	}

	reg := series.NewRegistry()
	idx := index.New()
	e.SetIndex(idx)

	ctx, cancel := context.WithCancel(context.Background())
	go e.Run(ctx)

	slog.Info("chronodb starting", "addr", addr, "data_dir", dataDir)
	if err := httpapi.Serve(addr, e, reg, idx); err != nil {
		slog.Error("server error", "err", err)
		cancel()
		os.Exit(1)
	}
	cancel()
}
