package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/coleYab/chronodb/internal/engine"
	"github.com/coleYab/chronodb/internal/index"
	"github.com/coleYab/chronodb/internal/series"
)

type Server struct {
	httpServer *http.Server
	engine     *engine.Engine
}

func NewServer(addr string, e *engine.Engine, reg *series.Registry, idx *index.Index) *Server {
	h := NewHandler(e, reg, idx)
	mux := http.NewServeMux()
	mux.HandleFunc("/batch", h.handleBatchAdd)
	mux.HandleFunc("/write", h.handleWrite)
	mux.HandleFunc("/query", h.handleQuery)
	mux.HandleFunc("/metrics", h.handleListMetrics)
	mux.HandleFunc("/engine/metrics", h.handleEngineMetrics)
	mux.HandleFunc("/series", h.handleSeries)
	mux.HandleFunc("/healthz", h.handleHealthz)
	mux.HandleFunc("/docs", h.handleDocs)
	mux.HandleFunc("/openapi.json", h.handleOpenAPI)
	mux.HandleFunc("/", h.handleLanding)

	return &Server{
		httpServer: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
		engine: e,
	}
}

func (s *Server) ListenAndServe() error {
	return s.httpServer.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func Serve(addr string, e *engine.Engine, reg *series.Registry, idx *index.Index) error {
	s := NewServer(addr, e, reg, idx)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		slog.Info("http server starting", "addr", addr)
		if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server error", "err", err)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.Shutdown(shutdownCtx)
}
