package server

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// Config holds server configuration
type Config struct {
	Port int
}

// Server wraps the HTTP server
type Server struct {
	server *http.Server
}

// NewServer creates a new Server instance
func NewServer(cfg Config, handler *Handler) *Server {
	mux := http.NewServeMux()

	// Read Operations
	mux.HandleFunc("GET /api/v1/{resource}/{namespace}/{name}", handler.HandleGet)
	mux.HandleFunc("GET /api/v1/{resource}/{namespace}", handler.HandleList)
	mux.HandleFunc("GET /api/v1/{resource}", handler.HandleList)

	// Write Operations
	mux.HandleFunc("POST /api/v1/{resource}/{namespace}", handler.HandleCreate)
	mux.HandleFunc("PUT /api/v1/{resource}/{namespace}/{name}", handler.HandleUpdate)
	mux.HandleFunc("PATCH /api/v1/{resource}/{namespace}/{name}", handler.HandlePatch)
	mux.HandleFunc("DELETE /api/v1/{resource}/{namespace}/{name}", handler.HandleDelete)

	return &Server{
		server: &http.Server{
			Addr:    fmt.Sprintf(":%d", cfg.Port),
			Handler: mux,
		},
	}
}

// Start starts the HTTP server
func (s *Server) Start() error {
	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("server listen failed: %w", err)
	}
	return nil
}

// Shutdown gracefully stops the server
func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

// Run starts the server in a goroutine and waits for context cancellation to shut it down
func (s *Server) Run(ctx context.Context) error {
	errChan := make(chan error, 1)
	go func() {
		errChan <- s.Start()
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.Shutdown(shutdownCtx)
	}
}

