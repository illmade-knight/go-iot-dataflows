package bqinit

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/illmade-knight/go-iot/pkg/messagepipeline"
	"github.com/rs/zerolog"
)

// --- Application Server (Generic) ---

// Server holds all the components of our microservice.
// It is now generic and can handle any type T that is to be processed.
type Server[T any] struct {
	logger          zerolog.Logger
	config          *Config
	batchProcessing *messagepipeline.ProcessingService[T]
	httpServer      *http.Server
}

// NewServer creates and configures a new generic Server instance.
func NewServer[T any](cfg *Config, b *messagepipeline.ProcessingService[T], logger zerolog.Logger) *Server[T] {
	return &Server[T]{
		logger:          logger,
		config:          cfg,
		batchProcessing: b,
	}
}

// GetHTTPPort returns the server's configured HTTP port.
func (s *Server[T]) GetHTTPPort() string {
	return s.config.HTTPPort
}

// GetHandler returns the server's HTTP handler for health checks.
func (s *Server[T]) GetHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/healthz", s.healthzHandler)
	return mux
}

// Start runs the main application logic.
func (s *Server[T]) Start() error {
	s.logger.Info().Msg("Starting generic BQ server...")

	if err := s.batchProcessing.Start(); err != nil {
		return fmt.Errorf("failed to start processing service: %w", err)
	}
	s.logger.Info().Msg("Data processing service started.")

	s.httpServer = &http.Server{
		Addr:    s.config.HTTPPort,
		Handler: s.GetHandler(),
	}

	s.logger.Info().Str("address", s.config.HTTPPort).Msg("Starting health check server.")
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("health check server failed: %w", err)
	}

	return nil
}

// Shutdown gracefully stops all components of the service.
func (s *Server[T]) Shutdown() {
	s.logger.Info().Msg("Shutting down generic BQ server...")

	s.batchProcessing.Stop()
	s.logger.Info().Msg("Data processing service stopped.")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(ctx); err != nil {
			s.logger.Error().Err(err).Msg("Error during health check server shutdown.")
		} else {
			s.logger.Info().Msg("Health check server stopped.")
		}
	}
}

// healthzHandler responds to health check probes.
func (s *Server[T]) healthzHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}
