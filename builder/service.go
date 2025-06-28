// github.com/illmade-knight/go-iot-dataflows/builder/service.go
package builder

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// BaseConfig holds common configuration fields for all services.
type BaseConfig struct {
	LogLevel        string `mapstructure:"log_level"`
	HTTPPort        string `mapstructure:"http_port"`
	ProjectID       string `mapstructure:"project_id"`
	CredentialsFile string `mapstructure:"credentials_file"`
}

// ConfigLoader defines an interface for loading service-specific configurations.
type ConfigLoader[C any] interface {
	LoadConfig() (*C, error)
}

// Service defines the common interface for all microservices.
type Service interface {
	Start() error
	Shutdown()
	Mux() *http.ServeMux
	GetHTTPPort() string
}

// BaseServer provides common functionalities for microservice servers.
type BaseServer struct {
	Logger     zerolog.Logger
	HTTPPort   string
	httpServer *http.Server
	mux        *http.ServeMux
	actualAddr string // Stores the actual address the server is listening on
	mu         sync.RWMutex
}

// NewBaseServer creates and initializes a new BaseServer.
func NewBaseServer(logger zerolog.Logger, httpPort string) *BaseServer {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", HealthzHandler)

	return &BaseServer{
		Logger:   logger,
		HTTPPort: httpPort,
		mux:      mux,
		httpServer: &http.Server{
			Addr:    httpPort,
			Handler: mux,
		},
	}
}

// Start initiates the HTTP server in a background goroutine.
// It now correctly captures and stores the actual port when ":0" is used.
func (s *BaseServer) Start() error {
	// Create a listener on the configured port. If the port is ":0",
	// the OS will assign a random free port.
	listener, err := net.Listen("tcp", s.HTTPPort)
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %w", s.HTTPPort, err)
	}

	// Store the actual address, including the dynamically assigned port.
	s.mu.Lock()
	s.actualAddr = listener.Addr().String()
	s.mu.Unlock()

	s.Logger.Info().Str("address", s.actualAddr).Msg("HTTP server starting to listen")

	// Start serving requests in a background goroutine.
	go func() {
		if err := s.httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			s.Logger.Error().Err(err).Msg("HTTP server failed")
		}
	}()

	return nil
}

// Shutdown gracefully stops the HTTP server.
func (s *BaseServer) Shutdown() {
	s.Logger.Info().Msg("Shutting down HTTP server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.httpServer.Shutdown(ctx); err != nil {
		s.Logger.Error().Err(err).Msg("Error during HTTP server shutdown.")
	} else {
		s.Logger.Info().Msg("HTTP server stopped.")
	}
}

// GetHTTPPort returns the actual configured HTTP port the server is listening on.
// This is now safe to call from multiple goroutines.
func (s *BaseServer) GetHTTPPort() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Return the port part of the address (e.g., "8080" from "127.0.0.1:8080")
	_, port, err := net.SplitHostPort(s.actualAddr)
	if err != nil {
		// Fallback for safety, though it should not happen after Start()
		return s.HTTPPort
	}
	return ":" + port
}

// Mux returns the underlying ServeMux.
func (s *BaseServer) Mux() *http.ServeMux {
	return s.mux
}

// HealthzHandler responds to health check probes with a 200 OK status.
func HealthzHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}
