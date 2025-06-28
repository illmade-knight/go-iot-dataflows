package servicedirector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/illmade-knight/go-iot-dataflows/builder"
	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/rs/zerolog"
)

// ServiceDirector implements the builder.Service interface.
type ServiceDirector struct {
	*builder.BaseServer
	serviceManager *servicemanager.ServiceManager
	servicesDef    servicemanager.ServicesDefinition
	config         *Config
	logger         zerolog.Logger
}

// NewServiceDirector creates and initializes a new ServiceDirector instance.
// It now accepts a schemaRegistry to be passed down to the ServiceManager.
func NewServiceDirector(ctx context.Context, cfg *Config, loader ServicesDefinitionLoader, schemaRegistry map[string]interface{}, logger zerolog.Logger) (*ServiceDirector, error) {
	directorLogger := logger.With().Str("component", "ServiceDirector").Logger()

	servicesDef, err := loader.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("director: failed to load service definitions: %w", err)
	}

	// If the provided schema registry is nil, create an empty one to avoid panics.
	if schemaRegistry == nil {
		schemaRegistry = make(map[string]interface{})
	}

	// Pass the provided schemaRegistry to the ServiceManager.
	sm, err := servicemanager.NewServiceManager(ctx, servicesDef, cfg.Environment, schemaRegistry, directorLogger)
	if err != nil {
		return nil, fmt.Errorf("director: failed to create ServiceManager: %w", err)
	}

	baseServer := builder.NewBaseServer(directorLogger, cfg.HTTPPort)
	mux := baseServer.Mux()

	// Orchestration Endpoints
	mux.HandleFunc("/orchestrate/setup", func(w http.ResponseWriter, r *http.Request) {
		setupHandler(sm, cfg.Environment, directorLogger, w, r)
	})
	mux.HandleFunc("/orchestrate/teardown", func(w http.ResponseWriter, r *http.Request) {
		teardownHandler(sm, cfg.Environment, directorLogger, w, r)
	})

	// Configuration Serving Endpoint
	mux.HandleFunc("/config/", func(w http.ResponseWriter, r *http.Request) {
		configHandler(servicesDef, directorLogger, w, r)
	})

	// --- NEW: Verification Endpoint ---
	mux.HandleFunc("/verify/dataflow", func(w http.ResponseWriter, r *http.Request) {
		verifyHandler(sm, cfg.Environment, directorLogger, w, r)
	})

	directorLogger.Info().
		Str("http_port", cfg.HTTPPort).
		Str("environment", cfg.Environment).
		Str("services_def_source", cfg.ServicesDefSourceType).
		Msg("ServiceDirector initialized.")

	return &ServiceDirector{
		BaseServer:     baseServer,
		serviceManager: sm,
		servicesDef:    servicesDef,
		config:         cfg,
		logger:         directorLogger,
	}, nil
}

// ... Start, Shutdown, Mux, GetHTTPPort methods remain the same ...

func (sd *ServiceDirector) Start() error {
	sd.logger.Info().Msg("Starting ServiceDirector...")
	if err := sd.BaseServer.Start(); err != nil {
		sd.logger.Error().Err(err).Msg("ServiceDirector HTTP server failed to start")
		return err
	}
	sd.logger.Info().Msg("ServiceDirector started successfully.")
	return nil
}

func (sd *ServiceDirector) Shutdown() {
	sd.logger.Info().Msg("Shutting down ServiceDirector...")
	sd.BaseServer.Shutdown()
	sd.logger.Info().Msg("ServiceDirector shut down gracefully.")
}

func (sd *ServiceDirector) Mux() *http.ServeMux {
	return sd.BaseServer.Mux()
}

func (sd *ServiceDirector) GetHTTPPort() string {
	return sd.BaseServer.GetHTTPPort()
}

// --- Handlers ---

// VerifyDataflowRequest is the expected payload for the verification endpoint.
type VerifyDataflowRequest struct {
	DataflowName string `json:"dataflow_name"`
	ServiceName  string `json:"service_name"`
}

// verifyHandler handles requests from microservices to verify their dataflow resources.
func verifyHandler(sm *servicemanager.ServiceManager, env string, logger zerolog.Logger, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req VerifyDataflowRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.DataflowName == "" || req.ServiceName == "" {
		http.Error(w, "dataflow_name and service_name must be provided", http.StatusBadRequest)
		return
	}

	logger.Info().Str("dataflow", req.DataflowName).Str("service", req.ServiceName).Msg("Received request to verify dataflow.")

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()

	// Use the ServiceManager to perform the verification.
	err := sm.VerifyDataflow(ctx, env, req.DataflowName)
	if err != nil {
		logger.Error().Err(err).Str("dataflow", req.DataflowName).Msg("Dataflow verification failed")
		http.Error(w, fmt.Sprintf("Dataflow '%s' verification failed: %v", req.DataflowName, err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(fmt.Sprintf("Dataflow '%s' verified successfully for service '%s'.", req.DataflowName, req.ServiceName)))
	logger.Info().Str("dataflow", req.DataflowName).Msg("Successfully completed verification of dataflow.")
}

// setupHandler, teardownHandler, and configHandler remain the same
func setupHandler(sm *servicemanager.ServiceManager, env string, logger zerolog.Logger, w http.ResponseWriter, r *http.Request) {
	logger.Info().Msg("Received request to setup all dataflows.")
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	_, err := sm.SetupAll(ctx, env)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to setup all dataflows")
		http.Error(w, fmt.Sprintf("Failed to setup all dataflows: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("All dataflows set up successfully."))
	logger.Info().Msg("Successfully completed setup of all dataflows.")
}

func teardownHandler(sm *servicemanager.ServiceManager, env string, logger zerolog.Logger, w http.ResponseWriter, r *http.Request) {
	logger.Info().Msg("Received request to teardown ephemeral dataflows.")
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	err := sm.TeardownAll(ctx, env)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to teardown all dataflows")
		http.Error(w, fmt.Sprintf("Failed to teardown all dataflows: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("All ephemeral dataflows torn down successfully."))
	logger.Info().Msg("Successfully completed teardown of ephemeral dataflows.")
}

func configHandler(servicesDef servicemanager.ServicesDefinition, logger zerolog.Logger, w http.ResponseWriter, r *http.Request) {
	serviceName := r.URL.Path[len("/config/"):]
	if serviceName == "" {
		http.Error(w, "Service name not provided in path, e.g., /config/my-service", http.StatusBadRequest)
		return
	}

	logger.Info().Str("service_name", serviceName).Msg("Received request for service configuration.")

	svcSpec, err := servicesDef.GetService(serviceName)
	if err != nil {
		logger.Warn().Err(err).Str("service_name", serviceName).Msg("Service configuration not found")
		http.Error(w, fmt.Sprintf("Service '%s' configuration not found: %v", serviceName, err), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(svcSpec); err != nil {
		logger.Error().Err(err).Str("service_name", serviceName).Msg("Failed to encode service configuration to JSON")
		http.Error(w, "Internal server error: Failed to encode config", http.StatusInternalServerError)
		return
	}

	logger.Info().Str("service_name", serviceName).Msg("Successfully served service configuration.")
}
