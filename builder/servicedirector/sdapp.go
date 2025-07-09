package servicedirector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/illmade-knight/go-cloud-manager/pkg/servicemanager"
	"github.com/illmade-knight/go-iot-dataflows/builder"
	"github.com/rs/zerolog"
)

// Director implements the builder.Service interface for our main application.
type Director struct {
	*builder.BaseServer
	serviceManager *servicemanager.ServiceManager
	architecture   *servicemanager.MicroserviceArchitecture // Holds the loaded architecture
	config         *Config
	logger         zerolog.Logger
}

// NewServiceDirector creates and initializes a new Director instance.
func NewServiceDirector(ctx context.Context, cfg *Config, loader servicemanager.ArchitectureIO, schemaRegistry map[string]interface{}, logger zerolog.Logger) (*Director, error) {
	directorLogger := logger.With().Str("component", "Director").Logger()

	// Load the entire microservice architecture using the provided loader.
	arch, err := loader.LoadArchitecture(ctx)
	if err != nil {
		return nil, fmt.Errorf("director: failed to load service definitions: %w", err)
	}
	logger.Info().Str("projectID", arch.ProjectID).Msg("loaded project architecture")

	// Create the stateless ServiceManager engine, passing in the default environment for client creation.
	sm, err := servicemanager.NewServiceManager(ctx, arch.Environment, schemaRegistry, nil, directorLogger)
	if err != nil {
		return nil, fmt.Errorf("director: failed to create ServiceManager: %w", err)
	}

	baseServer := builder.NewBaseServer(directorLogger, cfg.HTTPPort)

	// Create the Director instance, storing both the engine (sm) and the config (arch).
	d := &Director{
		BaseServer:     baseServer,
		serviceManager: sm,
		architecture:   arch, // <-- The architecture is now stored here
		config:         cfg,
		logger:         directorLogger,
	}

	// Register handlers as methods on the Director struct.
	// This gives them access to the stored architecture via the `d` receiver.
	mux := baseServer.Mux()
	mux.HandleFunc("/orchestrate/setup", d.setupHandler)
	mux.HandleFunc("/orchestrate/teardown", d.teardownHandler)
	mux.HandleFunc("/verify/dataflow", d.verifyHandler)

	directorLogger.Info().
		Str("http_port", cfg.HTTPPort).
		Str("services_def_source", cfg.ServicesDefSourceType).
		Msg("Director initialized.")

	return d, nil
}

// Start, Shutdown, and other builder methods remain the same.
func (d *Director) Start() error {
	return d.BaseServer.Start()
}

// --- Handlers as Methods ---

// VerifyDataflowRequest is the expected payload for the verification endpoint.
type VerifyDataflowRequest struct {
	DataflowName string `json:"dataflow_name"`
	ServiceName  string `json:"service_name"` // Included for logging/auditing
}

// setupHandler now calls the stateless SetupAll method.
func (d *Director) setupHandler(w http.ResponseWriter, r *http.Request) {
	d.logger.Info().Msg("Received request to setup all dataflows.")
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()

	if _, err := d.serviceManager.SetupAll(ctx, d.architecture); err != nil {
		d.logger.Error().Err(err).Msg("Failed to setup all dataflows")
		http.Error(w, fmt.Sprintf("Failed to setup all dataflows: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("All dataflows set up successfully."))
	d.logger.Info().Msg("Successfully completed setup of all dataflows.")
}

// teardownHandler now calls the stateless TeardownAll method.
func (d *Director) teardownHandler(w http.ResponseWriter, r *http.Request) {
	d.logger.Info().Msg("Received request to teardown ephemeral dataflows.")
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()

	if err := d.serviceManager.TeardownAll(ctx, d.architecture); err != nil {
		d.logger.Error().Err(err).Msg("Failed to teardown all dataflows")
		http.Error(w, fmt.Sprintf("Failed to teardown all dataflows: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("All ephemeral dataflows torn down successfully."))
}

// verifyHandler handles requests from microservices to verify their dataflow resources.
func (d *Director) verifyHandler(w http.ResponseWriter, r *http.Request) {
	var req VerifyDataflowRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	d.logger.Info().Str("dataflow", req.DataflowName).Str("service", req.ServiceName).Msg("Received request to verify dataflow.")
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()

	// Use the ServiceManager to perform the verification, passing in the architecture.
	err := d.serviceManager.VerifyDataflow(ctx, d.architecture, req.DataflowName)
	if err != nil {
		d.logger.Error().Err(err).Str("dataflow", req.DataflowName).Msg("Dataflow verification failed")
		http.Error(w, fmt.Sprintf("Dataflow '%s' verification failed: %v", req.DataflowName, err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(fmt.Sprintf("Dataflow '%s' verified successfully.", req.DataflowName)))
}
