// github.com/illmade-knight/go-iot-dataflows/builder/ingestion/app.go
package ingestion

import (
	"context"
	"fmt"
	"github.com/illmade-knight/go-cloud-manager/microservice"
	"net/http"

	"github.com/illmade-knight/go-cloud-manager/microservice/servicedirector"

	"cloud.google.com/go/pubsub"
	"github.com/illmade-knight/go-iot/pkg/mqttconverter"
	"github.com/rs/zerolog"
	"google.golang.org/api/option"
)

// IngestionServiceWrapper wraps the core MQTT ingestion logic.
type IngestionServiceWrapper struct {
	*microservice.BaseServer
	ingestionService *mqttconverter.IngestionService
	pubsubClient     *pubsub.Client
	logger           zerolog.Logger
}

// NewIngestionServiceWrapper creates and configures a new IngestionServiceWrapper.
// It now performs a resource verification check against the Director on startup.
// NewIngestionServiceWrapper creates and configures a new IngestionServiceWrapper.
// It is now updated to accept an AttributeExtractor to enable attribute injection.
func NewIngestionServiceWrapper(
	cfg *Config,
	extractor mqttconverter.AttributeExtractor, // CORRECTED: Added extractor parameter
	logger zerolog.Logger,
	serviceName string,
	dataflowName string,
) (*IngestionServiceWrapper, error) {
	ctx := context.Background()
	ingestionLogger := logger.With().Str("component", "IngestionService").Logger()

	// --- Verify resources with Director ---
	if cfg.ServiceDirectorURL != "" {
		directorClient, err := servicedirector.NewClient(cfg.ServiceDirectorURL, ingestionLogger)
		if err != nil {
			return nil, fmt.Errorf("failed to create service director client: %w", err)
		}
		if err := directorClient.VerifyDataflow(ctx, dataflowName, serviceName); err != nil {
			return nil, fmt.Errorf("resource verification failed via Director: %w", err)
		}
	} else {
		ingestionLogger.Warn().Msg("ServiceDirectorURL not set, skipping resource verification. This is not recommended for production.")
	}

	// --- Initialize service components ---
	var opts []option.ClientOption
	if cfg.Publisher.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.Publisher.CredentialsFile))
		ingestionLogger.Info().Str("credentials_file", cfg.Publisher.CredentialsFile).Msg("Using specified credentials file for Pub/Sub client")
	} else {
		ingestionLogger.Info().Msg("Using Application Default Credentials (ADC) for Pub/Sub client")
	}

	psClient, err := pubsub.NewClient(ctx, cfg.ProjectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub client: %w", err)
	}

	publisherCfg := mqttconverter.GooglePubsubPublisherConfig{
		ProjectID: cfg.ProjectID,
		TopicID:   cfg.Publisher.TopicID,
	}
	publisher, err := mqttconverter.NewGooglePubsubPublisher(ctx, publisherCfg, ingestionLogger)
	if err != nil {
		psClient.Close()
		return nil, fmt.Errorf("failed to create Google Pub/Sub publisher: %w", err)
	}

	ingestionService := mqttconverter.NewIngestionService(
		publisher,
		extractor, // CORRECTED: Pass the provided extractor to the service.
		ingestionLogger,
		cfg.Service,
		cfg.MQTT,
	)

	baseServer := microservice.NewBaseServer(ingestionLogger, cfg.HTTPPort)

	return &IngestionServiceWrapper{
		BaseServer:       baseServer,
		ingestionService: ingestionService,
		pubsubClient:     psClient,
		logger:           ingestionLogger,
	}, nil
}

// Start initiates the MQTT ingestion service and the embedded HTTP server.
func (s *IngestionServiceWrapper) Start() error {
	s.logger.Info().Msg("Starting MQTT ingestion server components...")
	if err := s.ingestionService.Start(); err != nil {
		s.logger.Error().Err(err).Msg("failed to start core ingestion service")
		return err
	}
	return s.BaseServer.Start()
}

// Shutdown gracefully stops the MQTT ingestion service and the embedded HTTP server.
func (s *IngestionServiceWrapper) Shutdown() {
	s.logger.Info().Msg("Shutting down MQTT ingestion server components...")
	s.ingestionService.Stop()
	s.logger.Info().Msg("Core ingestion service stopped.")

	s.BaseServer.Shutdown()

	if s.pubsubClient != nil {
		s.pubsubClient.Close()
		s.logger.Info().Msg("Pub/Sub client closed.")
	}
}

// Mux returns the HTTP ServeMux for the service.
func (s *IngestionServiceWrapper) Mux() *http.ServeMux {
	return s.BaseServer.Mux()
}

// GetHTTPPort returns the HTTP port the service is listening on.
func (s *IngestionServiceWrapper) GetHTTPPort() string {
	return s.BaseServer.GetHTTPPort()
}
