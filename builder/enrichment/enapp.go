// github.com/illmade-knight/go-iot-dataflows/builder/enrichment/enapp.go
package enrichment

import (
	"context"
	"fmt"
	"net/http"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub"
	"github.com/illmade-knight/go-iot-dataflows/builder"
	"github.com/illmade-knight/go-iot-dataflows/builder/servicedirector"
	"github.com/illmade-knight/go-iot/pkg/device"
	"github.com/illmade-knight/go-iot/pkg/messagepipeline"
	"github.com/rs/zerolog"
	"google.golang.org/api/option"
)

// EnrichmentServiceWrapper wraps the processing service for a common interface.
type EnrichmentServiceWrapper[T any] struct {
	*builder.BaseServer
	processingService      *messagepipeline.ProcessingService[T]
	pubsubClient           *pubsub.Client
	firestoreClient        *firestore.Client
	metadataFetcherCleanup func() error
	logger                 zerolog.Logger
}

// MessageEnricherFactory defines a function that creates a MessageTransformer.
// It accepts the dependencies needed by the enricher and returns the transformer function.
type MessageEnricherFactory[T any] func(fetcher device.DeviceMetadataFetcher, logger zerolog.Logger) messagepipeline.MessageTransformer[T]

// NewEnrichmentServiceWrapper creates and configures a new EnrichmentServiceWrapper.
func NewEnrichmentServiceWrapper[T any](
	cfg *Config,
	logger zerolog.Logger,
	enricherFactory MessageEnricherFactory[T],
) (*EnrichmentServiceWrapper[T], error) {
	ctx := context.Background()
	enrichmentLogger := logger.With().Str("component", "EnrichmentService").Logger()

	// --- Verify resources with ServiceDirector ---
	if cfg.ServiceDirectorURL != "" {
		directorClient, err := servicedirector.NewClient(cfg.ServiceDirectorURL, enrichmentLogger)
		if err != nil {
			return nil, fmt.Errorf("failed to create service director client: %w", err)
		}
		if err := directorClient.VerifyDataflow(ctx, cfg.DataflowName, cfg.ServiceName); err != nil {
			return nil, fmt.Errorf("resource verification failed via ServiceDirector: %w", err)
		}
		enrichmentLogger.Info().Msg("Resource verification successful.")
	} else {
		enrichmentLogger.Warn().Msg("ServiceDirectorURL not set, skipping resource verification.")
	}

	// --- Initialize Clients ---
	var opts []option.ClientOption
	if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
	}

	psClient, err := pubsub.NewClient(ctx, cfg.ProjectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub client: %w", err)
	}

	fsClient, err := firestore.NewClient(ctx, cfg.ProjectID, opts...)
	if err != nil {
		psClient.Close()
		return nil, fmt.Errorf("failed to create Firestore client: %w", err)
	}

	// --- Initialize Pipeline Components ---
	// Correctly create the config struct expected by the library function.
	consumerCfg := &messagepipeline.GooglePubsubConsumerConfig{
		ProjectID:      cfg.ProjectID,
		SubscriptionID: cfg.Consumer.SubscriptionID,
	}
	consumer, err := messagepipeline.NewGooglePubsubConsumer(consumerCfg, psClient, enrichmentLogger)
	if err != nil {
		psClient.Close()
		fsClient.Close()
		return nil, fmt.Errorf("failed to create Pub/Sub consumer: %w", err)
	}

	producer, err := messagepipeline.NewGooglePubsubProducer[T](psClient, cfg.Producer, enrichmentLogger)
	if err != nil {
		psClient.Close()
		fsClient.Close()
		return nil, fmt.Errorf("failed to create Pub/Sub producer: %w", err)
	}

	sourceFetcher, err := device.NewGoogleDeviceMetadataFetcher(fsClient, cfg.CacheConfig.FirestoreConfig, enrichmentLogger)
	if err != nil {
		psClient.Close()
		fsClient.Close()
		return nil, fmt.Errorf("failed to create Firestore metadata fetcher: %w", err)
	}

	metadataFetcher, cleanup, err := device.NewChainedFetcher(ctx, &cfg.CacheConfig.RedisConfig, sourceFetcher, enrichmentLogger)
	if err != nil {
		psClient.Close()
		fsClient.Close()
		return nil, fmt.Errorf("failed to create chained metadata fetcher: %w", err)
	}

	// Create the core message processing service.
	processingService, err := messagepipeline.NewProcessingService[T](
		cfg.ProcessorConfig.NumWorkers,
		consumer,
		producer,
		enricherFactory(metadataFetcher, enrichmentLogger),
		enrichmentLogger,
	)
	if err != nil {
		psClient.Close()
		fsClient.Close()
		cleanup() // Clean up the fetcher's resources
		return nil, fmt.Errorf("failed to create processing service: %w", err)
	}

	// --- Setup Base Server ---
	baseServer := builder.NewBaseServer(enrichmentLogger, cfg.HTTPPort)

	return &EnrichmentServiceWrapper[T]{
		BaseServer:             baseServer,
		processingService:      processingService,
		pubsubClient:           psClient,
		firestoreClient:        fsClient,
		metadataFetcherCleanup: cleanup,
		logger:                 enrichmentLogger,
	}, nil
}

// Start initiates the enrichment processing service and the embedded HTTP server.
func (s *EnrichmentServiceWrapper[T]) Start() error {
	s.logger.Info().Msg("Starting enrichment server components...")
	if err := s.processingService.Start(); err != nil {
		return fmt.Errorf("failed to start processing service: %w", err)
	}
	s.logger.Info().Msg("Data processing service started.")
	return s.BaseServer.Start()
}

// Shutdown gracefully stops the enrichment processing service and its clients.
func (s *EnrichmentServiceWrapper[T]) Shutdown() {
	s.logger.Info().Msg("Shutting down enrichment server components...")
	s.processingService.Stop()
	s.logger.Info().Msg("Data processing service stopped.")
	s.BaseServer.Shutdown()

	if s.metadataFetcherCleanup != nil {
		if err := s.metadataFetcherCleanup(); err != nil {
			s.logger.Error().Err(err).Msg("Error during metadata fetcher cleanup")
		}
	}
	if s.firestoreClient != nil {
		s.firestoreClient.Close()
	}
	if s.pubsubClient != nil {
		s.pubsubClient.Close()
	}
	s.logger.Info().Msg("Enrichment server shut down gracefully.")
}

// Mux returns the HTTP ServeMux to register additional handlers.
func (s *EnrichmentServiceWrapper[T]) Mux() *http.ServeMux {
	return s.BaseServer.Mux()
}

// GetHTTPPort returns the HTTP port the service is listening on.
func (s *EnrichmentServiceWrapper[T]) GetHTTPPort() string {
	return s.BaseServer.GetHTTPPort()
}
