// github.com/illmade-knight/go-iot-dataflows/builder/enrichment/enapp.go
package enrichment

import (
	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"github.com/illmade-knight/go-cloud-manager/microservice"

	"github.com/illmade-knight/go-cloud-manager/microservice/servicedirector"

	"github.com/illmade-knight/go-iot/pkg/device"
	"github.com/illmade-knight/go-iot/pkg/enrichment"
	"github.com/illmade-knight/go-iot/pkg/messagepipeline"
	"github.com/illmade-knight/go-iot/pkg/types"
	"github.com/rs/zerolog"
	"google.golang.org/api/option"
	"net/http"
)

// EnrichmentServiceWrapper wraps the processing service for a common interface.
type EnrichmentServiceWrapper[T any] struct {
	*microservice.BaseServer
	wrapperContext         context.Context
	serviceCancel          context.CancelFunc
	processingService      *messagepipeline.ProcessingService[T]
	metadataFetcherCleanup func() error
	logger                 zerolog.Logger
}

// MessageEnricherFactory defines a function that creates a MessageTransformer.
// It accepts the dependencies needed by the enricher and returns the transformer function.
type MessageEnricherFactory[T any] func(fetcher device.DeviceMetadataFetcher, logger zerolog.Logger) messagepipeline.MessageTransformer[T]

// NewPublishMessageEnrichmentServiceWrapper creates and configures a new EnrichmentServiceWrapper.
func NewPublishMessageEnrichmentServiceWrapper(
	cfg *Config,
	parentContext context.Context,
	logger zerolog.Logger,
) (*EnrichmentServiceWrapper[types.PublishMessage], error) {

	enrichmentLogger := logger.With().Str("component", "EnrichmentService").Logger()

	// Create a new context that we can serviceCancel from our Shutdown method
	serviceCtx, serviceCancel := context.WithCancel(parentContext)

	// --- Verify resources with Director ---
	if cfg.ServiceDirectorURL != "" {
		directorClient, err := servicedirector.NewClient(cfg.ServiceDirectorURL, enrichmentLogger)
		if err != nil {
			return nil, fmt.Errorf("failed to create service director client: %w", err)
		}
		if err := directorClient.VerifyDataflow(serviceCtx, cfg.DataflowName, cfg.ServiceName); err != nil {
			return nil, fmt.Errorf("resource verification failed via Director: %w", err)
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

	psClient, err := pubsub.NewClient(serviceCtx, cfg.ProjectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub client: %w", err)
	}

	fsClient, err := firestore.NewClient(serviceCtx, cfg.ProjectID, opts...)
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

	sourceFetcher, err := device.NewGoogleDeviceMetadataFetcher(fsClient, cfg.CacheConfig.FirestoreConfig, enrichmentLogger)
	if err != nil {
		psClient.Close()
		fsClient.Close()
		return nil, fmt.Errorf("failed to create Firestore metadata fetcher: %w", err)
	}

	redisCache, err := device.NewRedisDeviceMetadataFetcher(serviceCtx, &cfg.CacheConfig.RedisConfig, enrichmentLogger)
	if err != nil {
		psClient.Close()
		fsClient.Close()
		return nil, fmt.Errorf("failed to create redis cache layer: %w", err)
	}
	metadataFetcher, metadataCleanup, err := device.NewCacheFallbackFetcher(serviceCtx, redisCache, sourceFetcher, enrichmentLogger)
	if err != nil {
		psClient.Close()
		fsClient.Close()
		return nil, fmt.Errorf("failed to create chained metadata fetcher: %w", err)
	}

	mainProducer, err := messagepipeline.NewGooglePubsubProducer[types.PublishMessage](
		psClient,
		cfg.ProducerConfig,
		enrichmentLogger,
	)

	enricher := enrichment.NewMessageEnricher(metadataFetcher, nil, logger)

	processingService, err := messagepipeline.NewProcessingService[types.PublishMessage](2, consumer, mainProducer, enricher, logger)

	// --- Setup Base Server ---
	baseServer := microservice.NewBaseServer(enrichmentLogger, cfg.HTTPPort)

	return &EnrichmentServiceWrapper[types.PublishMessage]{
		BaseServer:             baseServer,
		serviceCancel:          serviceCancel,
		wrapperContext:         serviceCtx,
		processingService:      processingService,
		metadataFetcherCleanup: metadataCleanup,
		logger:                 enrichmentLogger,
	}, nil
}

// Start initiates the enrichment processing service and the embedded HTTP server.
func (s *EnrichmentServiceWrapper[T]) Start() error {
	s.logger.Info().Msg("Starting enrichment server components...")
	if err := s.processingService.Start(s.wrapperContext); err != nil {
		return fmt.Errorf("failed to start processing service: %w", err)
	}
	s.logger.Info().Msg("Data processing service started.")
	return s.BaseServer.Start()
}

// Shutdown gracefully stops the enrichment processing service and its clients.
func (s *EnrichmentServiceWrapper[T]) Shutdown() {
	s.logger.Info().Msg("Shutting down enrichment server components...")
	s.serviceCancel()
	s.processingService.Stop()
	s.logger.Info().Msg("Data processing service stopped.")
	s.BaseServer.Shutdown()

	if s.metadataFetcherCleanup != nil {
		if err := s.metadataFetcherCleanup(); err != nil {
			s.logger.Error().Err(err).Msg("Error during metadata fetcher cleanup")
		}
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
