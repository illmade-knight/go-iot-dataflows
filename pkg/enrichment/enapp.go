// github.com/illmade-knight/go-iot-dataflows/builder/enrichment/enapp.go
package enrichment

import (
	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"net/http"

	"github.com/illmade-knight/go-cloud-manager/microservice"
	"github.com/illmade-knight/go-cloud-manager/microservice/servicedirector"
	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/illmade-knight/go-dataflow/pkg/enrichment"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/types"

	"github.com/rs/zerolog"
	"google.golang.org/api/option"
)

// DeviceMetadata is the concrete data structure for device enrichment.
type DeviceMetadata struct {
	ClientID   string `firestore:"clientID"`
	LocationID string `firestore:"locationID"`
	Category   string `firestore:"deviceCategory"`
}

// EnrichmentServiceWrapper wraps the processing service for a common interface.
type EnrichmentServiceWrapper struct {
	*microservice.BaseServer
	wrapperContext    context.Context
	serviceCancel     context.CancelFunc
	processingService *messagepipeline.ProcessingService[types.PublishMessage]
	fetcherCleanup    func() error
	logger            zerolog.Logger
}

// NewPublishMessageEnrichmentServiceWrapper creates and configures a new EnrichmentServiceWrapper.
// NewPublishMessageEnrichmentServiceWrapper creates and configures a new EnrichmentServiceWrapper.
func NewPublishMessageEnrichmentServiceWrapper(
	cfg *Config,
	parentContext context.Context,
	logger zerolog.Logger,
) (wrapper *EnrichmentServiceWrapper, err error) { // 1. Named error return

	enrichmentLogger := logger.With().Str("component", "EnrichmentService").Logger()
	serviceCtx, serviceCancel := context.WithCancel(parentContext)

	// 2. A single defer block for cleanup on failure.
	// This function will execute right before NewPublishMessageEnrichmentServiceWrapper returns.
	defer func() {
		if err != nil {
			// If an error occurred at any point, cancel the context and clean up
			// any resources that were successfully created before the failure.
			serviceCancel()
		}
	}()

	if cfg.ServiceDirectorURL != "" {
		var directorClient *servicedirector.Client
		directorClient, err = servicedirector.NewClient(cfg.ServiceDirectorURL, enrichmentLogger)
		if err != nil {
			return nil, fmt.Errorf("failed to create service director client: %w", err)
		}
		err = directorClient.VerifyDataflow(serviceCtx, cfg.DataflowName, cfg.ServiceName)
		if err != nil {
			return nil, fmt.Errorf("resource verification failed via Director: %w", err)
		}
		enrichmentLogger.Info().Msg("Resource verification successful.")
	} else {
		enrichmentLogger.Warn().Msg("ServiceDirectorURL not set, skipping resource verification.")
	}

	// Sequentially create resources. If any fail, the defer block handles cleanup.
	var opts []option.ClientOption
	psClient, err := pubsub.NewClient(serviceCtx, cfg.ProjectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub client: %w", err)
	}
	// On error, defer will call serviceCancel. psClient is closed by canceling the context.

	fsClient, err := firestore.NewClient(serviceCtx, cfg.ProjectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Firestore client: %w", err)
	}
	// On error, defer will call serviceCancel, which closes both fsClient and psClient.

	sourceFetcher, err := cache.NewFirestoreSource[string, DeviceMetadata](fsClient, cfg.CacheConfig.FirestoreConfig, enrichmentLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Firestore source fetcher: %w", err)
	}

	redisCache, err := cache.NewRedisCache[string, DeviceMetadata](serviceCtx, &cfg.CacheConfig.RedisConfig, enrichmentLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create redis cache layer: %w", err)
	}

	fetcher, fetcherCleanup, err := enrichment.NewCacheFallbackFetcher[string, DeviceMetadata](serviceCtx, redisCache, sourceFetcher, enrichmentLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create cache fallback fetcher: %w", err)
	}
	// The new defer block will now also call fetcherCleanup on failure.
	defer func() {
		if err != nil {
			fetcherCleanup()
		}
	}()

	keyExtractor := func(msg types.ConsumedMessage) (string, bool) {
		uid, ok := msg.Attributes["uid"]
		return uid, ok
	}
	enricherFunc := func(msg *types.PublishMessage, data DeviceMetadata) {
		msg.DeviceInfo.Name = data.ClientID
		msg.DeviceInfo.Location = data.LocationID
		msg.DeviceInfo.ServiceTag = data.Category
	}

	transformer := enrichment.NewEnrichmentTransformer[string, DeviceMetadata](fetcher, keyExtractor, enricherFunc, nil, enrichmentLogger)

	consumerCfg := &messagepipeline.GooglePubsubConsumerConfig{
		ProjectID:      cfg.ProjectID,
		SubscriptionID: cfg.Consumer.SubscriptionID,
	}
	consumer, err := messagepipeline.NewGooglePubsubConsumer(consumerCfg, psClient, enrichmentLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	mainProducer, err := messagepipeline.NewGooglePubsubProducer[types.PublishMessage](psClient, cfg.ProducerConfig, enrichmentLogger)
	if err != nil {
		consumer.Stop() // Manually clean up consumer since it was successful
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	processingService, err := messagepipeline.NewProcessingService(cfg.ProcessorConfig.NumWorkers, consumer, mainProducer, transformer, logger)
	if err != nil {
		mainProducer.Stop() // Manually clean up successful resources
		consumer.Stop()
		return nil, fmt.Errorf("failed to create processing service: %w", err)
	}

	baseServer := microservice.NewBaseServer(enrichmentLogger, cfg.HTTPPort)

	// On success, return the fully constructed wrapper. The err is nil, so the defer does nothing.
	return &EnrichmentServiceWrapper{
		BaseServer:        baseServer,
		serviceCancel:     serviceCancel,
		wrapperContext:    serviceCtx,
		processingService: processingService,
		fetcherCleanup:    fetcherCleanup,
		logger:            enrichmentLogger,
	}, nil
}

func (s *EnrichmentServiceWrapper) Start() error {
	s.logger.Info().Msg("Starting enrichment server components...")
	if err := s.processingService.Start(s.wrapperContext); err != nil {
		return fmt.Errorf("failed to start processing service: %w", err)
	}
	s.logger.Info().Msg("Data processing service started.")
	return s.BaseServer.Start()
}

func (s *EnrichmentServiceWrapper) Shutdown() {
	s.logger.Info().Msg("Shutting down enrichment server components...")
	s.serviceCancel()
	s.processingService.Stop()
	s.logger.Info().Msg("Data processing service stopped.")
	s.BaseServer.Shutdown()

	if s.fetcherCleanup != nil {
		if err := s.fetcherCleanup(); err != nil {
			s.logger.Error().Err(err).Msg("Error during fetcher cleanup")
		}
	}
	s.logger.Info().Msg("Enrichment server shut down gracefully.")
}

func (s *EnrichmentServiceWrapper) Mux() *http.ServeMux {
	return s.BaseServer.Mux()
}

func (s *EnrichmentServiceWrapper) GetHTTPPort() string {
	return s.BaseServer.GetHTTPPort()
}
