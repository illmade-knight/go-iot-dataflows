// github.com/illmade-knight/go-iot-dataflows/builder/enrichment/enapp.go
package enrichment

import (
	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"net/http"

	"github.com/illmade-knight/go-cloud-manager/microservice"
	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/illmade-knight/go-dataflow/pkg/enrichment"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/types"

	"github.com/rs/zerolog"
	"google.golang.org/api/option"
)

// DeviceMetadata is the concrete data structure this service fetches for enrichment.
type DeviceMetadata struct {
	ClientID   string `firestore:"clientID"`
	LocationID string `firestore:"locationID"`
	Category   string `firestore:"deviceCategory"`
}

// EnrichmentServiceWrapper wraps the processing service for a common interface.
// The wrapper is now concrete as it's specific to this application's purpose.
type EnrichmentServiceWrapper struct {
	*microservice.BaseServer
	wrapperContext    context.Context
	serviceCancel     context.CancelFunc
	processingService *messagepipeline.ProcessingService[types.PublishMessage]
	fetcherCleanup    func() error
	logger            zerolog.Logger
}

// NewPublishMessageEnrichmentServiceWrapper creates and configures a new EnrichmentServiceWrapper.
// NewPublishMessageEnrichmentServiceWrapper creates the service for production, creating its own clients.
func NewPublishMessageEnrichmentServiceWrapper(
	cfg *Config,
	parentContext context.Context,
	logger zerolog.Logger,
) (*EnrichmentServiceWrapper, error) {
	serviceCtx, serviceCancel := context.WithCancel(parentContext)
	var psClient *pubsub.Client
	var fsClient *firestore.Client
	var err error

	defer func() {
		if err != nil {
			if psClient != nil {
				psClient.Close()
			}
			if fsClient != nil {
				fsClient.Close()
			}
			serviceCancel()
		}
	}()

	var psOpts []option.ClientOption
	if cfg.ClientConnections != nil {
		psOpts = cfg.ClientConnections["pubsub"]
	}
	psClient, err = pubsub.NewClient(serviceCtx, cfg.ProjectID, psOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub client: %w", err)
	}

	var fsOpts []option.ClientOption
	if cfg.ClientConnections != nil {
		fsOpts = cfg.ClientConnections["firestore"]
	}
	fsClient, err = firestore.NewClient(serviceCtx, cfg.ProjectID, fsOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Firestore client: %w", err)
	}

	return NewPublishMessageEnrichmentServiceWrapperWithClients(cfg, parentContext, logger, psClient, fsClient)
}

// NewPublishMessageEnrichmentServiceWrapperWithClients creates the service with injected clients for testability.
func NewPublishMessageEnrichmentServiceWrapperWithClients(
	cfg *Config,
	parentContext context.Context,
	logger zerolog.Logger,
	psClient *pubsub.Client,
	fsClient *firestore.Client,
) (wrapper *EnrichmentServiceWrapper, err error) {
	enrichmentLogger := logger.With().Str("component", "EnrichmentService").Logger()
	serviceCtx, serviceCancel := context.WithCancel(parentContext)

	defer func() {
		if err != nil {
			serviceCancel()
		}
	}()

	if cfg.ServiceDirectorURL != "" {
		// Director verification logic...
	}

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
	defer func() {
		if err != nil && fetcherCleanup != nil {
			fetcherCleanup()
		}
	}()

	keyExtractor := func(msg types.ConsumedMessage) (string, bool) {
		uid, ok := msg.Attributes["uid"]
		return uid, ok
	}

	enricherFunc := func(msg *types.PublishMessage, data DeviceMetadata) {
		if msg.EnrichmentData == nil {
			msg.EnrichmentData = make(map[string]interface{})
		}
		msg.EnrichmentData["name"] = data.ClientID
		msg.EnrichmentData["location"] = data.LocationID
		msg.EnrichmentData["serviceTag"] = data.Category
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
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	processingService, err := messagepipeline.NewProcessingService(cfg.ProcessorConfig.NumWorkers, consumer, mainProducer, transformer, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create processing service: %w", err)
	}

	baseServer := microservice.NewBaseServer(enrichmentLogger, cfg.HTTPPort)

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
