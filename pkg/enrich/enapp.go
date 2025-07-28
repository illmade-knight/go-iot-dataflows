// github.com/illmade-knight/go-iot-dataflows/builder/enrichment/enapp.go
package enrich

import (
	"context"
	"fmt"
	"net/http"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub"
	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/illmade-knight/go-dataflow/pkg/enrichment"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/microservice"
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
type EnrichmentServiceWrapper struct {
	*microservice.BaseServer
	processingService *messagepipeline.ProcessingService[types.PublishMessage]
	fetcherCleanup    func() error
	logger            zerolog.Logger
}

// NewEnrichmentServiceWrapper creates and configures a new EnrichmentServiceWrapper.
func NewEnrichmentServiceWrapper(
	ctx context.Context,
	cfg *Config,
	logger zerolog.Logger,
) (wrapper *EnrichmentServiceWrapper, err error) {
	var psClient *pubsub.Client
	var fsClient *firestore.Client

	defer func() {
		if err != nil {
			if psClient != nil {
				_ = psClient.Close()
			}
			if fsClient != nil {
				_ = fsClient.Close()
			}
		}
	}()

	var psOpts []option.ClientOption
	if cfg.ClientConnections != nil {
		psOpts = cfg.ClientConnections["pubsub"]
	}
	psClient, err = pubsub.NewClient(ctx, cfg.ProjectID, psOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub client: %w", err)
	}

	var fsOpts []option.ClientOption
	if cfg.ClientConnections != nil {
		fsOpts = cfg.ClientConnections["firestore"]
	}
	fsClient, err = firestore.NewClient(ctx, cfg.ProjectID, fsOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Firestore client: %w", err)
	}

	return NewEnrichmentServiceWrapperWithClients(ctx, cfg, logger, psClient, fsClient)
}

// NewEnrichmentServiceWrapperWithClients creates the service with injected clients for testability.
func NewEnrichmentServiceWrapperWithClients(
	ctx context.Context,
	cfg *Config,
	logger zerolog.Logger,
	psClient *pubsub.Client,
	fsClient *firestore.Client,
) (wrapper *EnrichmentServiceWrapper, err error) {
	enrichmentLogger := logger.With().Str("component", "EnrichmentService").Logger()

	var fetcherCleanup func() error
	defer func() {
		if err != nil && fetcherCleanup != nil {
			_ = fetcherCleanup()
		}
	}()

	sourceFetcher, err := cache.NewFirestoreSource[string, DeviceMetadata](cfg.CacheConfig.FirestoreConfig, fsClient, enrichmentLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Firestore source fetcher: %w", err)
	}

	redisCache, err := cache.NewRedisCache[string, DeviceMetadata](ctx, &cfg.CacheConfig.RedisConfig, enrichmentLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create redis cache layer: %w", err)
	}

	fetcherCfg := &enrichment.FetcherConfig{CacheWriteTimeout: cfg.CacheConfig.CacheWriteTimeout}
	fetcher, fetcherCleanup, err := enrichment.NewCacheFallbackFetcher[string, DeviceMetadata](fetcherCfg, redisCache, sourceFetcher, enrichmentLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create cache fallback fetcher: %w", err)
	}

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

	consumerCfg := messagepipeline.NewGooglePubsubConsumerDefaults(cfg.ProjectID)
	consumerCfg.SubscriptionID = cfg.Consumer.SubscriptionID
	consumer, err := messagepipeline.NewGooglePubsubConsumer(ctx, consumerCfg, psClient, enrichmentLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	mainProducer, err := messagepipeline.NewGooglePubsubProducer[types.PublishMessage](ctx, cfg.ProducerConfig, psClient, enrichmentLogger)
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
		processingService: processingService,
		fetcherCleanup:    fetcherCleanup,
		logger:            enrichmentLogger,
	}, nil
}

// Start initiates the processing service and the embedded HTTP server.
func (s *EnrichmentServiceWrapper) Start(ctx context.Context) error {
	s.logger.Info().Msg("Starting enrichment server components...")
	if err := s.processingService.Start(ctx); err != nil {
		return fmt.Errorf("failed to start processing service: %w", err)
	}
	s.logger.Info().Msg("Data processing service started.")
	return s.BaseServer.Start()
}

// Shutdown gracefully stops the processing service and its components.
func (s *EnrichmentServiceWrapper) Shutdown(ctx context.Context) error {
	s.logger.Info().Msg("Shutting down enrichment server components...")
	s.processingService.Stop(ctx)
	s.logger.Info().Msg("Data processing service stopped.")

	if s.fetcherCleanup != nil {
		if err := s.fetcherCleanup(); err != nil {
			s.logger.Error().Err(err).Msg("Error during fetcher cleanup")
		}
	}
	s.logger.Info().Msg("Enrichment server shut down gracefully.")
	return s.BaseServer.Shutdown(ctx)
}

func (s *EnrichmentServiceWrapper) Mux() *http.ServeMux {
	return s.BaseServer.Mux()
}

func (s *EnrichmentServiceWrapper) GetHTTPPort() string {
	return s.BaseServer.GetHTTPPort()
}
