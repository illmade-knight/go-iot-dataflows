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

// EnrichmentServiceWrapper is now a generic wrapper over the enrichment pipeline.
// It is parameterized by the enrichment key type (K) and the enrichment data type (V).
type EnrichmentServiceWrapper[K comparable, V any] struct {
	*microservice.BaseServer
	processingService *messagepipeline.ProcessingService[types.PublishMessage]
	fetcherCleanup    func() error
	logger            zerolog.Logger
}

// NewEnrichmentServiceWrapper is the generic, high-level constructor.
func NewEnrichmentServiceWrapper[K comparable, V any](
	ctx context.Context,
	cfg *Config,
	logger zerolog.Logger,
	keyExtractor enrichment.KeyExtractor[K],
	enricher enrichment.Enricher[V],
) (wrapper *EnrichmentServiceWrapper[K, V], err error) {
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

	return NewEnrichmentServiceWrapperWithClients[K, V](ctx, cfg, logger, psClient, fsClient, keyExtractor, enricher)
}

// NewEnrichmentServiceWrapperWithClients creates the service with injected clients for testability.
func NewEnrichmentServiceWrapperWithClients[K comparable, V any](
	ctx context.Context,
	cfg *Config,
	logger zerolog.Logger,
	psClient *pubsub.Client,
	fsClient *firestore.Client,
	keyExtractor enrichment.KeyExtractor[K],
	enricher enrichment.Enricher[V],
) (wrapper *EnrichmentServiceWrapper[K, V], err error) {
	enrichmentLogger := logger.With().Str("component", "EnrichmentService").Logger()

	var fetcherCleanup func() error
	defer func() {
		if err != nil && fetcherCleanup != nil {
			_ = fetcherCleanup()
		}
	}()

	sourceFetcher, err := cache.NewFirestoreSource[K, V](cfg.CacheConfig.FirestoreConfig, fsClient, enrichmentLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Firestore source fetcher: %w", err)
	}

	fetcher := sourceFetcher.Fetch
	fetcherCleanup = sourceFetcher.Close

	// for production we should have a redis layer
	if cfg.CacheConfig.RedisConfig.Addr != "" {
		redisCache, err := cache.NewRedisCache[K, V](ctx, &cfg.CacheConfig.RedisConfig, enrichmentLogger)
		if err != nil {
			return nil, fmt.Errorf("failed to create redis cache layer: %w", err)
		}

		fetcherCfg := &enrichment.FetcherConfig{CacheWriteTimeout: cfg.CacheConfig.CacheWriteTimeout}
		fetcher, fetcherCleanup, err = enrichment.NewCacheFallbackFetcher[K, V](fetcherCfg, redisCache, sourceFetcher, enrichmentLogger)
		if err != nil {
			return nil, fmt.Errorf("failed to create cache fallback fetcher: %w", err)
		}
	} else {
		logger.Warn().Msg("no redis layer - use only in low volume scenarios")
	}
	transformer := enrichment.NewEnrichmentTransformer[K, V](fetcher, keyExtractor, enricher, nil, enrichmentLogger)

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

	return &EnrichmentServiceWrapper[K, V]{
		BaseServer:        baseServer,
		processingService: processingService,
		fetcherCleanup:    fetcherCleanup,
		logger:            enrichmentLogger,
	}, nil
}

// Start is now a method on the generic wrapper.
func (s *EnrichmentServiceWrapper[K, V]) Start(ctx context.Context) error {
	s.logger.Info().Msg("Starting enrichment server components...")
	if err := s.processingService.Start(ctx); err != nil {
		return fmt.Errorf("failed to start processing service: %w", err)
	}
	s.logger.Info().Msg("Data processing service started.")
	return s.BaseServer.Start()
}

// Shutdown is now a method on the generic wrapper.
func (s *EnrichmentServiceWrapper[K, V]) Shutdown(ctx context.Context) error {
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

func (s *EnrichmentServiceWrapper[K, V]) Mux() *http.ServeMux {
	return s.BaseServer.Mux()
}

func (s *EnrichmentServiceWrapper[K, V]) GetHTTPPort() string {
	return s.BaseServer.GetHTTPPort()
}
