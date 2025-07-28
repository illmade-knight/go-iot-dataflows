package icestore

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/illmade-knight/go-dataflow/pkg/icestore"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/microservice"
	"github.com/illmade-knight/go-dataflow/pkg/types"
	"github.com/rs/zerolog"
)

// IceStoreServiceWrapper wraps the IceStorageService for a common interface.
type IceStoreServiceWrapper struct {
	*microservice.BaseServer
	processingService *messagepipeline.ProcessingService[icestore.ArchivalData]
	pubsubClient      *pubsub.Client
	gcsClient         *storage.Client
	logger            zerolog.Logger
	bucketName        string
	projectID         string
}

// NewIceStoreServiceWrapper creates and configures a new IceStoreServiceWrapper instance.
// It uses a deferred function with a named error return to ensure resources are cleaned up on failure.
func NewIceStoreServiceWrapper(
	ctx context.Context,
	cfg *Config,
	logger zerolog.Logger,
) (wrapper *IceStoreServiceWrapper, err error) {
	isLogger := logger.With().Str("component", "IceStore").Logger()

	var gcsClient *storage.Client
	var psClient *pubsub.Client

	// This deferred function will execute if any part of the constructor fails and returns an error.
	defer func() {
		if err != nil {
			isLogger.Error().Err(err).Msg("Failed to initialize IceStoreServiceWrapper, cleaning up resources.")
			if gcsClient != nil {
				if closeErr := gcsClient.Close(); closeErr != nil {
					isLogger.Error().Err(closeErr).Msg("Failed to close GCS client during error cleanup.")
				}
			}
			if psClient != nil {
				if closeErr := psClient.Close(); closeErr != nil {
					isLogger.Error().Err(closeErr).Msg("Failed to close Pub/Sub client during error cleanup.")
				}
			}
		}
	}()

	gcsClient, err = storage.NewClient(ctx, cfg.GCSOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	psClient, err = pubsub.NewClient(ctx, cfg.ProjectID, cfg.PubsubOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub client: %w", err)
	}

	consumerCfg := messagepipeline.NewGooglePubsubConsumerDefaults(cfg.ProjectID)
	consumerCfg.SubscriptionID = cfg.Consumer.SubscriptionID
	consumer, err := messagepipeline.NewGooglePubsubConsumer(ctx, consumerCfg, psClient, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub consumer: %w", err)
	}

	batcher, err := icestore.NewGCSBatchProcessor(
		icestore.NewGCSClientAdapter(gcsClient),
		&icestore.BatcherConfig{
			BatchSize:     cfg.BatchProcessing.BatchSize,
			FlushInterval: cfg.BatchProcessing.FlushInterval,
			UploadTimeout: cfg.BatchProcessing.UploadTimeout,
		},
		icestore.GCSBatchUploaderConfig{
			BucketName:   cfg.IceStore.BucketName,
			ObjectPrefix: cfg.IceStore.ObjectPrefix,
		},
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS batch processor: %w", err)
	}

	processingService, err := icestore.NewIceStorageService(
		cfg.BatchProcessing.NumWorkers,
		consumer,
		batcher,
		// The transformer now requires a context, which we provide.
		func(ctx context.Context, msg types.ConsumedMessage) (*icestore.ArchivalData, bool, error) {
			return icestore.ArchivalTransformer(ctx, msg)
		},
		logger,
	)
	if err != nil {
		// Stop the batcher if the final service creation fails.
		_ = batcher.Stop(context.Background())
		return nil, fmt.Errorf("failed to create processing service: %w", err)
	}

	baseServer := microservice.NewBaseServer(logger, cfg.HTTPPort)
	return &IceStoreServiceWrapper{
		BaseServer:        baseServer,
		processingService: processingService,
		pubsubClient:      psClient,
		gcsClient:         gcsClient,
		logger:            isLogger,
		bucketName:        cfg.IceStore.BucketName,
		projectID:         cfg.ProjectID,
	}, nil
}

// Start initiates the IceStore processing service and the embedded HTTP server.
func (s *IceStoreServiceWrapper) Start(ctx context.Context) error {
	s.logger.Info().Msg("Starting IceStore server components...")

	createCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	s.logger.Info().Str("bucket", s.bucketName).Msg("Ensuring GCS bucket exists.")
	if err := s.gcsClient.Bucket(s.bucketName).Create(createCtx, s.projectID, nil); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			s.logger.Warn().Err(err).Str("bucket", s.bucketName).Msg("Could not create/verify GCS bucket; please ensure it exists.")
		}
	}

	if err := s.processingService.Start(ctx); err != nil {
		return fmt.Errorf("failed to start processing service: %w", err)
	}
	s.logger.Info().Msg("Data processing service started.")
	return s.BaseServer.Start()
}

// Shutdown gracefully stops the IceStore processing service and its components.
func (s *IceStoreServiceWrapper) Shutdown(ctx context.Context) error {
	s.logger.Info().Msg("Shutting down IceStore server components...")
	s.processingService.Stop(ctx)
	s.logger.Info().Msg("Data processing service stopped.")

	// Close the clients this wrapper is responsible for.
	if s.pubsubClient != nil {
		if err := s.pubsubClient.Close(); err != nil {
			s.logger.Error().Err(err).Msg("Error closing Pub/Sub client.")
		}
	}
	if s.gcsClient != nil {
		if err := s.gcsClient.Close(); err != nil {
			s.logger.Error().Err(err).Msg("Error closing GCS client.")
		}
	}

	return s.BaseServer.Shutdown(ctx)
}

// Mux returns the HTTP ServeMux for the service.
func (s *IceStoreServiceWrapper) Mux() *http.ServeMux {
	return s.BaseServer.Mux()
}

// GetHTTPPort returns the HTTP port the service is listening on.
func (s *IceStoreServiceWrapper) GetHTTPPort() string {
	return s.BaseServer.GetHTTPPort()
}
