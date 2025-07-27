package icestore

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/illmade-knight/go-cloud-manager/microservice"
	"github.com/illmade-knight/go-dataflow/pkg/icestore"        // Existing icestore package from go-iot
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline" // For MetricReporter
	"os"
	"strings"

	"github.com/rs/zerolog"
	"google.golang.org/api/option"
)

// IceStoreServiceWrapper wraps the IceStorageService for a common interface.
// It implements the builder.Service interface.
type IceStoreServiceWrapper struct {
	*microservice.BaseServer                                                           // Embed the base server for common HTTP functionality
	processingService        *messagepipeline.ProcessingService[icestore.ArchivalData] // The core processing logic
	pubsubClient             *pubsub.Client                                            // Pub/Sub client for message consumption
	gcsClient                *storage.Client                                           // GCS client for data archival
	logger                   zerolog.Logger
	bucketName               string // The GCS bucket name, needed for create/delete
	projectID                string // The GCP Project ID, needed for bucket creation
}

// NewIceStoreServiceWrapper creates and configures a new IceStoreServiceWrapper instance.
func NewIceStoreServiceWrapper(
	cfg *Config,
	logger zerolog.Logger,
) (wrapper *IceStoreServiceWrapper, err error) { // 1. Named error return
	serviceCtx, serviceCancel := context.WithCancel(context.Background())
	isLogger := logger.With().Str("component", "IceStore").Logger()

	// --- Client variables to be cleaned up by defer on failure ---
	var gcsClient *storage.Client
	var psClient *pubsub.Client

	// 2. Single defer block for cleanup on initialization failure.
	defer func() {
		if err != nil {
			// If an error occurred, clean up any clients that were successfully created.
			if gcsClient != nil {
				_ = gcsClient.Close()
			}
			if psClient != nil {
				_ = psClient.Close()
			}
			serviceCancel()
		}
	}()

	var generalOpts []option.ClientOption
	if cfg.CredentialsFile != "" {
		generalOpts = append(generalOpts, option.WithCredentialsFile(cfg.CredentialsFile))
	}

	gcsOpts := append([]option.ClientOption{}, generalOpts...)
	if cfg.IceStore.CredentialsFile != "" {
		gcsOpts = []option.ClientOption{option.WithCredentialsFile(cfg.IceStore.CredentialsFile)}
	}

	if emulatorHost := os.Getenv("STORAGE_EMULATOR_HOST"); emulatorHost != "" {
		gcsOpts = append(gcsOpts, option.WithoutAuthentication(), option.WithEndpoint(emulatorHost))
	}

	gcsClient, err = storage.NewClient(serviceCtx, gcsOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	psOpts := append([]option.ClientOption{}, generalOpts...)
	if cfg.Consumer.CredentialsFile != "" {
		psOpts = []option.ClientOption{option.WithCredentialsFile(cfg.Consumer.CredentialsFile)}
	}

	psClient, err = pubsub.NewClient(serviceCtx, cfg.ProjectID, psOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub client: %w", err)
	}

	consumerCfg := &messagepipeline.GooglePubsubConsumerConfig{
		ProjectID:      cfg.ProjectID,
		SubscriptionID: cfg.Consumer.SubscriptionID,
	}
	consumer, err := messagepipeline.NewGooglePubsubConsumer(consumerCfg, psClient, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub consumer: %w", err)
	}

	batcher, err := icestore.NewGCSBatchProcessor(
		icestore.NewGCSClientAdapter(gcsClient),
		&icestore.BatcherConfig{
			BatchSize:    cfg.BatchProcessing.BatchSize,
			FlushTimeout: cfg.BatchProcessing.FlushTimeout,
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
		icestore.ArchivalTransformer,
		logger,
	)
	if err != nil {
		batcher.Stop()
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
// It also attempts to create the GCS bucket if it doesn't exist.
func (s *IceStoreServiceWrapper) Start() error {
	s.logger.Info().Msg("Starting IceStore server components...")

	// Attempt to create the GCS bucket. In production, this should be handled
	// by IaC (e.g., via ServiceDirector). This is for local/dev convenience.
	s.logger.Info().Str("bucket", s.bucketName).Msg("Ensuring GCS bucket exists.")
	if err := s.gcsClient.Bucket(s.bucketName).Create(context.Background(), s.projectID, nil); err != nil {
		// Ignore "already exists" errors, but log others as a warning.
		if !strings.Contains(err.Error(), "You already own this bucket") && !strings.Contains(err.Error(), "bucket already exists") {
			s.logger.Warn().Err(err).Str("bucket", s.bucketName).Msg("Failed to create GCS bucket; please ensure it exists.")
		} else {
			s.logger.Info().Str("bucket", s.bucketName).Msg("GCS bucket already exists.")
		}
	}

	if err := s.processingService.Start(context.Background()); err != nil {
		return fmt.Errorf("failed to start processing service: %w", err)
	}
	s.logger.Info().Msg("Data processing service started.")
	return s.BaseServer.Start()
}

// Shutdown gracefully stops the IceStore processing service and the embedded HTTP server.
func (s *IceStoreServiceWrapper) Shutdown() {
	s.logger.Info().Msg("Shutting down IceStore server components...")
	s.processingService.Stop()
	s.logger.Info().Msg("Data processing service stopped.")
	s.BaseServer.Shutdown()

	if s.pubsubClient != nil {
		if err := s.pubsubClient.Close(); err != nil {
			s.logger.Error().Err(err).Msg("Error closing Pub/Sub client.")
		}
		s.logger.Info().Msg("Pub/Sub client closed.")
	}
	if s.gcsClient != nil {
		if err := s.gcsClient.Close(); err != nil {
			s.logger.Error().Err(err).Msg("Error closing GCS client.")
		}
		s.logger.Info().Msg("GCS client closed.")
	}
}

// GetHTTPPort returns the HTTP port the service is listening on.
func (s *IceStoreServiceWrapper) GetHTTPPort() string {
	return s.BaseServer.GetHTTPPort()
}
