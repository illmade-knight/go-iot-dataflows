// github.com/illmade-knight/go-iot-dataflows/builder/icestore/app.go
package icestore

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/illmade-knight/go-iot-dataflows/builder"   // Import the builder for common interfaces
	"github.com/illmade-knight/go-iot/pkg/icestore"        // Existing icestore package from go-iot
	"github.com/illmade-knight/go-iot/pkg/messagepipeline" // For MetricReporter
	"github.com/rs/zerolog"
	"google.golang.org/api/option"
	"net/http"
	"os"
	"strings"
)

// IceStoreServiceWrapper wraps the IceStorageService for a common interface.
// It implements the builder.Service interface.
type IceStoreServiceWrapper struct {
	*builder.BaseServer                                                           // Embed the base server for common HTTP functionality
	processingService   *messagepipeline.ProcessingService[icestore.ArchivalData] // The core processing logic
	pubsubClient        *pubsub.Client                                            // Pub/Sub client for message consumption
	gcsClient           *storage.Client                                           // GCS client for data archival
	logger              zerolog.Logger
	bucketName          string // The GCS bucket name, needed for create/delete
	projectID           string // The GCP Project ID, needed for bucket creation
}

// NewIceStoreServiceWrapper creates and configures a new IceStoreServiceWrapper instance.
// It initializes all necessary components: GCS client, Pub/Sub client,
// Pub/Sub consumer, GCS batch processor, and the core processing service.
//
// metricReporter: An optional interface to push metrics to an external monitoring system.
// serviceName: The unique name of this service instance (e.g., "icestore-service").
// dataflowName: The name of the dataflow this service is part of (e.g., "my-archive-dataflow").
// These names are crucial for meaningful metric labeling.
func NewIceStoreServiceWrapper(cfg *Config, logger zerolog.Logger,
	serviceName string, dataflowName string, // Add serviceName and dataflowName
) (*IceStoreServiceWrapper, error) {
	ctx := context.Background() // Use a context that can be managed by the main lifecycle

	// Determine GCP client options (credentials file or ADC) for Pub/Sub and GCS.
	var opts []option.ClientOption
	if cfg.CredentialsFile != "" { // General credentials file
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
		logger.Info().Str("credentials_file", cfg.CredentialsFile).Msg("Using specified credentials file for general GCP clients")
	} else {
		logger.Info().Msg("Using Application Default Credentials (ADC) for general GCP clients")
	}

	// Override options specifically for GCS if IceStore has its own credentials file.
	var gcsOpts []option.ClientOption
	gcsOpts = append(gcsOpts, opts...) // Start with general options
	if cfg.IceStore.CredentialsFile != "" {
		gcsOpts = []option.ClientOption{option.WithCredentialsFile(cfg.IceStore.CredentialsFile)}
		logger.Info().Str("credentials_file", cfg.IceStore.CredentialsFile).Msg("Using specified credentials file for GCS client (overriding general)")
	}

	// Handle GCS emulator specific options if the environment variable is set
	if emulatorHost := os.Getenv("STORAGE_EMULATOR_HOST"); emulatorHost != "" {
		gcsOpts = append(gcsOpts, option.WithoutAuthentication(), option.WithEndpoint(emulatorHost))
		logger.Info().Str("emulator_host", emulatorHost).Msg("Configuring GCS client for emulator.")
	}

	// Create the GCS client.
	gcsClient, err := storage.NewClient(ctx, gcsOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	// Create the Pub/Sub client.
	var psOpts []option.ClientOption
	psOpts = append(psOpts, opts...) // Start with general options
	if cfg.Consumer.CredentialsFile != "" {
		psOpts = []option.ClientOption{option.WithCredentialsFile(cfg.Consumer.CredentialsFile)}
		logger.Info().Str("credentials_file", cfg.Consumer.CredentialsFile).Msg("Using specified credentials file for Pub/Sub consumer (overriding general)")
	}

	psClient, err := pubsub.NewClient(ctx, cfg.ProjectID, psOpts...)
	if err != nil {
		gcsClient.Close() // Close GCS client on Pub/Sub client creation failure
		return nil, fmt.Errorf("failed to create pubsub client: %w", err)
	}

	// Create the Pub/Sub consumer.
	consumerCfg := &messagepipeline.GooglePubsubConsumerConfig{
		ProjectID:      cfg.ProjectID,
		SubscriptionID: cfg.Consumer.SubscriptionID,
	}
	consumer, err := messagepipeline.NewGooglePubsubConsumer(consumerCfg, psClient, logger)
	if err != nil {
		gcsClient.Close()
		psClient.Close() // Close clients on consumer creation failure
		return nil, fmt.Errorf("failed to create Pub/Sub consumer: %w", err)
	}

	// Create the GCS batch processor. This component handles writing batches of data to GCS.
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
		gcsClient.Close()
		psClient.Close()
		return nil, fmt.Errorf("failed to create GCS batch processor: %w", err)
	}

	// Assemble the core processing service.
	// Pass the metricReporter and service/dataflow names here for metric labeling.
	processingService, err := icestore.NewIceStorageService(
		cfg.BatchProcessing.NumWorkers,
		consumer,
		batcher,
		icestore.ArchivalTransformer, // Assuming this transformer is appropriate for your archival data
		logger,
	)
	if err != nil {
		gcsClient.Close()
		psClient.Close()
		batcher.Stop() // Ensure batcher is closed on failure
		return nil, fmt.Errorf("failed to create processing service: %w", err)
	}

	// Setup HTTP handler for health checks and embed it in BaseServer.
	mux := http.NewServeMux()
	mux.HandleFunc("/", builder.HealthzHandler)
	mux.HandleFunc("/healthz", builder.HealthzHandler)

	baseServer := builder.NewBaseServer(logger, cfg.HTTPPort, mux)

	return &IceStoreServiceWrapper{
		BaseServer:        baseServer,
		processingService: processingService,
		pubsubClient:      psClient,
		gcsClient:         gcsClient,
		logger:            logger,
		bucketName:        cfg.IceStore.BucketName, // Store for bucket creation/deletion
		projectID:         cfg.ProjectID,           // Store for bucket creation/deletion
	}, nil
}

// Start initiates the IceStore processing service and the embedded HTTP server.
// It also attempts to create the GCS bucket if it doesn't exist.
func (s *IceStoreServiceWrapper) Start() error {
	s.logger.Info().Msg("Starting IceStore server components...")

	// Attempt to create the GCS bucket. In a production scenario, this might be
	// handled by infrastructure-as-code (e.g., ServiceDirector setup).
	// This is defensive for local testing/rapid prototyping.
	s.logger.Info().Str("bucket", s.bucketName).Msg("Attempting to create GCS bucket (if it doesn't exist).")
	if err := s.gcsClient.Bucket(s.bucketName).Create(context.Background(), s.projectID, nil); err != nil {
		// Ignore if bucket already exists error, but log others
		if !strings.Contains(err.Error(), "You already own this bucket") && !strings.Contains(err.Error(), "bucket already exists") {
			s.logger.Warn().Err(err).Str("bucket", s.bucketName).Msg("Failed to create GCS bucket, continuing anyway (might be pre-existing).")
		} else {
			s.logger.Info().Str("bucket", s.bucketName).Msg("GCS bucket already exists.")
		}
	}

	if err := s.processingService.Start(); err != nil {
		return fmt.Errorf("failed to start processing service: %w", err)
	}
	s.logger.Info().Msg("Data processing service started.")
	return s.BaseServer.Start() // Start the HTTP server from the BaseServer
}

// Shutdown gracefully stops the IceStore processing service and the embedded HTTP server.
// It also closes all associated clients.
func (s *IceStoreServiceWrapper) Shutdown() {
	s.logger.Info().Msg("Shutting down IceStore server components...")
	s.processingService.Stop() // Stop the core processing logic
	s.logger.Info().Msg("Data processing service stopped.")
	s.BaseServer.Shutdown() // Shut down the HTTP server from the BaseServer

	// Close clients
	if s.pubsubClient != nil {
		s.pubsubClient.Close()
		s.logger.Info().Msg("Pub/Sub client closed.")
	}
	if s.gcsClient != nil {
		s.gcsClient.Close()
		s.logger.Info().Msg("GCS client closed.")
	}
}

// GetHTTPHandler returns the HTTP handler for the service, inherited from BaseServer.
func (s *IceStoreServiceWrapper) GetHTTPHandler() http.Handler {
	return s.BaseServer.GetHandler()
}

// GetHTTPPort returns the HTTP port the service is listening on, inherited from BaseServer.
func (s *IceStoreServiceWrapper) GetHTTPPort() string {
	return s.BaseServer.GetHTTPPort()
}
