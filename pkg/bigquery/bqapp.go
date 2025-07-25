// github.com/illmade-knight/go-iot-dataflows/builder/bigquery/app.go
package bigquery

import (
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"net/http"

	"github.com/illmade-knight/go-cloud-manager/microservice"
	"github.com/illmade-knight/go-cloud-manager/microservice/servicedirector"
	"github.com/illmade-knight/go-dataflow/pkg/bqstore"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"

	"github.com/rs/zerolog"
	"google.golang.org/api/option"
)

// BQServiceWrapper wraps your existing ProcessingService for a common interface.
// It implements the builder.Service interface.
type BQServiceWrapper[T any] struct {
	*microservice.BaseServer
	processingService *messagepipeline.ProcessingService[T]
	bqClient          *bigquery.Client
	pubsubClient      *pubsub.Client
	logger            zerolog.Logger
}

// NewBQServiceWrapper creates and configures a new generic BQServiceWrapper instance.
func NewBQServiceWrapper[T any](
	cfg *Config,
	logger zerolog.Logger,
	transformer messagepipeline.MessageTransformer[T],
) (wrapper *BQServiceWrapper[T], err error) { // 1. Named error return

	ctx := context.Background()
	bqLogger := logger.With().Str("component", "BQService").Logger()

	// --- Client variables to be cleaned up by defer on failure ---
	var bqClient *bigquery.Client
	var psClient *pubsub.Client

	// 2. Single defer block for cleanup on initialization failure.
	defer func() {
		if err != nil {
			// If an error occurred, clean up any clients that were successfully created.
			if bqClient != nil {
				bqClient.Close()
			}
			if psClient != nil {
				psClient.Close()
			}
		}
	}()

	if cfg.ServiceDirectorURL != "" {
		var directorClient *servicedirector.Client
		directorClient, err = servicedirector.NewClient(cfg.ServiceDirectorURL, bqLogger)
		if err != nil {
			return nil, fmt.Errorf("failed to create service director client: %w", err)
		}
		if err = directorClient.VerifyDataflow(ctx, cfg.DataflowName, cfg.ServiceName); err != nil {
			return nil, fmt.Errorf("resource verification failed via Director: %w", err)
		}
		bqLogger.Info().Msg("Resource verification successful.")
	} else {
		bqLogger.Warn().Msg("ServiceDirectorURL not set, skipping resource verification.")
	}

	var opts []option.ClientOption
	if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
	}

	// 3. Simplified sequential resource creation.
	bqClient, err = bigquery.NewClient(ctx, cfg.ProjectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
	}

	psClient, err = pubsub.NewClient(ctx, cfg.ProjectID, opts...)
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

	bqInserterCfg := &bqstore.BigQueryDatasetConfig{
		ProjectID: cfg.ProjectID,
		DatasetID: cfg.BigQueryConfig.DatasetID,
		TableID:   cfg.BigQueryConfig.TableID,
	}
	bigQueryInserter, err := bqstore.NewBigQueryInserter[T](ctx, bqClient, bqInserterCfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery inserter: %w", err)
	}

	batcherCfg := &bqstore.BatchInserterConfig{
		BatchSize:    cfg.BatchProcessing.BatchSize,
		FlushTimeout: cfg.BatchProcessing.FlushTimeout,
	}
	batchInserter := bqstore.NewBatcher[T](batcherCfg, bigQueryInserter, logger)

	processingService, err := bqstore.NewBigQueryService[T](
		cfg.BatchProcessing.NumWorkers,
		consumer,
		batchInserter,
		transformer,
		logger,
	)
	if err != nil {
		// No need to clean up consumer/inserter, as they are not returned.
		// The top-level defer will clean up bqClient and psClient.
		return nil, fmt.Errorf("failed to create processing service: %w", err)
	}

	baseServer := microservice.NewBaseServer(logger, cfg.HTTPPort)

	// 4. On success, return the fully constructed wrapper.
	return &BQServiceWrapper[T]{
		BaseServer:        baseServer,
		processingService: processingService,
		bqClient:          bqClient,
		pubsubClient:      psClient,
		logger:            logger,
	}, nil
}

// Start initiates the BQ processing service and the embedded HTTP server.
func (s *BQServiceWrapper[T]) Start() error {
	s.logger.Info().Msg("Starting generic BQ server components...")
	if err := s.processingService.Start(context.Background()); err != nil {
		return fmt.Errorf("failed to start processing service: %w", err)
	}
	s.logger.Info().Msg("Data processing service started.")
	return s.BaseServer.Start()
}

// Shutdown gracefully stops the BQ processing service and the embedded HTTP server.
func (s *BQServiceWrapper[T]) Shutdown() {
	s.logger.Info().Msg("Shutting down generic BQ server components...")
	s.processingService.Stop()
	s.logger.Info().Msg("Data processing service stopped.")
	s.BaseServer.Shutdown()

	if s.bqClient != nil {
		s.bqClient.Close()
		s.logger.Info().Msg("BigQuery client closed.")
	}
	if s.pubsubClient != nil {
		s.pubsubClient.Close()
		s.logger.Info().Msg("Pub/Sub client closed.")
	}
}

// Mux returns the HTTP ServeMux to register additional handlers.
func (s *BQServiceWrapper[T]) Mux() *http.ServeMux {
	return s.BaseServer.Mux()
}

// GetHTTPPort returns the HTTP port the service is listening on.
func (s *BQServiceWrapper[T]) GetHTTPPort() string {
	return s.BaseServer.GetHTTPPort()
}
