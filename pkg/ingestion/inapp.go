package ingestion

import (
	"context"
	"fmt"
	"net/http"

	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/microservice"
	"github.com/illmade-knight/go-dataflow/pkg/mqttconverter"
)

// IngestionServiceWrapper now wraps the generic ProcessingService.
type IngestionServiceWrapper[T any] struct {
	*microservice.BaseServer
	consumer          *mqttconverter.MqttConsumer
	processingService *messagepipeline.ProcessingService[T]
	logger            zerolog.Logger
}

// NewIngestionServiceWrapper assembles the full ingestion pipeline from standard components.
func NewIngestionServiceWrapper[T any](
	ctx context.Context,
	cfg *Config,
	logger zerolog.Logger,
	transformer messagepipeline.MessageTransformer[T],
) (*IngestionServiceWrapper[T], error) {

	serviceLogger := logger.With().Str("service", "IngestionService").Logger()

	psClient, err := pubsub.NewClient(ctx, cfg.ProjectID, cfg.PubsubOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub client: %w", err)
	}

	// 2. Create the MqttConsumer, injecting the client.
	consumer, err := mqttconverter.NewMqttConsumer(&cfg.MQTT, serviceLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create MQTT consumer: %w", err)
	}

	// 3. Create the GooglePubsubProducer.
	producer, err := messagepipeline.NewGooglePubsubProducer[T](ctx, &cfg.Producer, psClient, serviceLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Google Pub/Sub producer: %w", err)
	}

	// 5. Create the generic ProcessingService.
	processingService, err := messagepipeline.NewProcessingService[T](
		cfg.NumProcessingWorkers,
		consumer,
		producer,
		transformer,
		serviceLogger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create processing service: %w", err)
	}

	baseServer := microservice.NewBaseServer(logger, cfg.HTTPPort)

	serviceWrapper := &IngestionServiceWrapper[T]{
		BaseServer:        baseServer,
		consumer:          consumer,
		processingService: processingService,
		logger:            serviceLogger,
	}

	// NEW: Register the custom handlers for this service.
	serviceWrapper.registerHandlers()

	return serviceWrapper, nil
}

// Start initiates the processing service and the embedded HTTP server.
func (s *IngestionServiceWrapper[T]) Start(ctx context.Context) error {
	s.logger.Info().Msg("Starting ingestion service components...")
	if err := s.processingService.Start(ctx); err != nil {
		s.logger.Error().Err(err).Msg("Failed to start core processing service")
		return err
	}
	return s.BaseServer.Start()
}

// Shutdown gracefully stops the processing service and the HTTP server.
func (s *IngestionServiceWrapper[T]) Shutdown(ctx context.Context) error {
	s.logger.Info().Msg("Shutting down ingestion server components...")
	s.processingService.Stop(ctx)
	s.logger.Info().Msg("Core processing service stopped.")
	return s.BaseServer.Shutdown(ctx)
}

// Mux returns the HTTP ServeMux for the service.
func (s *IngestionServiceWrapper[T]) Mux() *http.ServeMux {
	return s.BaseServer.Mux()
}

// GetHTTPPort returns the HTTP port the service is listening on.
func (s *IngestionServiceWrapper[T]) GetHTTPPort() string {
	return s.BaseServer.GetHTTPPort()
}

// IngestionServiceWrapper register new handler on top of the basic /healthz
func (s *IngestionServiceWrapper[T]) registerHandlers() {
	mux := s.Mux()
	// New readiness check
	mux.HandleFunc("/readyz", s.readinessCheck)
}

func (s *IngestionServiceWrapper[T]) readinessCheck(w http.ResponseWriter, r *http.Request) {
	// This requires exposing the connection status from the consumer.
	// For example, add a method like `IsConnected()` to the consumer interface.
	if s.consumer.IsConnected() { // Assumes this method is added
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("READY"))
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte("NOT READY"))
}
