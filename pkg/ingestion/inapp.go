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
	"github.com/illmade-knight/go-dataflow/pkg/types"
)

// IngestionServiceWrapper now wraps the generic ProcessingService.
type IngestionServiceWrapper struct {
	*microservice.BaseServer
	processingService *messagepipeline.ProcessingService[mqttconverter.RawMessage]
	logger            zerolog.Logger
}

// NewIngestionServiceWrapper assembles the full ingestion pipeline from standard components.
func NewIngestionServiceWrapper(
	ctx context.Context,
	cfg *Config,
	logger zerolog.Logger,
) (*IngestionServiceWrapper, error) {

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
	producer, err := messagepipeline.NewGooglePubsubProducer[mqttconverter.RawMessage](ctx, &cfg.Producer, psClient, serviceLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Google Pub/Sub producer: %w", err)
	}

	// 4. Define the transformer logic.
	transformer := func(ctx context.Context, msg types.ConsumedMessage) (*mqttconverter.RawMessage, bool, error) {
		transformed := &mqttconverter.RawMessage{
			Topic:     msg.Attributes["mqtt_topic"],
			Payload:   msg.Payload,
			Timestamp: msg.PublishTime,
		}
		return transformed, false, nil
	}

	// 5. Create the generic ProcessingService.
	processingService, err := messagepipeline.NewProcessingService[mqttconverter.RawMessage](
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
	return &IngestionServiceWrapper{
		BaseServer:        baseServer,
		processingService: processingService,
		logger:            serviceLogger,
	}, nil
}

// Start initiates the processing service and the embedded HTTP server.
func (s *IngestionServiceWrapper) Start(ctx context.Context) error {
	s.logger.Info().Msg("Starting ingestion service components...")
	if err := s.processingService.Start(ctx); err != nil {
		s.logger.Error().Err(err).Msg("Failed to start core processing service")
		return err
	}
	return s.BaseServer.Start()
}

// Shutdown gracefully stops the processing service and the HTTP server.
func (s *IngestionServiceWrapper) Shutdown(ctx context.Context) error {
	s.logger.Info().Msg("Shutting down ingestion server components...")
	s.processingService.Stop(ctx)
	s.logger.Info().Msg("Core processing service stopped.")
	return s.BaseServer.Shutdown(ctx)
}

// Mux returns the HTTP ServeMux for the service.
func (s *IngestionServiceWrapper) Mux() *http.ServeMux {
	return s.BaseServer.Mux()
}

// GetHTTPPort returns the HTTP port the service is listening on.
func (s *IngestionServiceWrapper) GetHTTPPort() string {
	return s.BaseServer.GetHTTPPort()
}
