// builder/ingestion/inapp.go
package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog"

	"github.com/illmade-knight/go-cloud-manager/microservice"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-iot-dataflows/pkg/mqttconverter"
)

// RawMessage defines the canonical structure for messages published by this service.
type RawMessage struct {
	Topic     string          `json:"topic"`
	Payload   json.RawMessage `json:"payload"`
	Timestamp string          `json:"timestamp"`
}

// IngestionServiceWrapper wraps the core MQTT ingestion logic.
type IngestionServiceWrapper struct {
	*microservice.BaseServer
	ingestionService *mqttconverter.IngestionService[RawMessage] // Now generic
	logger           zerolog.Logger
}

// NewIngestionServiceWrapper creates and configures the full ingestion service.
func NewIngestionServiceWrapper(
	cfg *Config,
	logger zerolog.Logger,
) (wrapper *IngestionServiceWrapper, err error) {

	serviceContext, serviceCancel := context.WithCancel(context.Background())
	defer serviceCancel()

	ingestionLogger := logger.With().Str("component", "IngestionService").Logger()

	var psClient *pubsub.Client

	defer func() {
		if err != nil {
			if psClient != nil {
				psClient.Close()
			}
			serviceCancel()
		}
	}()

	psClient, err = pubsub.NewClient(serviceContext, cfg.ProjectID, cfg.PubsubOptions...)
	if err != nil {
		return nil, err
	}
	// 1. Create the producer from the generic messagepipeline package
	producer, err := messagepipeline.NewGooglePubsubProducer[RawMessage](psClient, &cfg.Producer, ingestionLogger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Google Pub/Sub producer: %w", err)
	}

	// 2. Define the transformer to convert MQTT messages to our RawMessage format
	transformer := func(msg mqttconverter.InMessage) (*RawMessage, bool, error) {
		if msg.Duplicate {
			return nil, true, nil // Skip duplicates
		}
		transformed := &RawMessage{
			Topic:     msg.Topic,
			Payload:   msg.Payload, // The payload is already raw bytes
			Timestamp: msg.Timestamp.Format(time.RFC3339Nano),
		}
		return transformed, false, nil
	}

	// 3. Create the ingestion service, now passing the generic producer and transformer
	ingestionService := mqttconverter.NewIngestionService[RawMessage](
		producer,
		transformer,
		ingestionLogger,
		cfg.Service,
		cfg.MQTT,
	)

	baseServer := microservice.NewBaseServer(ingestionLogger, cfg.HTTPPort)

	return &IngestionServiceWrapper{
		BaseServer:       baseServer,
		ingestionService: ingestionService,
		logger:           ingestionLogger,
	}, nil
}

// Start initiates the MQTT ingestion service and the embedded HTTP server.
func (s *IngestionServiceWrapper) Start() error {
	s.logger.Info().Msg("Starting MQTT ingestion server components...")
	if err := s.ingestionService.Start(); err != nil {
		s.logger.Error().Err(err).Msg("failed to start core ingestion service")
		return err
	}
	return s.BaseServer.Start()
}

// Shutdown gracefully stops the MQTT ingestion service.
func (s *IngestionServiceWrapper) Shutdown() {
	s.logger.Info().Msg("Shutting down MQTT ingestion server components...")
	s.ingestionService.Stop()
	s.logger.Info().Msg("Core ingestion service stopped.")
	s.BaseServer.Shutdown()
}

// Mux returns the HTTP ServeMux for the service.
func (s *IngestionServiceWrapper) Mux() *http.ServeMux {
	return s.BaseServer.Mux()
}

// GetHTTPPort returns the HTTP port the service is listening on.
func (s *IngestionServiceWrapper) GetHTTPPort() string {
	return s.BaseServer.GetHTTPPort()
}
