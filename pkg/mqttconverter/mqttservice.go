// mqttconverter/mqttservice.go
package mqttconverter

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/types"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/rs/zerolog"
)

type InMessage struct {
	Payload   []byte    `json:"payload"`
	Topic     string    `json:"topic"`
	MessageID string    `json:"message_id"`
	Timestamp time.Time `json:"timestamp"`
	Duplicate bool      `json:"duplicate"`
}

// IngestionServiceConfig holds configuration for the IngestionService.
type IngestionServiceConfig struct {
	InputChanCapacity    int
	NumProcessingWorkers int
}

// DefaultIngestionServiceConfig provides sensible defaults.
func DefaultIngestionServiceConfig() IngestionServiceConfig {
	return IngestionServiceConfig{
		InputChanCapacity:    5000,
		NumProcessingWorkers: 20,
	}
}

// IngestionService processes raw messages from MQTT and hands them off to a generic processor.
type IngestionService[T any] struct {
	processor        messagepipeline.MessageProcessor[T]
	transformer      func(InMessage) (*T, bool, error)
	mqttClientConfig MQTTClientConfig
	pahoClient       mqtt.Client
	config           IngestionServiceConfig
	logger           zerolog.Logger
	MessagesChan     chan InMessage
	ErrorChan        chan error
	cancelCtx        context.Context
	cancelFunc       context.CancelFunc
	wg               sync.WaitGroup
	isShuttingDown   atomic.Bool
}

// NewIngestionService creates and initializes a new IngestionService.
func NewIngestionService[T any](
	processor messagepipeline.MessageProcessor[T],
	transformer func(InMessage) (*T, bool, error),
	logger zerolog.Logger,
	serviceCfg IngestionServiceConfig,
	mqttCfg MQTTClientConfig,
) *IngestionService[T] {

	if serviceCfg.NumProcessingWorkers <= 0 {
		defaultCfg := DefaultIngestionServiceConfig()
		logger.Warn().Int("default_workers", defaultCfg.NumProcessingWorkers).Msg("NumProcessingWorkers was zero or negative, applying default value.")
		serviceCfg.NumProcessingWorkers = defaultCfg.NumProcessingWorkers
	}
	if serviceCfg.InputChanCapacity <= 0 {
		defaultCfg := DefaultIngestionServiceConfig()
		logger.Warn().Int("default_capacity", defaultCfg.InputChanCapacity).Msg("InputChanCapacity was zero or negative, applying default value.")
		serviceCfg.InputChanCapacity = defaultCfg.InputChanCapacity
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &IngestionService[T]{
		config:           serviceCfg,
		mqttClientConfig: mqttCfg,
		processor:        processor,
		transformer:      transformer,
		logger:           logger,
		cancelCtx:        ctx,
		cancelFunc:       cancel,
		MessagesChan:     make(chan InMessage, serviceCfg.InputChanCapacity),
		ErrorChan:        make(chan error, serviceCfg.InputChanCapacity),
	}
}

// processSingleMessage transforms the MQTT message and sends it to the generic processor.
func (s *IngestionService[T]) processSingleMessage(ctx context.Context, msg InMessage, workerID int) {
	transformedPayload, skip, err := s.transformer(msg)
	if err != nil {
		s.logger.Error().Err(err).Str("topic", msg.Topic).Msg("Failed to transform message, skipping.")
		s.sendError(err)
		return
	}

	if skip {
		s.logger.Debug().Str("topic", msg.Topic).Msg("Transformer signaled to skip message.")
		return
	}

	batchedMsg := &types.BatchedMessage[T]{
		OriginalMessage: types.ConsumedMessage{
			PublishMessage: types.PublishMessage{ID: msg.MessageID},
			Attributes:     map[string]string{"mqtt_topic": msg.Topic},
		},
		Payload: transformedPayload,
	}

	select {
	case s.processor.Input() <- batchedMsg:
		s.logger.Debug().Int("worker_id", workerID).Str("topic", msg.Topic).Msg("Successfully handed message to processor")
	case <-ctx.Done():
		s.logger.Warn().Str("topic", msg.Topic).Msg("Shutdown in progress, dropping message.")
	}
}

// Start begins the message processing workers and connects the MQTT client.
func (s *IngestionService[T]) Start() error {
	s.logger.Info().Int("workers", s.config.NumProcessingWorkers).Msg("Starting IngestionService...")

	s.processor.Start(s.cancelCtx)

	for i := 0; i < s.config.NumProcessingWorkers; i++ {
		s.wg.Add(1)
		go func(workerID int) {
			defer s.wg.Done()
			for message := range s.MessagesChan {
				s.processSingleMessage(s.cancelCtx, message, workerID)
			}
		}(i)
	}

	if s.mqttClientConfig.BrokerURL == "" {
		s.logger.Info().Msg("IngestionService started without MQTT client (broker URL is empty).")
		return nil
	}

	if err := s.initAndConnectMQTTClient(); err != nil {
		s.Stop()
		return err
	}

	s.logger.Info().Msg("IngestionService started successfully.")
	return nil
}

// Stop gracefully shuts down the IngestionService.
func (s *IngestionService[T]) Stop() {
	s.logger.Info().Msg("--- Starting Graceful Shutdown ---")
	s.isShuttingDown.Store(true)

	if s.pahoClient != nil && s.pahoClient.IsConnected() {
		s.pahoClient.Unsubscribe(s.mqttClientConfig.Topic)
		s.pahoClient.Disconnect(500)
		s.logger.Info().Msg("Paho MQTT client disconnected.")
	}

	close(s.MessagesChan)
	s.wg.Wait()
	s.cancelFunc()

	if s.processor != nil {
		s.logger.Info().Msg("Stopping processor.")
		s.processor.Stop()
	}

	close(s.ErrorChan)
	s.logger.Info().Msg("IngestionService stopped.")
}

// --- (Other private methods like handleIncomingPahoMessage, initAndConnectMQTTClient, etc. would be here) ---

func (s *IngestionService[T]) handleIncomingPahoMessage(_ mqtt.Client, msg mqtt.Message) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Warn().Interface("panic", r).Msg("Recovered from panic in message handler on shutdown.")
		}
	}()

	if s.isShuttingDown.Load() {
		return
	}

	messagePayload := make([]byte, len(msg.Payload()))
	copy(messagePayload, msg.Payload())

	s.MessagesChan <- InMessage{
		Payload:   messagePayload,
		Topic:     msg.Topic(),
		Duplicate: msg.Duplicate(),
		MessageID: fmt.Sprintf("%d", msg.MessageID()),
		Timestamp: time.Now().UTC(),
	}
}

func (s *IngestionService[T]) sendError(err error) {
	select {
	case s.ErrorChan <- err:
	default:
		s.logger.Warn().Err(err).Msg("ErrorChan is full, dropping error.")
	}
}

func (s *IngestionService[T]) onPahoConnect(client mqtt.Client) {
	s.logger.Info().Str("broker", s.mqttClientConfig.BrokerURL).Msg("Paho client connected to MQTT broker")
	if token := client.Subscribe(s.mqttClientConfig.Topic, 1, s.handleIncomingPahoMessage); token.Wait() && token.Error() != nil {
		s.sendError(fmt.Errorf("failed to subscribe to %s: %w", s.mqttClientConfig.Topic, token.Error()))
	}
}

func (s *IngestionService[T]) onPahoConnectionLost(client mqtt.Client, err error) {
	s.logger.Error().Err(err).Msg("Paho client lost MQTT connection. Auto-reconnect will be attempted.")
}

func newTLSConfig(cfg *MQTTClientConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{InsecureSkipVerify: cfg.InsecureSkipVerify}
	if cfg.CACertFile != "" {
		caCert, err := os.ReadFile(cfg.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig.RootCAs = caCertPool
	}
	if cfg.ClientCertFile != "" && cfg.ClientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client key pair: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	return tlsConfig, nil
}

func (s *IngestionService[T]) initAndConnectMQTTClient() error {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(s.mqttClientConfig.BrokerURL)
	opts.SetClientID(fmt.Sprintf("%s-%d", s.mqttClientConfig.ClientIDPrefix, time.Now().UnixNano()))
	opts.SetUsername(s.mqttClientConfig.Username)
	opts.SetPassword(s.mqttClientConfig.Password)
	opts.SetKeepAlive(s.mqttClientConfig.KeepAlive)
	opts.SetConnectTimeout(s.mqttClientConfig.ConnectTimeout)
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(s.mqttClientConfig.ReconnectWaitMax)
	opts.SetOrderMatters(false)

	if strings.HasPrefix(s.mqttClientConfig.BrokerURL, "tls://") {
		tlsConfig, err := newTLSConfig(&s.mqttClientConfig)
		if err != nil {
			return err
		}
		opts.SetTLSConfig(tlsConfig)
	}

	opts.SetOnConnectHandler(s.onPahoConnect)
	opts.SetConnectionLostHandler(s.onPahoConnectionLost)

	s.pahoClient = mqtt.NewClient(opts)
	if token := s.pahoClient.Connect(); token.WaitTimeout(s.mqttClientConfig.ConnectTimeout) && token.Error() != nil {
		return fmt.Errorf("paho MQTT client connect error: %w", token.Error())
	}

	return nil
}
