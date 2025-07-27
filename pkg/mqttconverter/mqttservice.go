package mqttconverter

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
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
// It is now generic to support any transformed data type.
type IngestionService[T any] struct {
	// --- MINIMAL CHANGES START HERE ---
	processor   messagepipeline.MessageProcessor[T]
	transformer func(InMessage) (*T, bool, error)
	// --- MINIMAL CHANGES END HERE ---

	mqttClientConfig      MQTTClientConfig
	pahoClient            mqtt.Client
	config                IngestionServiceConfig
	logger                zerolog.Logger
	MessagesChan          chan InMessage
	ErrorChan             chan error
	cancelCtx             context.Context
	cancelFunc            context.CancelFunc
	wg                    sync.WaitGroup
	closeErrorChanOnce    sync.Once
	closeMessagesChanOnce sync.Once
	isShuttingDown        atomic.Bool
}

// NewIngestionService creates and initializes a new generic IngestionService.
func NewIngestionService[T any](
	processor messagepipeline.MessageProcessor[T],
	transformer func(InMessage) (*T, bool, error),
	logger zerolog.Logger,
	serviceCfg IngestionServiceConfig,
	mqttCfg MQTTClientConfig,
) *IngestionService[T] {

	if serviceCfg.NumProcessingWorkers <= 0 {
		defaultCfg := DefaultIngestionServiceConfig()
		logger.Warn().
			Int("provided_workers", serviceCfg.NumProcessingWorkers).
			Int("default_workers", defaultCfg.NumProcessingWorkers).
			Msg("NumProcessingWorkers was zero or negative, applying default value.")
		serviceCfg.NumProcessingWorkers = defaultCfg.NumProcessingWorkers
	}
	if serviceCfg.InputChanCapacity <= 0 {
		defaultCfg := DefaultIngestionServiceConfig()
		logger.Warn().
			Int("provided_capacity", serviceCfg.InputChanCapacity).
			Int("default_capacity", defaultCfg.InputChanCapacity).
			Msg("InputChanCapacity was zero or negative, applying default value.")
		serviceCfg.InputChanCapacity = defaultCfg.InputChanCapacity
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &IngestionService[T]{
		config:           serviceCfg,
		mqttClientConfig: mqttCfg,
		// --- MINIMAL CHANGES START HERE ---
		processor:   processor,
		transformer: transformer,
		// --- MINIMAL CHANGES END HERE ---
		logger:       logger,
		cancelCtx:    ctx,
		cancelFunc:   cancel,
		MessagesChan: make(chan InMessage, serviceCfg.InputChanCapacity),
		ErrorChan:    make(chan error, serviceCfg.InputChanCapacity),
	}
}

// processSingleMessage is updated to use the transformer and processor.
func (s *IngestionService[T]) processSingleMessage(ctx context.Context, msg InMessage, workerID int) {
	s.logger.Debug().Int("worker_id", workerID).Msg("Received message for processing")

	// --- MINIMAL CHANGES START HERE ---
	// 1. Transform the incoming message into the target type T.
	transformedPayload, skip, err := s.transformer(msg)
	if err != nil {
		s.logger.Error().
			Int("worker_id", workerID).
			Err(err).
			Str("topic", msg.Topic).
			Msg("Failed to transform message; dropping it.")
		s.sendError(err)
		return // Drop the message if transformation fails.
	}

	if skip {
		s.logger.Debug().Int("worker_id", workerID).Str("topic", msg.Topic).Msg("Transformer signaled to skip message.")
		return
	}

	// 2. Wrap the transformed payload in the standard pipeline message format.
	batchedMsg := &types.BatchedMessage[T]{
		OriginalMessage: types.ConsumedMessage{
			PublishMessage: types.PublishMessage{ID: msg.MessageID},
			Attributes:     map[string]string{"mqtt_topic": msg.Topic},
		},
		Payload: transformedPayload,
	}

	// 3. Send the structured message to the next stage in the pipeline.
	select {
	case s.processor.Input() <- batchedMsg:
		s.logger.Debug().Int("worker_id", workerID).Str("topic", msg.Topic).Msg("Successfully handed message to processor")
	case <-ctx.Done():
		s.logger.Warn().Str("topic", msg.Topic).Msg("Shutdown in progress, dropping transformed message.")
	}
	// --- MINIMAL CHANGES END HERE ---
}

// Stop is updated to stop the processor instead of the old publisher.
func (s *IngestionService[T]) Stop() {
	s.logger.Info().Msg("--- Starting Graceful Shutdown ---")

	s.isShuttingDown.Store(true)
	s.logger.Info().Msg("Step 1: Shutdown signaled. No new messages will be queued by the handler.")

	if s.pahoClient != nil && s.pahoClient.IsConnected() {
		topic := s.mqttClientConfig.Topic
		s.logger.Info().Str("topic", topic).Msg("Step 2a: Unsubscribing from MQTT topic.")
		if token := s.pahoClient.Unsubscribe(topic); token.WaitTimeout(2*time.Second) && token.Error() != nil {
			s.logger.Warn().Err(token.Error()).Msg("Failed to unsubscribe during shutdown.")
		}
		s.pahoClient.Disconnect(500)
		s.logger.Info().Msg("Step 2b: Paho MQTT client disconnected.")
	}

	s.logger.Info().Msg("Step 3: Closing message channel. Workers will now drain the buffer.")
	s.closeMessagesChanOnce.Do(func() {
		close(s.MessagesChan)
	})

	s.logger.Info().Msg("Step 4: Waiting for worker goroutines to finish draining...")
	s.wg.Wait()
	s.logger.Info().Msg("All worker goroutines have completed.")

	s.logger.Info().Msg("Step 5: Cancelling context.")
	s.cancelFunc()

	// --- MINIMAL CHANGE START HERE ---
	if s.processor != nil {
		s.logger.Info().Msg("Step 6: Stopping processor.")
		s.processor.Stop()
		s.logger.Info().Msg("Processor stopped.")
	}
	// --- MINIMAL CHANGE END HERE ---

	if s.ErrorChan != nil {
		s.closeErrorChanOnce.Do(func() {
			close(s.ErrorChan)
			s.logger.Info().Msg("ErrorChan closed.")
		})
	}
	s.logger.Info().Msg("IngestionService stopped.")
}

// --- ALL THE FOLLOWING CODE IS EXACTLY THE SAME AS YOUR WORKING VERSION ---

func (s *IngestionService[T]) Err() <-chan error {
	return s.ErrorChan
}

func (s *IngestionService[T]) handleIncomingPahoMessage(_ mqtt.Client, msg mqtt.Message) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Warn().
				Interface("panic", r).
				Str("topic", msg.Topic()).
				Msg("Recovered from panic in message handler. This is expected during a race condition on shutdown.")
		}
	}()
	if s.isShuttingDown.Load() {
		s.logger.Warn().Str("topic", msg.Topic()).Msg("Shutdown in progress, Paho message dropped.")
		return
	}
	s.logger.Info().Interface("msg", msg).Bool("ts", msg.Duplicate()).Msg("received paho message")
	messagePayload := make([]byte, len(msg.Payload()))
	copy(messagePayload, msg.Payload())
	message := InMessage{
		Payload:   messagePayload,
		Topic:     msg.Topic(),
		Duplicate: msg.Duplicate(),
		MessageID: fmt.Sprintf("%d", msg.MessageID()),
		Timestamp: time.Now().UTC(),
	}
	s.MessagesChan <- message
	s.logger.Debug().Str("topic", msg.Topic()).Msg("Message pushed to MessagesChan")
}

func (s *IngestionService[T]) sendError(err error) {
	if s.ErrorChan == nil {
		return
	}
	select {
	case s.ErrorChan <- err:
	default:
		s.logger.Warn().Err(err).Msg("ErrorChan is full, dropping error")
	}
}

func (s *IngestionService[T]) Start() error {
	s.logger.Info().
		Int("workers", s.config.NumProcessingWorkers).
		Int("channel_capacity", s.config.InputChanCapacity).
		Msg("Starting IngestionService...")

	s.processor.Start(s.cancelCtx)

	for i := 0; i < s.config.NumProcessingWorkers; i++ {
		s.wg.Add(1)
		go func(workerID int) {
			defer s.wg.Done()
			s.logger.Info().Int("worker_id", workerID).Msg("Starting processing worker")
			for message := range s.MessagesChan {
				s.processSingleMessage(s.cancelCtx, message, workerID)
			}
			s.logger.Info().Int("worker_id", workerID).Msg("MessagesChan closed, worker stopping")
		}(i)
	}
	if s.mqttClientConfig.BrokerURL == "" {
		s.logger.Info().Msg("IngestionService started without MQTT client (broker URL is empty).")
		return nil
	}
	if s.mqttClientConfig.KeepAlive == 0 {
		s.mqttClientConfig.KeepAlive = 10 * time.Second
		s.logger.Warn().Msg("mqtt config had a zero KeepAlive value - setting to 10 * time.Second")
	}
	if s.mqttClientConfig.ConnectTimeout == 0 {
		s.mqttClientConfig.ConnectTimeout = 5 * time.Second
		s.logger.Warn().Msg("mqtt config had a zero ConnectTimeout value - setting to 5 * time.Second")
	}
	if err := s.initAndConnectMQTTClient(); err != nil {
		s.logger.Error().Err(err).Msg("Failed to initialize or connect MQTT client during Start.")
		s.Stop()
		return err
	}
	s.logger.Info().Msg("IngestionService started successfully.")
	return nil
}

func (s *IngestionService[T]) onPahoConnect(client mqtt.Client) {
	s.logger.Info().Str("broker", s.mqttClientConfig.BrokerURL).Msg("Paho client connected to MQTT broker")
	topic := s.mqttClientConfig.Topic
	qos := byte(1)
	s.logger.Info().Str("topic", topic).Msg("Subscribing to MQTT topic")
	if token := client.Subscribe(topic, qos, s.handleIncomingPahoMessage); token.Wait() && token.Error() != nil {
		s.logger.Error().Err(token.Error()).Str("topic", topic).Msg("Failed to subscribe to MQTT topic")
		s.sendError(fmt.Errorf("failed to subscribe to %s: %w", topic, token.Error()))
	} else {
		s.logger.Info().Str("topic", topic).Msg("Successfully subscribed to MQTT topic")
	}
}

func (s *IngestionService[T]) onPahoConnectionLost(_ mqtt.Client, err error) {
	s.logger.Error().Err(err).Msg("Paho client lost MQTT connection. Auto-reconnect will be attempted.")
}

func newTLSConfig(cfg *MQTTClientConfig, _ zerolog.Logger) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: cfg.InsecureSkipVerify,
	}
	if cfg.CACertFile != "" {
		caCert, err := os.ReadFile(cfg.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate file %s: %w", cfg.CACertFile, err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to append CA certificate from %s to pool", cfg.CACertFile)
		}
		tlsConfig.RootCAs = caCertPool
	}
	if cfg.ClientCertFile != "" && cfg.ClientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate/key pair: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	return tlsConfig, nil
}

func (s *IngestionService[T]) initAndConnectMQTTClient() error {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(s.mqttClientConfig.BrokerURL)
	uniqueSuffix := time.Now().UnixNano() % 1000000
	opts.SetClientID(fmt.Sprintf("%s%d", s.mqttClientConfig.ClientIDPrefix, uniqueSuffix))
	opts.SetUsername(s.mqttClientConfig.Username)
	opts.SetPassword(s.mqttClientConfig.Password)
	opts.SetKeepAlive(s.mqttClientConfig.KeepAlive)
	opts.SetConnectTimeout(s.mqttClientConfig.ConnectTimeout)
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(s.mqttClientConfig.ReconnectWaitMax)
	opts.SetOrderMatters(false)
	opts.SetConnectionAttemptHandler(func(broker *url.URL, tlsCfg *tls.Config) *tls.Config {
		s.logger.Info().Str("broker", broker.String()).Msg("Attempting to connect to MQTT broker")
		return tlsCfg
	})
	if strings.HasPrefix(strings.ToLower(s.mqttClientConfig.BrokerURL), "tls://") ||
		strings.HasPrefix(strings.ToLower(s.mqttClientConfig.BrokerURL), "ssl://") {
		tlsConfig, err := newTLSConfig(&s.mqttClientConfig, s.logger)
		if err != nil {
			return fmt.Errorf("failed to create TLS config: %w", err)
		}
		opts.SetTLSConfig(tlsConfig)
		s.logger.Info().Msg("TLS configured for MQTT client.")
	}
	opts.SetOnConnectHandler(s.onPahoConnect)
	opts.SetConnectionLostHandler(s.onPahoConnectionLost)
	s.pahoClient = mqtt.NewClient(opts)
	s.logger.Info().Str("client_id", opts.ClientID).Msg("Paho MQTT client created. Attempting to connect...")
	if token := s.pahoClient.Connect(); token.WaitTimeout(s.mqttClientConfig.ConnectTimeout) && token.Error() != nil {
		return fmt.Errorf("paho MQTT client connect error: %w", token.Error())
	}
	return nil
}
