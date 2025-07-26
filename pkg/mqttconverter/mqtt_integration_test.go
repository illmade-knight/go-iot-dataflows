// mqttconverter/mqttservice_integration_test.go
//go:build integration

package mqttconverter

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"errors"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

// RawMessage defines the canonical data structure for this test.
type RawMessage struct {
	Topic     string          `json:"topic"`
	Payload   json.RawMessage `json:"payload"`
	Timestamp string          `json:"timestamp"`
}

func TestIngestionService_Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// --- 1. Setup Emulators and a real logger ---
	// Using a real logger to see service output during the test.
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).
		Level(zerolog.DebugLevel).
		With().Timestamp().Logger()

	mqttConnection := emulators.SetupMosquittoContainer(t, ctx, emulators.GetDefaultMqttImageContainer())
	pubsubConnection := emulators.SetupPubsubEmulator(t, ctx, emulators.GetDefaultPubsubConfig("test-project", map[string]string{"processed-topic": "processed-sub"}))

	// --- 2. Manually Construct Service Components ---
	// This approach gives us direct control and visibility, unlike the application wrapper.

	// Create a shared Pub/Sub client pointing to the emulator
	psClient, err := pubsub.NewClient(ctx, "test-project", pubsubConnection.ClientOptions...)
	require.NoError(t, err)
	defer psClient.Close()

	// Manually create the configuration for each component
	mqttCfg := MQTTClientConfig{
		BrokerURL:      mqttConnection.EmulatorAddress,
		Topic:          "devices/+/data",
		ClientIDPrefix: "ingestion-test-",
	}

	producerCfg := &messagepipeline.GooglePubsubProducerConfig{
		ProjectID: "test-project",
		TopicID:   "processed-topic",
	}

	serviceCfg := DefaultIngestionServiceConfig()

	// Manually create the producer
	producer, err := messagepipeline.NewGooglePubsubProducer[RawMessage](psClient, producerCfg, logger)
	require.NoError(t, err)

	// Define the transformer function
	transformer := func(msg InMessage) (*RawMessage, bool, error) {
		if msg.Duplicate {
			return nil, true, nil
		}
		transformed := &RawMessage{
			Topic:     msg.Topic,
			Payload:   msg.Payload,
			Timestamp: msg.Timestamp.Format(time.RFC3339Nano),
		}
		return transformed, false, nil
	}

	// Manually create the IngestionService instance
	service := NewIngestionService[RawMessage](producer, transformer, logger, serviceCfg, mqttCfg)

	// --- 3. Start the Service and Test Clients ---
	err = service.Start()
	require.NoError(t, err, "Service should start without error")
	defer service.Stop()

	// Wait for MQTT subscription to be established
	time.Sleep(2 * time.Second)

	mqttTestPubClient, err := emulators.CreateTestMqttPublisher(mqttConnection.EmulatorAddress, "test-publisher-main")
	require.NoError(t, err)
	defer mqttTestPubClient.Disconnect(250)

	processedSub := psClient.Subscription("processed-sub")

	// --- 4. Publish Test Message and Verify ---
	t.Run("PublishAndReceiveTransformedMessage", func(t *testing.T) {
		devicePayload := map[string]interface{}{"value": 42, "status": "ok"}
		msgBytes, err := json.Marshal(devicePayload)
		require.NoError(t, err)

		publishTopic := "devices/test-eui-001/data"
		token := mqttTestPubClient.Publish(publishTopic, 1, false, msgBytes)
		require.True(t, token.WaitTimeout(10*time.Second), "MQTT Publish token timed out")
		require.NoError(t, token.Error(), "MQTT Publish failed")
		logger.Info().Str("topic", publishTopic).Msg("Test message published to MQTT")

		// --- Verification ---
		pullCtx, pullCancel := context.WithTimeout(ctx, 30*time.Second)
		defer pullCancel()

		var receivedMsg *pubsub.Message
		err = processedSub.Receive(pullCtx, func(ctxMsg context.Context, msg *pubsub.Message) {
			logger.Info().Str("id", msg.ID).Msg("Received message from Pub/Sub")
			msg.Ack()
			receivedMsg = msg
			pullCancel()
		})

		// We expect the context to be canceled by a successful receive, so we don't treat Canceled as an error.
		if err != nil && !errors.Is(err, context.Canceled) {
			require.NoError(t, err, "Receiving from Pub/Sub failed")
		}
		require.NotNil(t, receivedMsg, "Did not receive a message from Pub/Sub")

		var result RawMessage
		err = json.Unmarshal(receivedMsg.Data, &result)
		require.NoError(t, err, "Failed to unmarshal received Pub/Sub message into RawMessage struct")

		assert.Equal(t, publishTopic, result.Topic)
		assert.JSONEq(t, string(msgBytes), string(result.Payload))
	})
}
