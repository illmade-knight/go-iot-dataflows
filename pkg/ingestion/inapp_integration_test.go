// builder/ingestion/inapp_integration_test.go
//go:build integration

package ingestion

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/illmade-knight/go-cloud-manager/microservice"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-iot-dataflows/pkg/mqttconverter"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngestionServiceWrapper_Integration(t *testing.T) {
	testContext, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// --- 1. Setup Emulators and Logger ---
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).
		Level(zerolog.DebugLevel).
		With().Timestamp().Logger()

	mqttConnection := emulators.SetupMosquittoContainer(t, testContext, emulators.GetDefaultMqttImageContainer())
	pubsubConnection := emulators.SetupPubsubEmulator(t, testContext, emulators.GetDefaultPubsubConfig("test-project", map[string]string{"ingestion-output-topic": "ingestion-verifier-sub"}))

	// --- 2. Configure the Ingestion Service Wrapper ---
	// This configures the application exactly as it would run, but points it to our emulators.
	cfg := &Config{
		BaseConfig: microservice.BaseConfig{
			ProjectID: "test-project",
			HTTPPort:  ":0", // Use a random available port for the test
		},
		MQTT: mqttconverter.MQTTClientConfig{
			BrokerURL:      mqttConnection.EmulatorAddress,
			Topic:          "devices/+/data",
			ClientIDPrefix: "ingestion-wrapper-test-",
		},
		Producer: messagepipeline.GooglePubsubProducerConfig{
			TopicID: "ingestion-output-topic",
		},
		Service:       mqttconverter.DefaultIngestionServiceConfig(),
		PubsubOptions: pubsubConnection.ClientOptions, // Critical line to point to the emulator
	}

	// --- 3. Create and Start the Service Wrapper ---
	serviceWrapper, err := NewIngestionServiceWrapper(testContext, cfg, logger)
	require.NoError(t, err)

	go func() {
		if startErr := serviceWrapper.Start(); startErr != nil {
			t.Logf("IngestionServiceWrapper.Start() failed during test: %v", startErr)
		}
	}()
	defer serviceWrapper.Shutdown()

	// Give the service a moment to connect and subscribe to MQTT
	time.Sleep(2 * time.Second)

	// --- 4. Setup Test Clients ---
	mqttTestPubClient, err := emulators.CreateTestMqttPublisher(mqttConnection.EmulatorAddress, "test-publisher-main")
	require.NoError(t, err)
	defer mqttTestPubClient.Disconnect(250)

	subClient, err := pubsub.NewClient(testContext, "test-project", pubsubConnection.ClientOptions...)
	require.NoError(t, err)
	defer func(subClient *pubsub.Client) {
		_ = subClient.Close()
	}(subClient)
	processedSub := subClient.Subscription("ingestion-verifier-sub")

	// --- 5. Publish a Test Message and Verify the Result ---
	t.Run("Publish MQTT and Verify PubSub Output", func(t *testing.T) {
		// Define the raw payload a device would send
		devicePayload := map[string]interface{}{"temperature": 25.5, "humidity": 60}
		payloadBytes, err := json.Marshal(devicePayload)
		require.NoError(t, err)

		publishTopic := "devices/test-device-123/data"

		// Publish the message to the MQTT broker
		token := mqttTestPubClient.Publish(publishTopic, 1, false, payloadBytes)
		require.True(t, token.WaitTimeout(10*time.Second), "MQTT Publish token timed out")
		require.NoError(t, token.Error(), "MQTT Publish failed")
		logger.Info().Str("topic", publishTopic).Msg("Test message published to MQTT broker")

		// --- Verification ---
		pullCtx, pullCancel := context.WithTimeout(testContext, 30*time.Second)
		defer pullCancel()

		var receivedMsg *pubsub.Message
		err = processedSub.Receive(pullCtx, func(ctxMsg context.Context, msg *pubsub.Message) {
			logger.Info().Str("id", msg.ID).Msg("Received message from Pub/Sub for verification")
			msg.Ack()
			receivedMsg = msg
			pullCancel() // Stop receiving after the first message
		})

		// We expect the context to be canceled by a successful receive.
		// Any other error (like DeadlineExceeded) is a failure.
		if err != nil && !errors.Is(err, context.Canceled) {
			require.NoError(t, err, "Receiving from Pub/Sub failed")
		}
		require.NotNil(t, receivedMsg, "Did not receive a message from Pub/Sub")

		// Unmarshal and assert the structure of the message published by the service
		var result RawMessage
		err = json.Unmarshal(receivedMsg.Data, &result)
		require.NoError(t, err, "Failed to unmarshal received Pub/Sub message into RawMessage struct")

		assert.Equal(t, publishTopic, result.Topic)
		assert.JSONEq(t, string(payloadBytes), string(result.Payload))
		_, err = time.Parse(time.RFC3339Nano, result.Timestamp)
		assert.NoError(t, err, "Timestamp should be in a valid RFC3339Nano format")
	})
}
