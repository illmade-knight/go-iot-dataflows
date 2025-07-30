// builder/ingestion/inapp_integration_test.go
//go:build integration

package ingestion

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/illmade-knight/go-dataflow/pkg/types"
	"os"
	"regexp"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/microservice"
	"github.com/illmade-knight/go-dataflow/pkg/mqttconverter"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngestionServiceWrapper_Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	deviceFinder := regexp.MustCompile(`^[^/]+/([^/]+)/[^/]+$`)

	// --- 1. Setup Emulators and Logger ---
	logger := zerolog.New(os.Stderr).Level(zerolog.InfoLevel)

	mqttConnection := emulators.SetupMosquittoContainer(t, ctx, emulators.GetDefaultMqttImageContainer())
	pubsubConnection := emulators.SetupPubsubEmulator(t, ctx, emulators.GetDefaultPubsubConfig("test-project", map[string]string{"ingestion-output-topic": "ingestion-verifier-sub"}))

	projectID := "test-project"
	// --- 2. Configure the Ingestion Service Wrapper ---
	producerCfg := messagepipeline.NewGooglePubsubProducerDefaults(projectID)
	producerCfg.TopicID = "ingestion-output-topic"
	cfg := &Config{
		BaseConfig: microservice.BaseConfig{
			ProjectID: projectID,
			HTTPPort:  ":0",
		},
		MQTT: mqttconverter.MQTTClientConfig{
			BrokerURL:      mqttConnection.EmulatorAddress,
			Topic:          "devices/+/data",
			ClientIDPrefix: "ingestion-wrapper-test-",
			ConnectTimeout: 10 * time.Second,
		},
		Producer:             *producerCfg,
		NumProcessingWorkers: 5,
		PubsubOptions:        pubsubConnection.ClientOptions,
	}

	// The transformer logic is defined in the test setup.
	ingestionTransformer := func(ctx context.Context, msg types.ConsumedMessage) (*types.PublishMessage, bool, error) {
		// This logic can be expanded, but for now, it's a direct conversion.
		topic := msg.Attributes["mqtt_topic"]
		var deviceID string

		matches := deviceFinder.FindStringSubmatch(topic)
		if len(matches) > 1 {
			deviceID = matches[1]
		}

		output := &types.PublishMessage{
			ID:             msg.ID,
			Payload:        msg.Payload,
			PublishTime:    msg.PublishTime,
			EnrichmentData: map[string]interface{}{"DeviceID": deviceID, "Topic": topic, "Timestamp": msg.PublishTime},
		}
		return output, false, nil
	}

	// --- 3. Create and Start the Service Wrapper ---
	serviceWrapper, err := NewIngestionServiceWrapper[types.PublishMessage](ctx, cfg, logger, ingestionTransformer)
	require.NoError(t, err)

	serviceCtx, serviceCancel := context.WithCancel(ctx)
	t.Cleanup(serviceCancel)
	go func() {
		if startErr := serviceWrapper.Start(serviceCtx); startErr != nil {
			// Don't fail the test on context cancellation during cleanup.
			if !errors.Is(startErr, context.Canceled) {
				t.Logf("IngestionServiceWrapper.Start() failed during test: %v", startErr)
			}
		}
	}()
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = serviceWrapper.Shutdown(shutdownCtx)
	})

	// --- 4. Setup Test Clients ---
	mqttTestPubClient, err := emulators.CreateTestMqttPublisher(mqttConnection.EmulatorAddress, "test-publisher-main")
	require.NoError(t, err)
	t.Cleanup(func() { mqttTestPubClient.Disconnect(250) })

	subClient, err := pubsub.NewClient(ctx, "test-project", pubsubConnection.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = subClient.Close() })
	processedSub := subClient.Subscription("ingestion-verifier-sub")

	// --- 5. Publish a Test Message and Verify the Result ---
	t.Run("Publish MQTT and Verify PubSub Output", func(t *testing.T) {
		devicePayload := map[string]interface{}{"temperature": 25.5, "humidity": 60}
		payloadBytes, err := json.Marshal(devicePayload)
		require.NoError(t, err)

		publishTopic := "devices/test-device-123/data"
		token := mqttTestPubClient.Publish(publishTopic, 1, false, payloadBytes)
		token.Wait()
		require.NoError(t, token.Error())

		// --- Verification ---
		pullCtx, pullCancel := context.WithTimeout(ctx, 30*time.Second)
		t.Cleanup(pullCancel)

		var receivedMsg *pubsub.Message
		err = processedSub.Receive(pullCtx, func(ctxMsg context.Context, msg *pubsub.Message) {
			msg.Ack()
			receivedMsg = msg
			pullCancel() // Stop receiving after the first message
		})

		if err != nil && !errors.Is(err, context.Canceled) {
			require.NoError(t, err, "Receiving from Pub/Sub failed")
		}
		require.NotNil(t, receivedMsg, "Did not receive a message from Pub/Sub")

		var result types.PublishMessage
		err = json.Unmarshal(receivedMsg.Data, &result)
		require.NoError(t, err)

		assert.Equal(t, publishTopic, result.EnrichmentData["Topic"])
		assert.Equal(t, "test-device-123", result.EnrichmentData["DeviceID"])
		assert.JSONEq(t, string(payloadBytes), string(result.Payload))
	})
}
