//go:build integration

package enrichment

import (
	"cloud.google.com/go/firestore"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-cloud-manager/microservice"
	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"os"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/illmade-knight/go-dataflow/pkg/types"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	projectID = "test-project"
)

// receiveSingleMessage is a helper to wait for exactly one message from a subscription.
func receiveSingleMessage(t *testing.T, ctx context.Context, sub *pubsub.Subscription, timeout time.Duration) *pubsub.Message {
	t.Helper()
	var receivedMsg *pubsub.Message
	var mu sync.RWMutex

	receiveCtx, receiveCancel := context.WithTimeout(ctx, timeout)
	defer receiveCancel()

	err := sub.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
		mu.Lock()
		defer mu.Unlock()
		if receivedMsg == nil {
			receivedMsg = msg
			msg.Ack()
			receiveCancel() // Stop receiving after the first message
		} else {
			msg.Nack() // Nack any subsequent messages
		}
	})

	// context.Canceled is the expected error when pullCancel() is called on success.
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Logf("Receive loop ended with an unexpected error: %v", err)
	}

	mu.RLock()
	defer mu.RUnlock()
	return receivedMsg
}

func TestEnrichmentServiceWrapper_Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).
		Level(zerolog.DebugLevel).
		With().Timestamp().Logger()

	// --- 1. Setup Emulators ---
	rc := emulators.GetDefaultRedisImageContainer()
	redisConn := emulators.SetupRedisContainer(t, ctx, rc)
	fc := emulators.GetDefaultFirestoreConfig(projectID)
	firestoreConn := emulators.SetupFirestoreEmulator(t, ctx, fc)

	runID := uuid.New().String()[:8]
	inputTopicID := fmt.Sprintf("raw-device-messages-%s", runID)
	inputSubID := fmt.Sprintf("enrichment-app-sub-%s", runID)
	outputTopicID := fmt.Sprintf("enriched-device-messages-%s", runID)
	deadLetterTopicID := fmt.Sprintf("enrichment-app-dlt-%s", runID)

	pubsubConn := emulators.SetupPubsubEmulator(t, ctx, emulators.GetDefaultPubsubConfig(projectID, nil))

	// --- 2. Seed Firestore with Test Data ---
	fsClient, err := firestore.NewClient(ctx, projectID, firestoreConn.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = fsClient.Close()
	})

	testDeviceID := "device-001"
	testDeviceData := DeviceMetadata{
		ClientID:   "client-abc",
		LocationID: "location-123",
		Category:   "sensor",
	}
	_, err = fsClient.Collection("devices").Doc(testDeviceID).Set(ctx, testDeviceData)
	require.NoError(t, err)

	// --- 3. Configure the Service Wrapper ---
	cfg := &Config{
		BaseConfig: microservice.BaseConfig{
			ProjectID: projectID,
			HTTPPort:  ":0", // Use a random port
		},
		ServiceName:  "enrichment-app-test",
		DataflowName: "test-flow",
		Consumer:     Consumer{SubscriptionID: inputSubID},
		ProducerConfig: &messagepipeline.GooglePubsubProducerConfig{
			TopicID: outputTopicID,
		},
		CacheConfig: CacheConfig{
			RedisConfig: cache.RedisConfig{
				Addr:     redisConn.EmulatorAddress,
				CacheTTL: 5 * time.Minute,
			},
			FirestoreConfig: &cache.FirestoreConfig{
				CollectionName: "devices",
			},
		},
		ProcessorConfig: ProcessorConfig{NumWorkers: 2},
	}

	// --- 5. Setup Test Pub/Sub Client for Publishing/Verifying ---
	psClient, err := pubsub.NewClient(ctx, projectID, pubsubConn.ClientOptions...)
	t.Cleanup(func() {
		_ = psClient.Close()
	})

	inputTopic, err := psClient.CreateTopic(ctx, inputTopicID)
	require.NoError(t, err)
	t.Cleanup(func() {
		err = inputTopic.Delete(ctx)
		if err != nil {
			t.Logf("Error deleting topic: %v", err)
		}
	})

	inputSub, err := psClient.CreateSubscription(ctx, inputSubID, pubsub.SubscriptionConfig{
		Topic: inputTopic,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		err = inputSub.Delete(ctx)
		if err != nil {
			t.Logf("Error deleting subscription: %v", err)
		}
	})

	outputTopic, err := psClient.CreateTopic(ctx, outputTopicID)
	require.NoError(t, err)
	t.Cleanup(func() {
		err = outputTopic.Delete(ctx)
		if err != nil {
			t.Logf("Error deleting topic: %v", err)
		}
	})
	deadLetterTopic, err := psClient.CreateTopic(ctx, deadLetterTopicID)
	require.NoError(t, err)
	t.Cleanup(func() {
		err = deadLetterTopic.Delete(ctx)
		if err != nil {
			t.Logf("Error deleting topic: %v", err)
		}
	})

	// --- 4. Create and Start the Service Wrapper ---
	// Note: The wrapper creates its own internal Pub/Sub and Firestore clients.
	wrapper, err := NewPublishMessageEnrichmentServiceWrapperWithClients(cfg, logger, psClient, fsClient)
	require.NoError(t, err)

	go func() {
		if startErr := wrapper.Start(); startErr != nil {
			t.Logf("EnrichmentServiceWrapper.Start() failed during test: %v", startErr)
		}
	}()
	t.Cleanup(wrapper.Shutdown)

	// --- 6. Run Test Cases ---
	t.Run("Successful Enrichment", func(t *testing.T) {
		// Create a temporary subscription to the output topic to verify the result
		verifierSub, err := psClient.CreateSubscription(ctx, "verifier-sub-ok", pubsub.SubscriptionConfig{Topic: outputTopic})
		require.NoError(t, err)
		t.Cleanup(func() {
			err = verifierSub.Delete(ctx)
			if err != nil {
				t.Logf("Error deleting subscription: %v", err)
			}
		})

		// Publish a message that should be successfully enriched
		originalPayload := `{"value": 42}`
		res := inputTopic.Publish(ctx, &pubsub.Message{
			Data:       []byte(originalPayload),
			Attributes: map[string]string{"uid": testDeviceID},
		})
		_, err = res.Get(ctx)
		require.NoError(t, err)

		// Wait for the enriched message on the output topic
		receivedMsg := receiveSingleMessage(t, ctx, verifierSub, 15*time.Second)
		require.NotNil(t, receivedMsg, "Did not receive an enriched message on the output topic")

		// Assert the content of the enriched message
		var enrichedResult types.PublishMessage
		err = json.Unmarshal(receivedMsg.Data, &enrichedResult)
		require.NoError(t, err)

		assert.JSONEq(t, originalPayload, string(enrichedResult.Payload))
		require.NotNil(t, enrichedResult.EnrichmentData)
		assert.Equal(t, testDeviceData.ClientID, enrichedResult.EnrichmentData["name"])
		assert.Equal(t, testDeviceData.LocationID, enrichedResult.EnrichmentData["location"])
		assert.Equal(t, testDeviceData.Category, enrichedResult.EnrichmentData["serviceTag"])
	})

	t.Run("Failed Enrichment Sends to DLT", func(t *testing.T) {
		// This test requires a DLT to be configured on the input subscription,
		// which our wrapper doesn't do. For this test, we'll assume the transformer
		// would use a direct publisher if one were provided.
		// Since the current wrapper doesn't support a DLT publisher,
		// this test case is a placeholder for that future functionality.
		t.Skip("Skipping DLT test: Wrapper does not currently support a dead-letter publisher.")
	})
}
