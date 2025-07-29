//go:build integration

package enrich_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/illmade-knight/go-iot-dataflows/pkg/enrich"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/microservice"
	"github.com/illmade-knight/go-dataflow/pkg/types"
	"github.com/illmade-knight/go-test/emulators"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnrichmentServiceWrapper_Integration(t *testing.T) {
	testContext, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	logger := zerolog.New(os.Stderr).Level(zerolog.DebugLevel)

	projectID := "test-project"
	// --- 1. Setup Emulators ---
	rc := emulators.GetDefaultRedisImageContainer()
	redisConn := emulators.SetupRedisContainer(t, testContext, rc)
	fc := emulators.GetDefaultFirestoreConfig(projectID)
	firestoreConn := emulators.SetupFirestoreEmulator(t, testContext, fc)
	pc := emulators.GetDefaultPubsubConfig(projectID, nil)
	pubsubConn := emulators.SetupPubsubEmulator(t, testContext, pc)

	runID := uuid.New().String()[:8]
	inputTopicID := fmt.Sprintf("raw-messages-%s", runID)
	inputSubID := fmt.Sprintf("enrichment-sub-%s", runID)
	outputTopicID := fmt.Sprintf("enriched-messages-%s", runID)

	// --- 2. Seed Firestore with Test Data ---
	fsClient, err := firestore.NewClient(testContext, projectID, firestoreConn.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fsClient.Close() })

	testDeviceID := "device-001"
	testDeviceData := DeviceInfo{ClientID: "client-abc", LocationID: "location-123", Category: "sensor"}
	_, err = fsClient.Collection("devices").Doc(testDeviceID).Set(testContext, testDeviceData)
	require.NoError(t, err)

	// --- 3. Configure the Service Wrapper ---
	producerCfg := messagepipeline.NewGooglePubsubProducerDefaults(projectID)
	producerCfg.TopicID = outputTopicID
	cfg := &enrich.Config{
		BaseConfig:     microservice.BaseConfig{ProjectID: projectID, HTTPPort: ":0"},
		Consumer:       enrich.Consumer{SubscriptionID: inputSubID},
		ProducerConfig: producerCfg,
		CacheConfig: enrich.CacheConfig{
			RedisConfig:       cache.RedisConfig{Addr: redisConn.EmulatorAddress, CacheTTL: 5 * time.Minute},
			FirestoreConfig:   &cache.FirestoreConfig{CollectionName: "devices"},
			CacheWriteTimeout: 2 * time.Second,
		},
		ProcessorConfig: enrich.ProcessorConfig{NumWorkers: 2},
	}

	// --- 4. Setup Pub/Sub Resources ---
	psClient, err := pubsub.NewClient(testContext, projectID, pubsubConn.ClientOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = psClient.Close() })

	inputTopic, err := psClient.CreateTopic(testContext, inputTopicID)
	require.NoError(t, err)
	_, err = psClient.CreateSubscription(testContext, inputSubID, pubsub.SubscriptionConfig{Topic: inputTopic})
	require.NoError(t, err)
	outputTopic, err := psClient.CreateTopic(testContext, outputTopicID)
	require.NoError(t, err)

	// --- 5. Create and Start the Service Wrapper ---
	wrapper, err := enrich.NewEnrichmentServiceWrapperWithClients(testContext, cfg, logger, psClient, fsClient, BasicKeyExtractor, DeviceEnricher)
	require.NoError(t, err)

	serviceCtx, serviceCancel := context.WithCancel(testContext)
	t.Cleanup(serviceCancel)
	go func() {
		if startErr := wrapper.Start(serviceCtx); startErr != nil && !errors.Is(startErr, context.Canceled) {
			t.Logf("EnrichmentServiceWrapper.Start() failed: %v", startErr)
		}
	}()
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = wrapper.Shutdown(shutdownCtx)
	})

	// --- 6. Run Test ---
	t.Run("Successful Enrichment", func(t *testing.T) {
		verifierSub, err := psClient.CreateSubscription(testContext, "verifier-sub-ok", pubsub.SubscriptionConfig{Topic: outputTopic})
		require.NoError(t, err)
		t.Cleanup(func() { _ = verifierSub.Delete(testContext) })

		originalPayload := `{"value": 42}`
		res := inputTopic.Publish(testContext, &pubsub.Message{Data: []byte(originalPayload), Attributes: map[string]string{"uid": testDeviceID}})
		_, err = res.Get(testContext)
		require.NoError(t, err)

		receivedMsg := receiveSingleMessage(t, testContext, verifierSub, 15*time.Second)
		require.NotNil(t, receivedMsg, "Did not receive an enriched message")

		var enrichedResult types.PublishMessage
		err = json.Unmarshal(receivedMsg.Data, &enrichedResult)
		require.NoError(t, err)

		assert.JSONEq(t, originalPayload, string(enrichedResult.Payload))
		require.NotNil(t, enrichedResult.EnrichmentData)
		assert.Equal(t, testDeviceData.ClientID, enrichedResult.EnrichmentData["name"])
	})
}
