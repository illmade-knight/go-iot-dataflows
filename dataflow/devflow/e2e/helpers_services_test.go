//go:build integration

package e2e

import (
	"cloud.google.com/go/firestore"
	"context"
	"encoding/json"
	"fmt"
	"github.com/illmade-knight/go-iot/pkg/mqttconverter"
	"github.com/rs/zerolog/log"
	"net/http"
	"testing"
	"time"

	"github.com/illmade-knight/go-cloud-manager/pkg/servicemanager"

	"github.com/illmade-knight/go-iot-dataflows/builder"
	"github.com/illmade-knight/go-iot-dataflows/builder/bigquery"
	"github.com/illmade-knight/go-iot-dataflows/builder/enrichment"
	"github.com/illmade-knight/go-iot-dataflows/builder/icestore"
	"github.com/illmade-knight/go-iot-dataflows/builder/ingestion"
	"github.com/illmade-knight/go-iot-dataflows/builder/servicedirector"
	"github.com/illmade-knight/go-iot/helpers/loadgen"
	"github.com/illmade-knight/go-iot/pkg/device"
	"github.com/illmade-knight/go-iot/pkg/messagepipeline"
	"github.com/illmade-knight/go-iot/pkg/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// setupEnrichmentTestData creates devices and seeds Firestore for any test involving enrichment.
// It returns the devices for the load generator, a map for verification, and a cleanup function.
func setupEnrichmentTestData(
	t *testing.T,
	ctx context.Context,
	fsClient *firestore.Client,
	firestoreCollection string, // Accepts the name as a parameter
	runID string,
	numDevices int,
	rate float64,
) ([]*loadgen.Device, map[string]string, func()) { // No longer returns the name
	t.Helper()

	devices := make([]*loadgen.Device, numDevices)
	deviceToClientID := make(map[string]string)

	for i := 0; i < numDevices; i++ {
		deviceID := fmt.Sprintf("e2e-enrich-device-%d-%s", i, runID)
		clientID := fmt.Sprintf("client-for-%s", deviceID)
		deviceToClientID[deviceID] = clientID
		devices[i] = &loadgen.Device{ID: deviceID, MessageRate: rate, PayloadGenerator: &testPayloadGenerator{}}

		deviceDoc := map[string]interface{}{"clientID": clientID, "locationID": "loc-456", "DeviceCategory": "cat-789"}
		_, err := fsClient.Collection(firestoreCollection).Doc(deviceID).Set(ctx, deviceDoc)
		require.NoError(t, err, "Failed to set device document for %s", deviceID)
	}

	// Return a cleanup function that the calling test can run via t.Cleanup.
	cleanupFunc := func() {
		log.Info().Msg("Cleaning up Firestore documents from helper...")
		for _, device := range devices {
			fsClient.Collection(firestoreCollection).Doc(device.ID).Delete(context.Background())
		}
	}

	return devices, deviceToClientID, cleanupFunc
}

// startServiceDirector correctly initializes and starts the refactored ServiceDirector for testing.
func startServiceDirector(t *testing.T, ctx context.Context, logger zerolog.Logger, arch *servicemanager.MicroserviceArchitecture, schemaRegistry map[string]interface{}) (builder.Service, string) {
	t.Helper()

	//we'd actually prefer if we didn't need an empty registry if no Bigquery manager is required
	if schemaRegistry == nil {
		log.Warn().Msg("schema registry is nil, initializing empty registry")
		schemaRegistry = make(map[string]interface{})
	}

	// Use a mock loader that provides our in-memory architecture.
	loader := &inMemoryLoader{arch: arch}

	// Configure the director to run on a random available port.
	directorCfg := &servicedirector.Config{
		BaseConfig: builder.BaseConfig{
			HTTPPort: ":0", // Let the OS choose the port
		},
	}

	director, err := servicedirector.NewServiceDirector(ctx, directorCfg, loader, schemaRegistry, logger)
	require.NoError(t, err)

	err = director.Start()
	require.NoError(t, err)

	// Construct the base URL for the test client.
	baseURL := "http://127.0.0.1" + director.GetHTTPPort()
	return director, baseURL
}

// startIngestionService starts a simple ingestion service that acts as a pure bridge.
func startIngestionService(t *testing.T, logger zerolog.Logger, directorURL, mqttURL, projectID, topicID, dataflowName string) (builder.Service, error) {
	t.Helper()
	cfg := &ingestion.Config{
		BaseConfig:         builder.BaseConfig{LogLevel: "debug", HTTPPort: ":0", ProjectID: projectID},
		ServiceName:        "ingestion-service-e2e",
		DataflowName:       dataflowName,
		ServiceDirectorURL: directorURL,
	}
	cfg.Publisher.TopicID = topicID
	cfg.MQTT.BrokerURL = mqttURL
	cfg.MQTT.Topic = "devices/+/data"
	cfg.MQTT.ClientIDPrefix = "ingestion-e2e-"

	wrapper, err := ingestion.NewIngestionServiceWrapper(cfg, nil, logger, cfg.ServiceName, cfg.DataflowName)
	if err != nil {
		return nil, err
	}
	go func() {
		if err := wrapper.Start(); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("IngestionService failed")
		}
	}()
	return wrapper, nil
}

// NEW: startAttributeIngestionService starts an ingestion service with attribute extraction enabled.
func startAttributeIngestionService(t *testing.T, logger zerolog.Logger, directorURL, mqttURL, projectID, topicID, dataflowName string) (builder.Service, error) {
	t.Helper()
	cfg := &ingestion.Config{
		BaseConfig:         builder.BaseConfig{LogLevel: "debug", HTTPPort: ":0", ProjectID: projectID},
		ServiceName:        "ingestion-service-e2e-attr",
		DataflowName:       dataflowName,
		ServiceDirectorURL: directorURL,
	}
	cfg.Publisher.TopicID = topicID
	cfg.MQTT.BrokerURL = mqttURL
	cfg.MQTT.Topic = "devices/+/data"
	cfg.MQTT.ClientIDPrefix = "ingestion-e2e-attr-"

	var extractor mqttconverter.AttributeExtractor = &DeviceIDExtractor{}

	wrapper, err := ingestion.NewIngestionServiceWrapper(cfg, extractor, logger, cfg.ServiceName, cfg.DataflowName)
	if err != nil {
		return nil, err
	}

	go func() {
		if err := wrapper.Start(); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("AttributeIngestionService failed during test execution")
		}
	}()

	// Wait until the service's HTTP server is responsive before proceeding.
	// This prevents race conditions where the test tries to send messages
	// before the service is fully ready to receive them.
	require.Eventually(t, func() bool {
		port := wrapper.GetHTTPPort()
		if port == "" || port == ":0" {
			return false // Port not assigned yet
		}
		healthzURL := fmt.Sprintf("http://localhost%s/healthz", port)
		resp, err := http.Get(healthzURL)
		if err != nil {
			return false // Server not ready to accept connections
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 15*time.Second, 500*time.Millisecond, "AttributeIngestionService health check did not become OK")

	logger.Info().Str("service", cfg.ServiceName).Msg("Service is healthy.")
	return wrapper, nil
}

func startEnrichmentService(t *testing.T, ctx context.Context, logger zerolog.Logger, directorURL, projectID, subID, topicID, redisAddr, firestoreCollection, dataflowName string) (builder.Service, error) {
	t.Helper()
	cfg := &enrichment.Config{
		BaseConfig:         builder.BaseConfig{LogLevel: "debug", HTTPPort: ":0", ProjectID: projectID},
		ServiceName:        "enrichment-service-e2e",
		DataflowName:       dataflowName,
		ServiceDirectorURL: directorURL,
	}
	cfg.Consumer.SubscriptionID = subID
	cfg.ProducerConfig = &messagepipeline.GooglePubsubProducerConfig{TopicID: topicID}
	cfg.CacheConfig.RedisConfig.Addr = redisAddr
	cfg.CacheConfig.FirestoreConfig = &device.FirestoreFetcherConfig{CollectionName: firestoreCollection}
	cfg.ProcessorConfig.NumWorkers = 5

	wrapper, err := enrichment.NewPublishMessageEnrichmentServiceWrapper(cfg, ctx, logger)
	if err != nil {
		return nil, err
	}
	go func() {
		if err := wrapper.Start(); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("EnrichmentService failed")
		}
	}()
	return wrapper, nil
}

// startBigQueryService starts a BigQuery service for non-enriched payloads.
func startBigQueryService(t *testing.T, logger zerolog.Logger, directorURL, projectID, subID, datasetID, tableID, dataflowName string) (builder.Service, error) {
	t.Helper()
	cfg := &bigquery.Config{
		BaseConfig:         builder.BaseConfig{LogLevel: "debug", HTTPPort: ":0", ProjectID: projectID},
		ServiceName:        "bigquery-service-e2e",
		DataflowName:       dataflowName,
		ServiceDirectorURL: directorURL,
	}
	cfg.Consumer.SubscriptionID = subID
	cfg.BigQueryConfig.DatasetID = datasetID
	cfg.BigQueryConfig.TableID = tableID
	cfg.BatchProcessing.BatchSize = 10
	cfg.BatchProcessing.NumWorkers = 2
	cfg.BatchProcessing.FlushTimeout = 5 * time.Second
	transformer := func(msg types.ConsumedMessage) (*TestPayload, bool, error) {
		var p TestPayload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			return nil, false, err
		}
		return &p, false, nil
	}
	wrapper, err := bigquery.NewBQServiceWrapper[TestPayload](cfg, logger, transformer)
	if err != nil {
		return nil, err
	}
	go func() {
		if err := wrapper.Start(); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("BigQueryService (simple) failed")
		}
	}()
	return wrapper, nil
}

// startEnrichedBigQueryService starts a BigQuery service for enriched payloads.
func startEnrichedBigQueryService(t *testing.T, logger zerolog.Logger, directorURL, projectID, subID, datasetID, tableID, dataflowName string) (builder.Service, error) {
	t.Helper()
	cfg := &bigquery.Config{
		BaseConfig:         builder.BaseConfig{LogLevel: "debug", HTTPPort: ":0", ProjectID: projectID},
		ServiceName:        "bigquery-service-e2e",
		DataflowName:       dataflowName,
		ServiceDirectorURL: directorURL,
	}
	cfg.Consumer.SubscriptionID = subID
	cfg.BigQueryConfig.DatasetID = datasetID
	cfg.BigQueryConfig.TableID = tableID
	cfg.BatchProcessing.BatchSize = 10
	cfg.BatchProcessing.NumWorkers = 2
	cfg.BatchProcessing.FlushTimeout = 5 * time.Second

	transformer := func(msg types.ConsumedMessage) (*EnrichedTestPayload, bool, error) {
		var enrichedPayload types.PublishMessage
		if err := json.Unmarshal(msg.Payload, &enrichedPayload); err != nil {
			return nil, false, fmt.Errorf("failed to unmarshal enriched publish message: %w", err)
		}

		var originalPayload TestPayload
		if err := json.Unmarshal(enrichedPayload.Payload, &originalPayload); err != nil {
			return nil, false, fmt.Errorf("failed to unmarshal inner payload for BQ: %w", err)
		}

		p := &EnrichedTestPayload{
			DeviceID:  originalPayload.DeviceID,
			Timestamp: originalPayload.Timestamp,
			Value:     originalPayload.Value,
		}
		if enrichedPayload.DeviceInfo != nil {
			p.ClientID = enrichedPayload.DeviceInfo.Name
			p.LocationID = enrichedPayload.DeviceInfo.Location
			p.Category = enrichedPayload.DeviceInfo.ServiceTag
		}
		return p, false, nil
	}

	wrapper, err := bigquery.NewBQServiceWrapper[EnrichedTestPayload](cfg, logger, transformer)
	if err != nil {
		return nil, err
	}
	go func() {
		if err := wrapper.Start(); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("BigQueryService (enriched) failed")
		}
	}()
	return wrapper, nil
}

// startIceStoreService starts an IceStore service for archiving payloads to GCS.
func startIceStoreService(t *testing.T, logger zerolog.Logger, directorURL, projectID, subID, bucketName, dataflowName string) (builder.Service, error) {
	t.Helper()
	cfg := &icestore.Config{
		BaseConfig: builder.BaseConfig{
			LogLevel:  "debug",
			HTTPPort:  ":0",
			ProjectID: projectID,
		},
		ServiceName:        "icestore-service-e2e",
		DataflowName:       dataflowName,
		ServiceDirectorURL: directorURL,
	}
	cfg.Consumer.SubscriptionID = subID
	cfg.IceStore.BucketName = bucketName
	cfg.IceStore.ObjectPrefix = "e2e-archive/"
	cfg.BatchProcessing.BatchSize = 10
	cfg.BatchProcessing.NumWorkers = 2
	cfg.BatchProcessing.FlushTimeout = 5 * time.Second

	wrapper, err := icestore.NewIceStoreServiceWrapper(cfg, logger)
	if err != nil {
		return nil, err
	}

	go func() {
		if err := wrapper.Start(); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("IceStoreService failed")
		}
	}()
	return wrapper, nil
}
