//go:build integration

package managedload_test

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"

	"github.com/illmade-knight/go-iot-dataflows/gardenmonitor/bigquery/bqinit"
	"github.com/illmade-knight/go-iot-dataflows/gardenmonitor/enrich/eninit"

	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/helpers/loadgen"

	"github.com/illmade-knight/go-iot/pkg/bqstore"
	"github.com/illmade-knight/go-iot/pkg/device"
	"github.com/illmade-knight/go-iot/pkg/messagepipeline"
	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/illmade-knight/go-iot/pkg/types"
)

// --- Test Flags ---
var (
	keepEnrichedDataset = flag.Bool("keep-enriched-dataset", false, "If true, the BigQuery dataset for the enriched dataflow will not be deleted after the test.")
)

// --- Enriched Test Constants ---
const (
	enrichedCloudTestGCPProjectID = "gemini-power-test"
	enrichedLoadTestDuration      = 30 * time.Second
	enrichedLoadTestNumDevices    = 5
	enrichedLoadTestRatePerDevice = 2.0
	enrichedSuccessPercent        = 0.98
	enrichedCloudLoadTestTimeout  = 12 * time.Minute
	firestoreCollection           = "devices"
	testMqttTopicPattern          = "devices/+/data"
	testMqttClientIDPrefix        = "ingestion-service-cloud-load"
	cloudTestMqttHTTPPort         = ":9092"
	cloudTestBqHTTPPort           = ":9093"
)

// EnrichedGardenMonitorReadings defines the schema for our BigQuery table that stores the final, enriched data.
// It includes both the original telemetry and the metadata added by the enrichment service.
type EnrichedGardenMonitorReadings struct {
	// Original Data (can be nested for clarity)
	DE           string    `bigquery:"uid"`
	SIM          string    `bigquery:"sim"`
	RSSI         string    `bigquery:"rssi"`
	Version      string    `bigquery:"version"`
	Sequence     int       `bigquery:"sequence"`
	Battery      int       `bigquery:"battery"`
	Temperature  int       `bigquery:"temperature"`
	Humidity     int       `bigquery:"humidity"`
	SoilMoisture int       `bigquery:"soil_moisture"`
	WaterFlow    int       `bigquery:"water_flow"`
	WaterQuality int       `bigquery:"water_quality"`
	TankLevel    int       `bigquery:"tank_level"`
	AmbientLight int       `bigquery:"ambient_light"`
	Timestamp    time.Time `bigquery:"timestamp"`
	// Enriched Data
	ClientID   string `bigquery:"client_id"`
	LocationID string `bigquery:"location_id"`
	Category   string `bigquery:"category"`
}

// buildEnrichedTestServicesDefinition creates the service definition for the complete, enriched dataflow.
func buildEnrichedTestServicesDefinition(runID, gcpProjectID string) *servicemanager.TopLevelConfig {
	dataflowName := "mqtt-to-enriched-bigquery-loadtest"
	rawTopicID := fmt.Sprintf("raw-device-data-%s", runID)
	enrichedTopicID := fmt.Sprintf("enriched-device-data-%s", runID)
	enrichmentSubID := fmt.Sprintf("enrichment-service-sub-%s", runID)
	bqSubID := fmt.Sprintf("bq-service-sub-%s", runID)
	datasetID := fmt.Sprintf("enriched_device_analytics_%s", runID)
	tableID := fmt.Sprintf("enriched_monitor_payloads_%s", runID)

	return &servicemanager.TopLevelConfig{
		DefaultProjectID: gcpProjectID,
		Environments: map[string]servicemanager.EnvironmentSpec{
			"loadtest": {ProjectID: gcpProjectID},
		},
		Services: []servicemanager.ServiceSpec{
			{Name: "ingestion-service"},
			{Name: "enrichment-service"},
			{Name: "bq-writer-service"},
		},
		Dataflows: []servicemanager.ResourceGroup{
			{
				Name:         dataflowName,
				Description:  "Ephemeral dataflow for the enriched cloud load test.",
				ServiceNames: []string{"ingestion-service", "enrichment-service", "bq-writer-service"},
				Lifecycle: &servicemanager.LifecyclePolicy{
					Strategy:            servicemanager.LifecycleStrategyEphemeral,
					KeepResourcesOnTest: *keepEnrichedDataset,
				},
				Resources: servicemanager.ResourcesSpec{
					Topics: []servicemanager.TopicConfig{
						{Name: rawTopicID, ProducerService: "ingestion-service"},
						{Name: enrichedTopicID, ProducerService: "enrichment-service"},
					},
					Subscriptions: []servicemanager.SubscriptionConfig{
						{Name: enrichmentSubID, Topic: rawTopicID, ConsumerService: "enrichment-service"},
						{Name: bqSubID, Topic: enrichedTopicID, ConsumerService: "bq-writer-service"},
					},
					BigQueryDatasets: []servicemanager.BigQueryDataset{
						{Name: datasetID, Description: "Temp dataset for enriched load test"},
					},
					BigQueryTables: []servicemanager.BigQueryTable{
						{
							Name:                   tableID,
							Dataset:                datasetID,
							AccessingServices:      []string{"bq-writer-service"},
							SchemaSourceType:       "go_struct",
							SchemaSourceIdentifier: "github.com/illmade-knight/go-iot-dataflows/gardenmonitor/managedload_test.EnrichedGardenMonitorReadings",
						},
					},
				},
			},
		},
	}
}

// TestManagedEnrichedCloudLoad orchestrates the end-to-end test for the enrichment dataflow.
func TestManagedEnrichedCloudLoad(t *testing.T) {
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		t.Skip("Skipping cloud load test: GOOGLE_CLOUD_PROJECT environment variable must be set.")
	}
	t.Setenv("GOOGLE_CLOUD_PROJECT", projectID)

	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		log.Warn().Msg("GOOGLE_APPLICATION_CREDENTIALS not set, relying on Application Default Credentials (ADC).")
		adcCheckCtx, adcCheckCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer adcCheckCancel()
		realPubSubClient, errAdc := pubsub.NewClient(adcCheckCtx, projectID)
		if errAdc != nil {
			t.Skipf("Skipping cloud test: ADC check failed: %v. Please configure ADC or set GOOGLE_APPLICATION_CREDENTIALS.", errAdc)
		}
		realPubSubClient.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), enrichedCloudLoadTestTimeout)
	defer cancel()
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	// --- 2. Create Dynamic Services Definition ---
	runID := uuid.New().String()[:8]
	servicesConfig := buildEnrichedTestServicesDefinition(runID, projectID)
	servicesDef, err := servicemanager.NewInMemoryServicesDefinition(servicesConfig)
	require.NoError(t, err)

	schemaRegistry := make(map[string]interface{})
	schemaRegistry["github.com/illmade-knight/go-iot-dataflows/gardenmonitor/managedload_test.EnrichedGardenMonitorReadings"] = EnrichedGardenMonitorReadings{}

	// --- 3. Use ServiceManager to Provision Infrastructure ---
	log.Info().Str("runID", runID).Msg("Initializing ServiceManager for ENRICHED dataflow...")
	serviceManager, err := servicemanager.NewServiceManager(ctx, servicesDef, "loadtest", schemaRegistry, log.Logger)
	require.NoError(t, err)

	dataflowName := servicesConfig.Dataflows[0].Name
	_, err = serviceManager.SetupDataflow(ctx, "loadtest", dataflowName)
	require.NoError(t, err, "ServiceManager.SetupDataflow failed for enriched flow")

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		log.Info().Str("runID", runID).Msg("Tearing down ENRICHED dataflow resources...")
		teardownErr := serviceManager.TeardownDataflow(cleanupCtx, "loadtest", dataflowName)
		assert.NoError(t, teardownErr, "ServiceManager.TeardownDataflow failed")
	})

	// --- 4. Get Resource Names ---
	log.Info().Msg("Getting ENRICHED resource names from the service definition...")
	dataflowSpec, err := servicesDef.GetDataflow(dataflowName)
	require.NoError(t, err)
	rawTopicID := dataflowSpec.Resources.Topics[0].Name
	enrichedTopicID := dataflowSpec.Resources.Topics[1].Name
	enrichmentSubID := dataflowSpec.Resources.Subscriptions[0].Name
	bqSubID := dataflowSpec.Resources.Subscriptions[1].Name
	datasetID := dataflowSpec.Resources.BigQueryTables[0].Dataset
	tableID := dataflowSpec.Resources.BigQueryTables[0].Name
	log.Info().Str("rawTopic", rawTopicID).Str("enrichedTopic", enrichedTopicID).Str("enrichmentSub", enrichmentSubID).Str("bqSub", bqSubID).Msg("Using Pub/Sub resources")
	log.Info().Str("dataset", datasetID).Str("table", tableID).Msg("Using BigQuery resources")

	// --- 5. Start Emulators and Services ---
	log.Info().Msg("Setting up Mosquitto and Redis emulators...")
	mqttConnections := emulators.SetupMosquittoContainer(t, ctx, emulators.GetDefaultMqttImageContainer())
	redisConn := emulators.SetupRedisContainer(t, ctx, emulators.GetDefaultRedisImageContainer())

	// Use a real Firestore client
	firestoreClient, err := firestore.NewClient(ctx, projectID)
	require.NoError(t, err, "Failed to create real Firestore client")
	defer firestoreClient.Close()
	deviceIDs, deviceMetadata := seedDeviceMetadata(t, ctx, firestoreClient, firestoreCollection, enrichedLoadTestNumDevices)

	// Start the microservices in pipeline order
	ingestionService, mqttServer := startIngestionService(t, ctx, projectID, mqttConnections.EmulatorAddress, rawTopicID)
	defer mqttServer.Shutdown()
	go func() {
		for e := range ingestionService.Err() {
			log.Error().Err(e).Msg("Error from ingestion service")
		}
	}()

	_, enrichmentServer := startEnrichmentService(t, ctx, projectID, enrichmentSubID, enrichedTopicID, redisConn.EmulatorAddress)
	defer enrichmentServer.Shutdown()

	_, bqServer := startEnrichedBQProcessingService(t, ctx, projectID, bqSubID, datasetID, tableID)
	defer bqServer.Shutdown()

	log.Info().Msg("Pausing to allow services to start and connect...")
	time.Sleep(15 * time.Second)

	// --- 6. Start Load Generator ---
	log.Info().Msg("Configuring and starting load generator...")
	loadgenLogger := log.With().Str("service", "load-generator").Logger()
	loadgenClient := loadgen.NewMqttClient(mqttConnections.EmulatorAddress, testMqttTopicPattern, 1, loadgenLogger)
	devices := make([]*loadgen.Device, enrichedLoadTestNumDevices)
	for i := 0; i < enrichedLoadTestNumDevices; i++ {
		payloadGenerator := &gardenMonitorPayloadGenerator{}
		devices[i] = &loadgen.Device{
			ID:               deviceIDs[i],
			MessageRate:      enrichedLoadTestRatePerDevice,
			PayloadGenerator: payloadGenerator,
		}
	}

	loadGenerator := loadgen.NewLoadGenerator(loadgenClient, devices, loadgenLogger)

	populateFirestoreFromLoadgenDevices(t, ctx, firestoreClient, firestoreCollection, devices, 3)

	err = loadGenerator.Run(ctx, enrichedLoadTestDuration)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		require.NoError(t, err, "Load generator returned an unexpected error")
	}

	// --- 7. Verification ---
	log.Info().Msg("Load generation complete. Verifying results in BigQuery...")
	expectedMessages := int(float64(enrichedLoadTestNumDevices) * enrichedLoadTestRatePerDevice * enrichedLoadTestDuration.Seconds())
	successThreshold := int(float64(expectedMessages) * enrichedSuccessPercent)

	bqClient, err := bigquery.NewClient(ctx, projectID)
	require.NoError(t, err)
	defer bqClient.Close()

	// Poll BigQuery until the row count stabilizes and meets the threshold
	require.Eventually(t, func() bool {
		count, err := getRowCount(ctx, bqClient, datasetID, tableID)
		if err != nil {
			log.Warn().Err(err).Msg("Polling query for row count failed")
			return false
		}
		log.Info().Int64("current_count", count).Int("threshold", successThreshold).Msg("Polling BQ count for enriched data")
		return count >= int64(successThreshold)
	}, 4*time.Minute, 20*time.Second, "Timeout waiting for enriched data to land in BigQuery")

	// Final verification of data content
	verifyEnrichedData(t, ctx, bqClient, projectID, datasetID, tableID, deviceMetadata)
}

// --- Service Start-up Helpers ---

// startEnrichmentService configures and starts the enrichment microservice.
func startEnrichmentService(t *testing.T, ctx context.Context, projectID, subID, topicID, redisAddr string) (*messagepipeline.ProcessingService[eninit.TestEnrichedMessage], *eninit.Server) {
	t.Helper()
	cfg := &eninit.Config{
		LogLevel:  "debug",
		HTTPPort:  ":9094",
		ProjectID: projectID,
		Consumer: struct {
			SubscriptionID string `mapstructure:"subscription_id"`
		}{SubscriptionID: subID},
		Producer: &messagepipeline.GooglePubsubProducerConfig{
			ProjectID:  projectID,
			TopicID:    topicID,
			BatchDelay: 50 * time.Millisecond,
		},
		CacheConfig: struct {
			RedisConfig     device.RedisConfig             `mapstructure:"redis_config"`
			FirestoreConfig *device.FirestoreFetcherConfig `mapstructure:"firestore_config"`
		}{
			RedisConfig:     device.RedisConfig{Addr: redisAddr, CacheTTL: 5 * time.Minute},
			FirestoreConfig: &device.FirestoreFetcherConfig{ProjectID: projectID, CollectionName: firestoreCollection},
		},
		ProcessorConfig: struct {
			NumWorkers   int           `mapstructure:"num_workers"`
			BatchSize    int           `mapstructure:"batch_size"`
			FlushTimeout time.Duration `mapstructure:"flush_timeout"`
		}{NumWorkers: 5},
	}

	logger := log.With().Str("service", "enrichment").Logger()

	// Build the full chained fetcher with a real Firestore client and Redis emulator
	redisClient := redis.NewClient(&redis.Options{Addr: cfg.CacheConfig.RedisConfig.Addr})
	firestoreClient, err := firestore.NewClient(ctx, cfg.ProjectID)
	require.NoError(t, err, "Failed to create real Firestore client for enrichment service")

	sourceFetcher, err := device.NewGoogleDeviceMetadataFetcher(firestoreClient, cfg.CacheConfig.FirestoreConfig, logger)
	require.NoError(t, err)

	chainedFetcher, cleanup, err := device.NewChainedFetcher(ctx, &cfg.CacheConfig.RedisConfig, sourceFetcher, logger)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanup()
		redisClient.Close()
	})

	// Build the pipeline
	psClient, err := pubsub.NewClient(ctx, cfg.ProjectID)
	require.NoError(t, err)

	consumer, err := messagepipeline.NewGooglePubsubConsumer(&messagepipeline.GooglePubsubConsumerConfig{ProjectID: cfg.ProjectID, SubscriptionID: cfg.Consumer.SubscriptionID}, psClient, logger)
	require.NoError(t, err)
	producer, err := messagepipeline.NewGooglePubsubProducer[eninit.TestEnrichedMessage](psClient, cfg.Producer, logger)
	require.NoError(t, err)
	transformer := eninit.NewTestMessageEnricher(chainedFetcher, logger)
	service, err := messagepipeline.NewProcessingService(cfg.ProcessorConfig.NumWorkers, consumer, producer, transformer, logger)
	require.NoError(t, err)

	server := eninit.NewServer(cfg, service, logger)
	go func() {
		if err := server.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("Enrichment server failed: %v", err)
		}
	}()
	return service, server
}

// startEnrichedBQProcessingService starts the service that consumes enriched messages and writes them to BQ.
func startEnrichedBQProcessingService(t *testing.T, ctx context.Context, gcpProjectID, subID, datasetID, tableID string) (*messagepipeline.ProcessingService[EnrichedGardenMonitorReadings], *bqinit.Server[EnrichedGardenMonitorReadings]) {
	t.Helper()
	bqCfg := &bqinit.Config{
		LogLevel:  "info",
		HTTPPort:  ":9095",
		ProjectID: gcpProjectID,
		Consumer:  bqinit.Consumer{SubscriptionID: subID},
		BigQueryConfig: bqstore.BigQueryDatasetConfig{
			ProjectID: gcpProjectID, DatasetID: datasetID, TableID: tableID,
		},
		BatchProcessing: struct {
			bqstore.BatchInserterConfig `mapstructure:"datasetup"`
			NumWorkers                  int `mapstructure:"num_workers"`
		}{
			BatchInserterConfig: bqstore.BatchInserterConfig{BatchSize: 10, FlushTimeout: 10 * time.Second},
			NumWorkers:          2,
		},
	}

	bqLogger := log.With().Str("service", "enriched-bq-processor").Logger()
	bqClient, err := bigquery.NewClient(ctx, gcpProjectID)
	require.NoError(t, err)
	psClient, err := pubsub.NewClient(ctx, gcpProjectID)
	require.NoError(t, err)

	bqConsumer, err := messagepipeline.NewGooglePubsubConsumer(&messagepipeline.GooglePubsubConsumerConfig{
		ProjectID: bqCfg.ProjectID, SubscriptionID: bqCfg.Consumer.SubscriptionID,
	}, psClient, bqLogger)
	require.NoError(t, err)

	batchInserter, err := bqstore.NewBigQueryBatchProcessor[EnrichedGardenMonitorReadings](ctx, bqClient, &bqCfg.BatchProcessing.BatchInserterConfig, &bqCfg.BigQueryConfig, bqLogger)
	require.NoError(t, err)

	// This transformer maps the enriched Pub/Sub message to the BigQuery schema.
	transformer := func(msg types.ConsumedMessage) (*EnrichedGardenMonitorReadings, bool, error) {
		var enrichedMsg eninit.TestEnrichedMessage
		if err := json.Unmarshal(msg.Payload, &enrichedMsg); err != nil {
			return nil, false, fmt.Errorf("failed to unmarshal enriched message: %w", err)
		}

		var originalPayload types.GardenMonitorReadings
		if err := json.Unmarshal(enrichedMsg.OriginalPayload, &originalPayload); err != nil {
			return nil, false, fmt.Errorf("failed to unmarshal original payload from enriched message: %w", err)
		}

		bqRow := &EnrichedGardenMonitorReadings{
			DE:           originalPayload.DE,
			SIM:          originalPayload.SIM,
			RSSI:         originalPayload.RSSI,
			Version:      originalPayload.Version,
			Sequence:     originalPayload.Sequence,
			Battery:      originalPayload.Battery,
			Temperature:  originalPayload.Temperature,
			Humidity:     originalPayload.Humidity,
			SoilMoisture: originalPayload.SoilMoisture,
			WaterFlow:    originalPayload.WaterFlow,
			WaterQuality: originalPayload.WaterQuality,
			TankLevel:    originalPayload.TankLevel,
			AmbientLight: originalPayload.AmbientLight,
			Timestamp:    enrichedMsg.PublishTime, // Use publish time for BQ
			ClientID:     enrichedMsg.ClientID,
			LocationID:   enrichedMsg.LocationID,
			Category:     enrichedMsg.Category,
		}
		return bqRow, false, nil
	}

	// This now uses the generic NewBigQueryService
	processingService, err := bqstore.NewBigQueryService[EnrichedGardenMonitorReadings](bqCfg.BatchProcessing.NumWorkers, bqConsumer, batchInserter, transformer, bqLogger)
	require.NoError(t, err)

	bqServer := bqinit.NewServer(bqCfg, processingService, bqLogger)
	require.NoError(t, err)
	go func() {
		if srvErr := bqServer.Start(); srvErr != nil && !errors.Is(srvErr, http.ErrServerClosed) {
			t.Errorf("BQ Processing server failed: %v", srvErr)
		}
	}()
	return processingService, bqServer
}

// --- Data Seeding and Verification Helpers ---

// seedDeviceMetadata creates mock device metadata in Firestore for the test.
func seedDeviceMetadata(t *testing.T, ctx context.Context, client *firestore.Client, collection string, numDevices int) ([]string, map[string]map[string]interface{}) {
	t.Helper()
	deviceIDs := make([]string, numDevices)
	metadata := make(map[string]map[string]interface{})
	for i := 0; i < numDevices; i++ {
		deviceID := fmt.Sprintf("enriched-device-%d", i)
		deviceIDs[i] = deviceID
		docData := map[string]interface{}{
			"clientID":       fmt.Sprintf("client-%d", 100+i),
			"locationID":     fmt.Sprintf("location-%d", 200+i),
			"deviceCategory": "garden-sensor-pro",
		}
		metadata[deviceID] = docData
		_, err := client.Collection(collection).Doc(deviceID).Set(ctx, docData)
		require.NoError(t, err, "Failed to seed device metadata in Firestore")
	}
	log.Info().Int("count", numDevices).Msg("Seeded device metadata in Firestore")
	return deviceIDs, metadata
}

// verifyEnrichedData queries BigQuery and asserts that the data was enriched correctly.
func verifyEnrichedData(t *testing.T, ctx context.Context, bqClient *bigquery.Client, projectID, datasetID, tableID string, expectedMetadata map[string]map[string]interface{}) {
	t.Helper()
	log.Info().Msg("Verifying content of enriched data in BigQuery...")
	queryStr := fmt.Sprintf("SELECT uid, client_id, location_id, category FROM `%s.%s.%s` LIMIT 100", projectID, datasetID, tableID)
	query := bqClient.Query(queryStr)
	it, err := query.Read(ctx)
	require.NoError(t, err)

	rowsVerified := 0
	for {
		var row struct {
			UID      string `bigquery:"uid"`
			ClientID string `bigquery:"client_id"`
			Location string `bigquery:"location_id"`
			Category string `bigquery:"category"`
		}
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		require.NoError(t, err, "Failed to iterate BQ results")

		expected, ok := expectedMetadata[row.UID]
		require.True(t, ok, "Found a row in BQ with a UID that was not in the test seed data: %s", row.UID)

		assert.Equal(t, expected["clientID"], row.ClientID, "ClientID mismatch for UID %s", row.UID)
		assert.Equal(t, expected["locationID"], row.Location, "LocationID mismatch for UID %s", row.UID)
		assert.Equal(t, expected["deviceCategory"], row.Category, "Category mismatch for UID %s", row.UID)
		rowsVerified++
	}

	require.Greater(t, rowsVerified, 0, "Verification failed: No rows were found in the BigQuery table to verify.")
	log.Info().Int("rows_verified", rowsVerified).Msg("Successfully verified content of enriched data.")
}
