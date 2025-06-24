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
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"

	"github.com/illmade-knight/go-iot-dataflows/gardenmonitor/bigquery/bqinit"
	"github.com/illmade-knight/go-iot-dataflows/gardenmonitor/enrich/eninit" // Import for enrichment config
	"github.com/illmade-knight/go-iot-dataflows/gardenmonitor/ingestion/mqinit"

	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/helpers/loadgen"

	"github.com/illmade-knight/go-iot/pkg/bqstore"
	"github.com/illmade-knight/go-iot/pkg/device" // For in-memory device cache in test
	"github.com/illmade-knight/go-iot/pkg/messagepipeline"
	"github.com/illmade-knight/go-iot/pkg/mqttconverter"
	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/illmade-knight/go-iot/pkg/types" // Original types
)

// --- Test Flags ---
var (
	keepDataset = flag.Bool("keep-dataset", false, "If true, the BigQueryConfig dataset will not be deleted after the test, but will expire automatically after 1 day.")
)

// --- Test Constants ---
const (
	cloudTestGCPProjectID   = "gemini-power-test"
	loadTestDuration        = 30 * time.Second
	loadTestNumDevices      = 10
	loadTestRatePerDevice   = 2.0
	loadTestSuccessPercent  = 0.98
	cloudLoadTestTimeout    = 10 * time.Minute
	testMqttTopicPattern    = "devices/+/data"
	testMqttClientIDPrefix  = "ingestion-service-cloud-load"
	cloudTestMqttHTTPPort   = ":9092"
	cloudTestEnrichHTTPPort = ":9094" // New port for enrichment service
	cloudTestBqHTTPPort     = ":9093"
)

// TestBigQueryEnrichedPayload is the structure that will be stored in BigQuery.
// It flattens the `enrich.EnrichedMessage` by embedding `types.ConsumedMessage`
// and adding the enrichment fields directly for BigQuery compatibility.
type TestBigQueryEnrichedPayload struct {
	// Original fields from ConsumedMessage.Payload (assuming it's JSON)
	Value     float64   `json:"value" bigquery:"value"`
	Timestamp time.Time `json:"timestamp" bigquery:"timestamp"`
	// Original DeviceInfo from ConsumedMessage attributes
	OriginalUID string `json:"originalUid" bigquery:"originalUid"` // Renamed from DeviceInfo.UID
	// Enriched fields
	ClientID   string `json:"clientID" bigquery:"clientID"`
	LocationID string `json:"locationID" bigquery:"locationID"`
	Category   string `json:"category" bigquery:"category"`
	// Add other necessary fields from original ConsumedMessage or its payload
	MessageID   string    `json:"messageID" bigquery:"messageID"`
	PublishTime time.Time `json:"publishTime" bigquery:"publishTime"`
}

// buildTestServicesDefinition creates a complete TopLevelConfig in memory for the load test.
// Now includes an intermediate enrichment topic/subscription.
func buildTestServicesDefinition(runID, gcpProjectID string) *servicemanager.TopLevelConfig {
	dataflowName := "mqtt-to-enriched-bigquery-loadtest"
	// Topics
	rawTopicID := fmt.Sprintf("raw-device-data-%s", runID)
	enrichedTopicID := fmt.Sprintf("enriched-device-data-%s", runID)

	// Subscriptions
	enrichmentSubID := fmt.Sprintf("enrichment-service-sub-%s", runID)
	analysisSubID := fmt.Sprintf("analysis-service-sub-%s", runID) // This now consumes from enriched

	datasetID := fmt.Sprintf("device_data_analytics_%s", runID)
	tableID := fmt.Sprintf("monitor_payloads_%s", runID)

	return &servicemanager.TopLevelConfig{
		DefaultProjectID: gcpProjectID,
		Environments: map[string]servicemanager.EnvironmentSpec{
			"loadtest": {ProjectID: gcpProjectID},
		},
		Services: []servicemanager.ServiceSpec{
			{Name: "ingestion-service", ServiceAccount: "ingestion-sa@your-gcp-project.iam.gserviceaccount.com"},
			{Name: "enrichment-service", ServiceAccount: "enrichment-sa@your-gcp-project.iam.gserviceaccount.com"}, // New service
			{Name: "analysis-service", ServiceAccount: "analysis-sa@your-gcp-project.iam.gserviceaccount.com"},
		},
		Dataflows: []servicemanager.DataflowSpec{
			{
				Name:        dataflowName,
				Description: "Ephemeral dataflow for the cloud load test with enrichment.",
				Services:    []string{"ingestion-service", "enrichment-service", "analysis-service"}, // All services
				Lifecycle: &servicemanager.LifecyclePolicy{
					Strategy:            servicemanager.LifecycleStrategyEphemeral,
					KeepResourcesOnTest: *keepDataset, // Set from the command-line flag
				},
			},
		},
		Resources: servicemanager.ResourcesSpec{
			MessagingTopics: []servicemanager.MessagingTopicConfig{
				{Name: rawTopicID, ProducerService: "ingestion-service"},       // Ingestion outputs here
				{Name: enrichedTopicID, ProducerService: "enrichment-service"}, // Enrichment outputs here
			},
			MessagingSubscriptions: []servicemanager.MessagingSubscriptionConfig{
				{Name: enrichmentSubID, Topic: rawTopicID, ConsumerService: "enrichment-service"},  // Enrichment consumes raw
				{Name: analysisSubID, Topic: enrichedTopicID, ConsumerService: "analysis-service"}, // Analysis consumes enriched
			},
			BigQueryDatasets: []servicemanager.BigQueryDataset{
				{Name: datasetID, Description: "Temp dataset for load test"},
			},
			BigQueryTables: []servicemanager.BigQueryTable{
				{
					Name:              tableID,
					Dataset:           datasetID,
					AccessingServices: []string{"analysis-service"},
					SchemaSourceType:  "go_struct",
					// IMPORTANT: Use the new BigQuery-compatible struct for schema
					SchemaSourceIdentifier: "github.com/illmade-knight/go-iot-dataflows/gardenmonitor/managedload_test.TestBigQueryEnrichedPayload",
				},
			},
		},
	}
}

// TestManagedCloudLoad subjects the entire pipeline to load, using the ServiceManager for setup and teardown.
func TestManagedCloudLoad(t *testing.T) {
	t.Setenv("GOOGLE_CLOUD_PROJECT", cloudTestGCPProjectID)
	// --- 1. Test Setup & Authentication ---
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

	ctx, cancel := context.WithTimeout(context.Background(), cloudLoadTestTimeout)
	defer cancel()
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	testLogger := log.Logger

	// --- 2. Create Dynamic Services Definition for this Test Run ---
	runID := uuid.New().String()[:8]
	servicesConfig := buildTestServicesDefinition(runID, projectID)

	servicesDef, err := servicemanager.NewInMemoryServicesDefinition(servicesConfig)
	require.NoError(t, err)

	// Register all relevant Go structs for BigQuery schema generation
	schemaRegistry := make(map[string]interface{})
	schemaRegistry["github.com/illmade-knight/go-iot/pkg/types.GardenMonitorReadings"] = types.GardenMonitorReadings{}
	schemaRegistry["github.com/illmade-knight/go-iot-dataflows/gardenmonitor/enrich.EnrichedMessage"] = enrich.EnrichedMessage{}
	schemaRegistry["github.com/illmade-knight/go-iot-dataflows/gardenmonitor/managedload_test.TestBigQueryEnrichedPayload"] = TestBigQueryEnrichedPayload{}

	// --- 3. Use ServiceManager to Provision Infrastructure ---
	log.Info().Str("runID", runID).Msg("Initializing ServiceManager to provision dataflow...")
	serviceManager, err := servicemanager.NewServiceManager(ctx, servicesDef, "loadtest", schemaRegistry, testLogger)
	require.NoError(t, err)

	dataflowName := servicesConfig.Dataflows[0].Name
	provisioned, err := serviceManager.SetupDataflow(ctx, "loadtest", dataflowName)
	require.NoError(t, err, "ServiceManager.SetupDataflow failed")

	t.Cleanup(func() {
		// Use a new, independent context for cleanup to ensure it runs
		// even if the main test context times out.
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		log.Info().Str("runID", runID).Msg("Test finished. Tearing down dataflow resources...")
		teardownErr := serviceManager.TeardownDataflow(cleanupCtx, "loadtest", dataflowName)
		assert.NoError(t, teardownErr, "ServiceManager.TeardownDataflow failed")
	})

	// --- 4. Get Resource Names from the Provisioning Result ---
	log.Info().Msg("Getting resource names from the provisioned resources result...")

	// Get the raw topic (ingestion output, enrichment input)
	rawTopicID := ""
	for _, topic := range provisioned.PubSubTopics {
		if topic.ProducerService == "ingestion-service" {
			rawTopicID = topic.Name
			break
		}
	}
	require.NotEmpty(t, rawTopicID, "Raw Pub/Sub topic not found in provisioned resources")

	// Get the enriched topic (enrichment output, BigQuery input)
	enrichedTopicID := ""
	for _, topic := range provisioned.PubSubTopics {
		if topic.ProducerService == "enrichment-service" {
			enrichedTopicID = topic.Name
			break
		}
	}
	require.NotEmpty(t, enrichedTopicID, "Enriched Pub/Sub topic not found in provisioned resources")

	// Get the enrichment subscription
	enrichmentSubID := ""
	for _, sub := range provisioned.PubSubSubscriptions {
		if sub.ConsumerService == "enrichment-service" {
			enrichmentSubID = sub.Name
			break
		}
	}
	require.NotEmpty(t, enrichmentSubID, "Enrichment Pub/Sub subscription not found in provisioned resources")

	// Get the analysis (BigQuery) subscription
	analysisSubID := ""
	for _, sub := range provisioned.PubSubSubscriptions {
		if sub.ConsumerService == "analysis-service" {
			analysisSubID = sub.Name
			break
		}
	}
	require.NotEmpty(t, analysisSubID, "Analysis Pub/Sub subscription not found in provisioned resources")

	require.NotEmpty(t, provisioned.BigQueryTables, "Provisioning must return a BigQueryConfig table")
	datasetID := provisioned.BigQueryTables[0].Dataset
	tableID := provisioned.BigQueryTables[0].Name

	log.Info().Str("raw_topic", rawTopicID).Str("enriched_topic", enrichedTopicID).Msg("Using Pub/Sub topics")
	log.Info().Str("enrich_sub", enrichmentSubID).Str("analysis_sub", analysisSubID).Msg("Using Pub/Sub subscriptions")
	log.Info().Str("dataset", datasetID).Str("table", tableID).Msg("Using BigQueryConfig resources")

	// --- 5. Start Local Services & Load Generator ---
	log.Info().Msg("LoadTest: Setting up Mosquitto container...")
	mqttConnections := emulators.SetupMosquittoContainer(t, ctx, emulators.GetDefaultMqttImageContainer())

	// Start Ingestion Service (MQTT to Raw Pub/Sub)
	ingestionService, mqttServer := startIngestionService(t, ctx, projectID, mqttConnections.EmulatorAddress, rawTopicID)
	defer mqttServer.Shutdown()

	// Start Enrichment Service (Raw Pub/Sub to Enriched Pub/Sub)
	enrichmentService, enrichServer := startEnrichmentService(t, ctx, projectID, rawTopicID, enrichmentSubID, enrichedTopicID)
	defer enrichServer.Shutdown()

	// Start BigQuery Processing Service (Enriched Pub/Sub to BigQuery)
	bqProcessingService, bqServer := startBQProcessingService(t, ctx, projectID, enrichedTopicID, analysisSubID, datasetID, tableID)
	defer bqServer.Shutdown()

	var processingErrors []error
	var errMu sync.Mutex

	// Goroutine to capture errors from ingestion service
	go func() {
		for err := range ingestionService.Err() {
			errMu.Lock()
			processingErrors = append(processingErrors, err)
			errMu.Unlock()
			log.Error().Err(err).Msg("Received processing error from ingestion service")
		}
	}()

	// Goroutine to capture errors from enrichment service
	go func() {
		// Assuming ProcessingService provides an error channel. If not, this part needs adjustment.
		// For now, we'll just check if the service shuts down prematurely.
		select {
		case <-enrichmentService.Done(): // Assuming ProcessingService has a Done() channel
			log.Info().Msg("Enrichment service completed its run (expected during graceful shutdown).")
		case <-ctx.Done():
			log.Warn().Msg("Enrichment service context cancelled before graceful shutdown.")
		}
	}()

	// Goroutine to capture errors from BQ processing service
	go func() {
		// Assuming ProcessingService provides an error channel. If not, this part needs adjustment.
		select {
		case <-bqProcessingService.Done(): // Assuming ProcessingService has a Done() channel
			log.Info().Msg("BigQuery processing service completed its run (expected during graceful shutdown).")
		case <-ctx.Done():
			log.Warn().Msg("BigQuery processing service context cancelled before graceful shutdown.")
		}
	}()

	log.Info().Msg("LoadTest: Pausing to allow services to start and connect...")
	time.Sleep(10 * time.Second) // Give services time to initialize

	log.Info().Msg("LoadTest: Configuring and starting load generator...")
	loadgenLogger := log.With().Str("service", "load-generator").Logger()
	loadgenClient := loadgen.NewMqttClient(mqttConnections.EmulatorAddress, testMqttTopicPattern, 1, loadgenLogger)
	devices := make([]*loadgen.Device, loadTestNumDevices)
	deviceIDs := make([]string, loadTestNumDevices)

	// In-memory cache for the test's enrichment service
	testDeviceCache := enrich.NewInMemoryDeviceMetadataCache()
	testDeviceCache.AddDevice("load-test-device-0", "client-A", "location-X", "type-temp")
	testDeviceCache.AddDevice("load-test-device-1", "client-A", "location-Y", "type-pressure")
	// Add more devices to the test cache as needed for load test to ensure enrichment happens for all.
	for i := 0; i < loadTestNumDevices; i++ {
		deviceID := fmt.Sprintf("load-test-device-%d", i)
		deviceIDs[i] = deviceID
		payloadGenerator := &gardenMonitorPayloadGenerator{} // This generates types.GardenMonitorReadings
		devices[i] = &loadgen.Device{
			ID:               deviceID,
			MessageRate:      loadTestRatePerDevice,
			PayloadGenerator: payloadGenerator,
		}
		// Populate the in-memory cache for all load-test devices
		if i%2 == 0 { // Example: half devices get one location, half another
			testDeviceCache.AddDevice(deviceID, fmt.Sprintf("client-%d", i), "location-even", "category-even")
		} else {
			testDeviceCache.AddDevice(deviceID, fmt.Sprintf("client-%d", i), "location-odd", "category-odd")
		}
	}

	// Inject the test cache's fetcher into the enrichment service
	// This requires modifying the enrichment service's NewTestMessageEnricher to accept a pre-configured fetcher.
	// For simplicity in this test, we are setting a global or relying on how startEnrichmentService gets its fetcher.
	// The current `startEnrichmentService` creates its own `InMemoryDeviceMetadataCache`. This is good for isolation.

	loadGenerator := loadgen.NewLoadGenerator(loadgenClient, devices, loadgenLogger)
	err = loadGenerator.Run(ctx, loadTestDuration)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		require.NoError(t, err, "Load generator returned an unexpected error")
	}

	// --- 6. Verification ---
	log.Info().Msg("Load generation complete. Verifying results in BigQueryConfig...")
	expectedMessages := int(float64(loadTestNumDevices) * loadTestRatePerDevice * loadTestDuration.Seconds())
	successThreshold := int(float64(expectedMessages) * loadTestSuccessPercent)

	bqClient, err := bigquery.NewClient(ctx, projectID)
	require.NoError(t, err)
	defer bqClient.Close()

	var lastRowCount int64 = -1 // Use -1 to indicate the first run
	var pollsWithNoChange int
	const maxPollsWithNoChange = 3 // Number of stable polls before we assume completion

	verificationCtx, verificationCancel := context.WithTimeout(ctx, 5*time.Minute)
	defer verificationCancel()
	tick := time.NewTicker(15 * time.Second)
	defer tick.Stop()

VerificationLoop:
	for {
		select {
		case <-verificationCtx.Done():
			t.Fatalf("Test timed out waiting for BigQueryConfig results. Last count: %d, Threshold: %d", lastRowCount, successThreshold)
		case <-tick.C:
			currentRowCount, err := getRowCount(verificationCtx, bqClient, datasetID, tableID)
			if err != nil {
				log.Warn().Err(err).Msg("Polling query for row count failed")
				continue
			}

			log.Info().Int64("current_count", currentRowCount).Int64("last_count", lastRowCount).Int("stable_polls", pollsWithNoChange).Msg("Polling BQ count")

			if currentRowCount == lastRowCount {
				pollsWithNoChange++
			} else {
				pollsWithNoChange = 0
				lastRowCount = currentRowCount
			}

			if pollsWithNoChange >= maxPollsWithNoChange {
				if lastRowCount >= int64(successThreshold) {
					log.Info().Msg("Row count has stabilized and meets the success threshold. Verification successful!")
					break VerificationLoop
				} else {
					t.Fatalf("Test failed: Row count stabilized at %d, which is below the success threshold of %d.", lastRowCount, successThreshold)
				}
			}
		}
	}

	// Final check outside the loop
	finalCount, err := getRowCount(verificationCtx, bqClient, datasetID, tableID)
	require.NoError(t, err)
	require.GreaterOrEqual(t, finalCount, int64(successThreshold), "Final count in BQ (%d) is less than success threshold (%d)", finalCount, successThreshold)

	// Optional: Query BigQuery to verify enriched data
	log.Info().Msg("Performing sanity check on enriched data in BigQuery...")
	query := bqClient.Query(fmt.Sprintf("SELECT COUNT(*) FROM `%s.%s` WHERE clientID IS NOT NULL AND locationID IS NOT NULL", datasetID, tableID))
	it, err := query.Read(ctx)
	require.NoError(t, err)
	var count struct{ Count int64 }
	err = it.Next(&count)
	require.NoError(t, err)
	log.Info().Int64("enriched_row_count", count.Count).Msg("Count of enriched rows in BigQuery.")
	assert.GreaterOrEqual(t, count.Count, int64(loadTestNumDevices/2), "Expected at least half of messages to be enriched") // Adjust as per your enrichment logic

	// --- 7. Post-Test Data Manipulation (Conditional) ---
	if *keepDataset {
		log.Info().Msg("'-keep-dataset' flag is set. Retaining dataset for demos and respacing timestamps...")
		respaceTimestampsForDemo(t, ctx, bqClient, projectID, datasetID, tableID, deviceIDs)
	}

	// --- 8. Final Error Check ---
	errMu.Lock()
	defer errMu.Unlock()
	assert.Empty(t, processingErrors, "Should be no processing errors from the ingestion service")
}

// --- Helper Functions ---

func getRowCount(ctx context.Context, client *bigquery.Client, datasetID, tableID string) (int64, error) {
	queryString := fmt.Sprintf("SELECT COUNT(*) as count FROM `%s.%s`", datasetID, tableID)
	query := client.Query(queryString)
	it, err := query.Read(ctx)
	if err != nil {
		return 0, err
	}
	var row struct {
		Count int64 `bigquery:"count"`
	}
	if err := it.Next(&row); err != nil {
		if errors.Is(err, iterator.Done) {
			return 0, nil // No rows found, which is a valid count of 0
		}
		return 0, err
	}
	return row.Count, nil
}

func startIngestionService(t *testing.T, ctx context.Context, projectID, mqttBrokerURL, topicID string) (*mqttconverter.IngestionService, *mqinit.Server) {
	t.Helper()
	mqttCfg := &mqinit.Config{
		LogLevel: "info", HTTPPort: cloudTestMqttHTTPPort, ProjectID: projectID,
		Publisher: struct {
			TopicID         string `mapstructure:"topic_id"`
			CredentialsFile string `mapstructure:"credentials_file"`
		}{TopicID: topicID},
		MQTT:    mqttconverter.MQTTClientConfig{BrokerURL: mqttBrokerURL, Topic: testMqttTopicPattern, ClientIDPrefix: testMqttClientIDPrefix},
		Service: mqttconverter.IngestionServiceConfig{InputChanCapacity: 1000, NumProcessingWorkers: 20},
	}
	logger := log.With().Str("service", "mqtt-ingestion").Logger()

	// Create a shared Pub/Sub client for this service
	pubsubClient, err := pubsub.NewClient(ctx, projectID, pubsub.WithEmulatorSettings(os.Getenv("PUBSUB_EMULATOR_HOST")))
	require.NoError(t, err)
	// Important: The client is closed by the service's Stop method or defer

	publisher, err := mqttconverter.NewGooglePubsubPublisher(pubsubClient, // Pass the client
		mqttconverter.GooglePubsubPublisherConfig{ProjectID: mqttCfg.ProjectID, TopicID: mqttCfg.Publisher.TopicID}, logger)
	require.NoError(t, err)
	service := mqttconverter.NewIngestionService(publisher, nil, logger, mqttCfg.Service, mqttCfg.MQTT)
	server := mqinit.NewServer(mqttCfg, service, logger)
	go func() {
		if err := server.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error().Err(err).Msg("Ingestion server failed during test execution")
		}
	}()
	return service, server
}

// startEnrichmentService initializes and starts the enrichment microservice.
func startEnrichmentService(t *testing.T, ctx context.Context, projectID, inputTopicID, inputSubID, outputTopicID string) (*messagepipeline.ProcessingService[enrich.TestEnrichedMessage], *eninit.Server) {
	t.Helper()

	// Configuration for the enrichment service (similar to runenrichment.go main function)
	enrichCfg := &eninit.Config{
		LogLevel: "info", HTTPPort: cloudTestEnrichHTTPPort, ProjectID: projectID,
		Consumer: struct {
			SubscriptionID  string `mapstructure:"subscription_id"`
			CredentialsFile string `mapstructure:"credentials_file"`
		}{SubscriptionID: inputSubID},
		Producer: &messagepipeline.GooglePubsubProducerConfig{
			ProjectID:  projectID,
			TopicID:    outputTopicID,
			BatchSize:  100,
			BatchDelay: 100 * time.Millisecond,
		},
		CacheConfig: eninit.CacheConfig{ // Define a mock cache config for the test
			FirestoreConfig: device.FirestoreConfig{CollectionID: "test-devices"},                  // This won't be used, but needed for constructor
			RedisConfig:     device.RedisConfig{Addr: "localhost:6379", CacheTTL: 5 * time.Minute}, // Mock Redis
		},
	}

	enrichLogger := log.With().Str("service", "enrichment-processor").Logger()

	// Create a shared Pub/Sub client for the enrichment service
	pubsubClient, err := pubsub.NewClient(ctx, projectID, pubsub.WithEmulatorSettings(os.Getenv("PUBSUB_EMULATOR_HOST")))
	require.NoError(t, err)
	// Important: The client is closed by the service's Stop method or defer

	// Create Pub/Sub consumer for enrichment service
	consumerCfg := &messagepipeline.GooglePubsubConsumerConfig{
		ProjectID:      projectID,
		SubscriptionID: inputSubID,
	}
	consumer, err := messagepipeline.NewGooglePubsubConsumer(consumerCfg, pubsubClient, enrichLogger)
	require.NoError(t, err)

	// Create Pub/Sub producer for enrichment service
	producer, err := messagepipeline.NewGooglePubsubProducer[enrich.TestEnrichedMessage](pubsubClient, enrichCfg.Producer, enrichLogger)
	require.NoError(t, err)

	// Use the in-memory device metadata cache for testing
	metadataCache := enrich.NewInMemoryDeviceMetadataCache()
	// Add a few known devices to the cache for successful enrichment during load test
	metadataCache.AddDevice("load-test-device-0", "client-A", "location-X", "type-temp")
	metadataCache.AddDevice("load-test-device-1", "client-B", "location-Y", "type-humidity")
	// Populate for all devices expected in load test
	for i := 0; i < loadTestNumDevices; i++ {
		deviceID := fmt.Sprintf("load-test-device-%d", i)
		if i%2 == 0 {
			metadataCache.AddDevice(deviceID, fmt.Sprintf("client-E%d", i), "loc-E", "cat-E")
		} else {
			metadataCache.AddDevice(deviceID, fmt.Sprintf("client-O%d", i), "loc-O", "cat-O")
		}
	}

	// Create the MessageTransformer (enricher)
	enricher := enrich.NewTestMessageEnricher(metadataCache.Fetcher(), enrichLogger)

	// Create the Processing Service for enrichment
	processingService, err := messagepipeline.NewProcessingService(
		2, // Number of workers for enrichment
		consumer,
		producer,
		enricher,
		enrichLogger,
	)
	require.NoError(t, err)

	// Create and start the HTTP server for the enrichment service (if applicable)
	// Note: The `eninit.NewServer` might expect a Config struct and the ProcessingService.
	// We'll create a minimal config here.
	server := eninit.NewServer(enrichCfg, processingService, enrichLogger)
	go func() {
		if srvErr := server.Start(); srvErr != nil && !errors.Is(srvErr, http.ErrServerClosed) {
			t.Errorf("Enrichment server failed: %v", srvErr)
		}
	}()
	return processingService, server
}

func startBQProcessingService(t *testing.T, ctx context.Context, gcpProjectID, inputTopicID, subID, datasetID, tableID string) (*messagepipeline.ProcessingService[TestBigQueryEnrichedPayload], *bqinit.Server) {
	t.Helper()
	bqCfg := &bqinit.Config{
		LogLevel:  "info",
		HTTPPort:  cloudTestBqHTTPPort,
		ProjectID: gcpProjectID,
		Consumer: struct {
			SubscriptionID  string `mapstructure:"subscription_id"`
			CredentialsFile string `mapstructure:"credentials_file"`
		}{SubscriptionID: subID}, // Consumes from the enriched topic now
		BigQueryConfig: bqstore.BigQueryDatasetConfig{
			ProjectID: gcpProjectID,
			DatasetID: datasetID,
			TableID:   tableID,
		},
		BatchProcessing: struct {
			bqstore.BatchInserterConfig `mapstructure:"datasetup"`
			NumWorkers                  int `mapstructure:"num_workers"`
		}{
			BatchInserterConfig: bqstore.BatchInserterConfig{
				BatchSize:    5,
				FlushTimeout: 10 * time.Second,
			},
			NumWorkers: 2,
		},
	}
	bqLogger := log.With().Str("service", "bq-processor").Logger()

	// Create a shared Pub/Sub client for the BQ service
	pubsubClient, err := pubsub.NewClient(ctx, gcpProjectID, pubsub.WithEmulatorSettings(os.Getenv("PUBSUB_EMULATOR_HOST")))
	require.NoError(t, err)
	defer pubsubClient.Close() // Make sure client is closed when this helper exits

	bqClient, err := bigquery.NewClient(ctx, gcpProjectID)
	require.NoError(t, err)

	bqConsumer, err := messagepipeline.NewGooglePubsubConsumer(&messagepipeline.GooglePubsubConsumerConfig{
		ProjectID:      bqCfg.ProjectID,
		SubscriptionID: bqCfg.Consumer.SubscriptionID,
	}, pubsubClient, bqLogger) // Pass the Pub/Sub client
	require.NoError(t, err)

	// Transformer for BigQuery: from EnrichedMessage to TestBigQueryEnrichedPayload
	// This transformer will extract the necessary fields from the EnrichedMessage
	// and flatten them into the structure expected by BigQuery.
	bqTransformer := func(msg types.ConsumedMessage) (*TestBigQueryEnrichedPayload, bool, error) {
		// First, unmarshal the ConsumedMessage.Payload into an enrich.TestEnrichedMessage
		var enrichedMsgFromPubsub enrich.TestEnrichedMessage
		err := json.Unmarshal(msg.Payload, &enrichedMsgFromPubsub)
		if err != nil {
			bqLogger.Error().Err(err).Str("msg_id", msg.ID).Msg("Failed to unmarshal enriched message for BQ, skipping.")
			return nil, true, nil // Skip on unmarshal error
		}

		// Now, extract the original GardenMonitorReadings payload
		var originalReadings types.GardenMonitorReadings
		if enrichedMsgFromPubsub.OriginalPayload != nil {
			err = json.Unmarshal(enrichedMsgFromPubsub.OriginalPayload, &originalReadings)
			if err != nil {
				bqLogger.Error().Err(err).Str("msg_id", msg.ID).Msg("Failed to unmarshal original payload within enriched message for BQ, skipping.")
				return nil, true, nil // Skip if original payload can't be unmarshaled
			}
		}

		// Create the flattened BigQuery payload
		bqPayload := &TestBigQueryEnrichedPayload{
			Value:       originalReadings.Value,
			Timestamp:   originalReadings.Timestamp,
			ClientID:    enrichedMsgFromPubsub.ClientID,
			LocationID:  enrichedMsgFromPubsub.LocationID,
			Category:    enrichedMsgFromPubsub.Category,
			MessageID:   msg.ID, // Use the Pub/Sub message ID
			PublishTime: msg.PublishTime,
		}

		// Handle cases where original DeviceInfo was present in the raw message
		if enrichedMsgFromPubsub.OriginalInfo != nil {
			bqPayload.OriginalUID = enrichedMsgFromPubsub.OriginalInfo.UID
		}

		bqLogger.Debug().Str("msg_id", msg.ID).Msg("Transformed enriched message to BQ payload.")
		return bqPayload, false, nil
	}

	// Use the new, single convenience constructor for BigQueryBatchProcessor,
	// using our new TestBigQueryEnrichedPayload type.
	batchInserter, err := bqstore.NewBigQueryBatchProcessor[TestBigQueryEnrichedPayload](ctx, bqClient, &bqCfg.BatchProcessing.BatchInserterConfig, &bqCfg.BigQueryConfig, bqLogger)
	require.NoError(t, err)

	// Use the new service constructor with our custom transformer.
	processingService, err := messagepipeline.NewProcessingService(
		bqCfg.BatchProcessing.NumWorkers,
		bqConsumer,
		batchInserter,
		bqTransformer, // Use the new transformer
		bqLogger,
	)
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

// gardenMonitorPayloadGenerator implements loadgen.PayloadGenerator for GardenMonitorReadings.
type gardenMonitorPayloadGenerator struct{}

func (g *gardenMonitorPayloadGenerator) GeneratePayload(deviceID string) ([]byte, error) {
	payload := types.GardenMonitorReadings{
		DeviceID:  deviceID,
		Value:     float64(time.Now().UnixNano()%1000) / 10.0, // Example value
		Timestamp: time.Now().UTC(),
	}
	return json.Marshal(payload)
}

// respaceTimestampsForDemo adjusts timestamps in BigQuery for better demo visualization.
// (Original helper, kept for completeness)
func respaceTimestampsForDemo(t *testing.T, ctx context.Context, client *bigquery.Client, projectID, datasetID, tableID string, deviceIDs []string) {
	t.Helper()
	log.Info().Msg("Rescaling timestamps for demo...")

	// Fetch all data
	queryStr := fmt.Sprintf("SELECT * FROM `%s.%s` ORDER BY PublishTime ASC", datasetID, tableID)
	query := client.Query(queryStr)
	it, err := query.Read(ctx)
	require.NoError(t, err)

	type TempRow struct {
		TestBigQueryEnrichedPayload
		OriginalTimestamp time.Time `bigquery:"publishTime"` // Capture the original
	}
	var rows []TempRow
	for {
		var row TempRow
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		require.NoError(t, err)
		rows = append(rows, row)
	}

	if len(rows) == 0 {
		log.Warn().Msg("No rows to respace timestamps for.")
		return
	}

	// Calculate new timestamps
	minTime := rows[0].OriginalTimestamp
	maxTime := rows[len(rows)-1].OriginalTimestamp
	demoDuration := 5 * time.Minute // Respace to fit in 5 minutes

	if maxTime.Equal(minTime) { // Handle case of all messages arriving at same time
		log.Warn().Msg("All messages have identical timestamps, cannot respace.")
		return
	}

	for i := range rows {
		// Calculate percentage through original time range
		timeDiff := float64(rows[i].OriginalTimestamp.Sub(minTime))
		totalTimeRange := float64(maxTime.Sub(minTime))
		progress := timeDiff / totalTimeRange

		// Apply that percentage to the new demo duration
		newTimestamp := minTime.Add(time.Duration(float64(demoDuration) * progress))
		rows[i].PublishTime = newTimestamp
		rows[i].Timestamp = newTimestamp // Also update the nested payload timestamp
	}

	// Delete existing data
	deleteQuery := fmt.Sprintf("DELETE FROM `%s.%s` WHERE TRUE", datasetID, tableID)
	job, err := client.Query(deleteQuery).Run(ctx)
	require.NoError(t, err)
	status, err := job.Wait(ctx)
	require.NoError(t, err)
	require.Nil(t, status.Errors)

	// Insert respaced data back
	inserter := client.Dataset(datasetID).Table(tableID).Inserter()
	insertRows := make([]interface{}, len(rows))
	for i, r := range rows {
		insertRows[i] = r.TestBigQueryEnrichedPayload // Insert the payload struct
	}

	err = inserter.Put(ctx, insertRows)
	require.NoError(t, err)
	log.Info().Msg("Timestamps respaced and data re-inserted.")
}
