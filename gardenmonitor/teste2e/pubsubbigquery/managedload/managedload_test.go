//go:build integration

package managedload_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
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

	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/helpers/loadgen"

	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/illmade-knight/go-iot/pkg/types"
)

// --- Test Flags ---
var (
	keepDataset = flag.Bool("keep-dataset", false, "If true, the BigQuery dataset will not be deleted after the test, but will expire automatically after 1 day.")
)

// --- Test Constants ---
const (
	cloudTestGCPProjectID  = "gemini-power-test"
	loadTestDuration       = 30 * time.Second
	loadTestNumDevices     = 10
	loadTestRatePerDevice  = 2.0
	loadTestSuccessPercent = 0.98
	cloudLoadTestTimeout   = 10 * time.Minute
	testMqttTopicPattern   = "devices/+/data"
	testMqttClientIDPrefix = "ingestion-service-cloud-load"
	cloudTestMqttHTTPPort  = ":9092"
	cloudTestBqHTTPPort    = ":9093"
)

// buildTestServicesDefinition creates a complete TopLevelConfig in memory for the load test,
// following the new ResourceGroup structure.
func buildTestServicesDefinition(runID, gcpProjectID string) *servicemanager.TopLevelConfig {
	dataflowName := "mqtt-to-bigquery-loadtest"
	topicID := fmt.Sprintf("processed-device-data-%s", runID)
	subscriptionID := fmt.Sprintf("analysis-service-sub-%s", runID)
	datasetID := fmt.Sprintf("device_data_analytics_%s", runID)
	tableID := fmt.Sprintf("monitor_payloads_%s", runID)

	return &servicemanager.TopLevelConfig{
		DefaultProjectID: gcpProjectID,
		Environments: map[string]servicemanager.EnvironmentSpec{
			"loadtest": {ProjectID: gcpProjectID},
		},
		Services: []servicemanager.ServiceSpec{
			{Name: "ingestion-service", ServiceAccount: "ingestion-sa@your-gcp-project.iam.gserviceaccount.com"},
			{Name: "analysis-service", ServiceAccount: "analysis-sa@your-gcp-project.iam.gserviceaccount.com"},
		},
		Dataflows: []servicemanager.ResourceGroup{
			{
				Name:         dataflowName,
				Description:  "Ephemeral dataflow for the cloud load test.",
				ServiceNames: []string{"ingestion-service", "analysis-service"},
				Lifecycle: &servicemanager.LifecyclePolicy{
					Strategy:            servicemanager.LifecycleStrategyEphemeral,
					KeepResourcesOnTest: *keepDataset,
				},
				Resources: servicemanager.ResourcesSpec{
					Topics: []servicemanager.TopicConfig{
						{Name: topicID, ProducerService: "ingestion-service"},
					},
					Subscriptions: []servicemanager.SubscriptionConfig{
						{Name: subscriptionID, Topic: topicID, ConsumerService: "analysis-service"},
					},
					BigQueryDatasets: []servicemanager.BigQueryDataset{
						{Name: datasetID, Description: "Temp dataset for load test"},
					},
					BigQueryTables: []servicemanager.BigQueryTable{
						{
							Name:                   tableID,
							Dataset:                datasetID,
							AccessingServices:      []string{"analysis-service"},
							SchemaSourceType:       "go_struct",
							SchemaSourceIdentifier: "github.com/illmade-knight/go-iot/pkg/types.GardenMonitorReadings",
						},
					},
				},
			},
		},
	}
}

// TestManagedCloudLoad subjects the entire pipeline to load, using the ServiceManager for setup and teardown.
func TestManagedCloudLoad(t *testing.T) {
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

	schemaRegistry := make(map[string]interface{})
	schemaRegistry["github.com/illmade-knight/go-iot/pkg/types.GardenMonitorReadings"] = types.GardenMonitorReadings{}

	// --- 3. Use ServiceManager to Provision Infrastructure ---
	log.Info().Str("runID", runID).Msg("Initializing ServiceManager to provision dataflow...")
	serviceManager, err := servicemanager.NewServiceManager(ctx, servicesDef, "loadtest", schemaRegistry, testLogger)
	require.NoError(t, err)

	dataflowName := servicesConfig.Dataflows[0].Name
	_, err = serviceManager.SetupDataflow(ctx, "loadtest", dataflowName)
	require.NoError(t, err, "ServiceManager.SetupDataflow failed")

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		log.Info().Str("runID", runID).Msg("Test finished. Tearing down dataflow resources...")
		teardownErr := serviceManager.TeardownDataflow(cleanupCtx, "loadtest", dataflowName)
		assert.NoError(t, teardownErr, "ServiceManager.TeardownDataflow failed")
	})

	// --- 4. Get Resource Names from the Service Definition ---
	log.Info().Msg("Getting resource names from the service definition...")
	dataflowSpec, err := servicesDef.GetDataflow(dataflowName)
	require.NoError(t, err)

	require.NotEmpty(t, dataflowSpec.Resources.Topics, "Definition must contain a Pub/Sub topic")
	topicID := dataflowSpec.Resources.Topics[0].Name

	require.NotEmpty(t, dataflowSpec.Resources.Subscriptions, "Definition must contain a Pub/Sub subscription")
	subscriptionID := dataflowSpec.Resources.Subscriptions[0].Name

	require.NotEmpty(t, dataflowSpec.Resources.BigQueryTables, "Definition must contain a BigQuery table")
	datasetID := dataflowSpec.Resources.BigQueryTables[0].Dataset
	tableID := dataflowSpec.Resources.BigQueryTables[0].Name

	log.Info().Str("topic", topicID).Str("subscription", subscriptionID).Msg("Using Pub/Sub resources")
	log.Info().Str("dataset", datasetID).Str("table", tableID).Msg("Using BigQuery resources")

	// --- 5. Start Local Services & Load Generator ---
	log.Info().Msg("LoadTest: Setting up Mosquitto container...")
	mqttConnections := emulators.SetupMosquittoContainer(t, ctx, emulators.GetDefaultMqttImageContainer())

	ingestionService, mqttServer := startIngestionService(t, ctx, projectID, mqttConnections.EmulatorAddress, topicID)
	defer mqttServer.Shutdown()

	var processingErrors []error
	var errMu sync.Mutex
	go func() {
		for err := range ingestionService.Err() {
			errMu.Lock()
			processingErrors = append(processingErrors, err)
			errMu.Unlock()
			log.Error().Err(err).Msg("Received processing error from ingestion service")
		}
	}()

	_, bqServer := startBQProcessingService(t, ctx, projectID, subscriptionID, datasetID, tableID)
	defer bqServer.Shutdown()

	log.Info().Msg("LoadTest: Pausing to allow services to start and connect...")
	time.Sleep(10 * time.Second)

	log.Info().Msg("LoadTest: Configuring and starting load generator...")
	loadgenLogger := log.With().Str("service", "load-generator").Logger()
	loadgenClient := loadgen.NewMqttClient(mqttConnections.EmulatorAddress, testMqttTopicPattern, 1, loadgenLogger)
	devices := make([]*loadgen.Device, loadTestNumDevices)
	deviceIDs := make([]string, loadTestNumDevices)
	for i := 0; i < loadTestNumDevices; i++ {
		deviceID := fmt.Sprintf("load-test-device-%d", i)
		deviceIDs[i] = deviceID
		payloadGenerator := &gardenMonitorPayloadGenerator{}
		devices[i] = &loadgen.Device{
			ID:               deviceID,
			MessageRate:      loadTestRatePerDevice,
			PayloadGenerator: payloadGenerator,
		}
	}

	loadGenerator := loadgen.NewLoadGenerator(loadgenClient, devices, loadgenLogger)
	err = loadGenerator.Run(ctx, loadTestDuration)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		require.NoError(t, err, "Load generator returned an unexpected error")
	}

	// --- 6. Verification ---
	log.Info().Msg("Load generation complete. Verifying results in BigQuery...")
	expectedMessages := int(float64(loadTestNumDevices) * loadTestRatePerDevice * loadTestDuration.Seconds())
	successThreshold := int(float64(expectedMessages) * loadTestSuccessPercent)

	bqClient, err := bigquery.NewClient(ctx, projectID)
	require.NoError(t, err)
	defer bqClient.Close()

	var lastRowCount int64 = -1
	var pollsWithNoChange int
	const maxPollsWithNoChange = 3

	verificationCtx, verificationCancel := context.WithTimeout(ctx, 5*time.Minute)
	defer verificationCancel()
	tick := time.NewTicker(15 * time.Second)
	defer tick.Stop()

VerificationLoop:
	for {
		select {
		case <-verificationCtx.Done():
			t.Fatalf("Test timed out waiting for BigQuery results. Last count: %d, Threshold: %d", lastRowCount, successThreshold)
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

	finalCount, err := getRowCount(verificationCtx, bqClient, datasetID, tableID)
	require.NoError(t, err)
	require.GreaterOrEqual(t, finalCount, int64(successThreshold), "Final count in BQ (%d) is less than success threshold (%d)", finalCount, successThreshold)

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
