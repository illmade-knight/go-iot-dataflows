//go:build integration

package e2e

import (
	"bytes"
	"context"
	"flag" // Import the flag package
	"fmt"
	"google.golang.org/api/iterator"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage" // Import for GCS client
	"github.com/google/uuid"
	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/helpers/loadgen"
	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

const (
	generateCombinedMessagesFor = 5 * time.Second
	combinedTestNumDevices      = 3
	combinedTestRate            = 1.0
)

var (
	// keepResources is a flag to allow keeping cloud resources after the test for inspection.
	keepResources = flag.Bool("keep-resources", false, "Set to true to keep cloud resources after the test for inspection.")
)

func TestEnrichmentBigQueryIceStoreE2E(t *testing.T) {
	// Parse flags before running the test
	// This ensures the flag is available when the test runs via `go test -args -keep-resources`
	// If running directly, flags might not be parsed, so we check if already parsed.
	if !flag.Parsed() {
		flag.Parse()
	}

	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		t.Skip("Skipping E2E test: GOOGLE_CLOUD_PROJECT env var must be set.")
	}
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

	// --- Timing & Metrics Setup ---
	timings := make(map[string]string)
	testStart := time.Now()
	var publishedCount int
	var expectedMessageCount int // This will be the same for both BQ and GCS
	var verifiedBQCount int
	var verifiedGCSCount int

	t.Cleanup(func() {
		timings["TotalTestDuration"] = time.Since(testStart).String()
		timings["MessagesExpected"] = strconv.Itoa(expectedMessageCount)
		timings["MessagesPublished(Actual)"] = strconv.Itoa(publishedCount)
		timings["MessagesVerified(BigQuery)"] = strconv.Itoa(verifiedBQCount)
		timings["MessagesVerified(GCS)"] = strconv.Itoa(verifiedGCSCount)

		t.Log("\n--- Test Timing & Metrics Breakdown ---")
		for name, d := range timings {
			t.Logf("%-35s: %s", name, d)
		}
		t.Log("------------------------------------")
	})

	totalTestContext, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	logger := log.With().Str("test", "TestEnrichmentBigQueryIceStoreE2E").Logger()

	// 1. Define unique resources for this test run.
	runID := uuid.New().String()[:8]
	dataflowName := fmt.Sprintf("enrich-bq-icestore-flow-%s", runID)

	// Ingestion topic and enrichment flow
	ingestionTopicID := fmt.Sprintf("ingest-topic-%s", runID)
	enrichmentSubID := fmt.Sprintf("enrich-sub-%s", runID)
	enrichedTopicID := fmt.Sprintf("enriched-output-topic-%s", runID) // Output of enrichment

	// BigQuery specific resources
	bigquerySubID := fmt.Sprintf("bq-sub-%s", runID)
	uniqueDatasetID := fmt.Sprintf("dev_enriched_dataset_%s", runID)
	uniqueTableID := fmt.Sprintf("dev_enriched_payloads_%s", runID)

	// IceStore specific resources (receives directly from ingestion topic)
	icestoreSubID := fmt.Sprintf("icestore-sub-%s", runID)
	uniqueBucketName := fmt.Sprintf("icestore-bucket-%s", runID)

	firestoreCollection := fmt.Sprintf("devices-e2e-%s", runID)

	// Determine lifecycle strategy based on the flag
	lifecycleStrategy := servicemanager.LifecycleStrategyEphemeral
	if *keepResources {
		lifecycleStrategy = servicemanager.LifecycleStrategyPermanent
		logger.Info().Msg("Test resources will be KEPT after test execution for inspection.")
	} else {
		logger.Info().Msg("Test resources will be TORN DOWN after test execution.")
	}

	// 2. Build the services definition in memory.
	// Schema for BigQuery (enriched data)
	enrichedSchemaIdentifier := "github.com/illmade-knight/go-iot-dataflows/dataflow/devflow/e2e.EnrichedTestPayload"
	// Schema for IceStore (original data) - assuming TestPayload is the original structure
	originalSchemaIdentifier := "github.com/illmade-knight/go-iot-dataflows/dataflow/devflow/e2e.TestPayload"

	servicesConfig := &servicemanager.TopLevelConfig{
		DefaultProjectID: projectID,
		Environments: map[string]servicemanager.EnvironmentSpec{
			"dev": {ProjectID: projectID},
		},
		Dataflows: []servicemanager.ResourceGroup{{
			Name:      dataflowName,
			Lifecycle: &servicemanager.LifecyclePolicy{Strategy: lifecycleStrategy}, // Use the determined strategy
			Resources: servicemanager.ResourcesSpec{
				Topics: []servicemanager.TopicConfig{
					{Name: ingestionTopicID},
					{Name: enrichedTopicID},
				},
				Subscriptions: []servicemanager.SubscriptionConfig{
					{
						Name:            enrichmentSubID,
						Topic:           ingestionTopicID,
						ConsumerService: "",
					},
					{
						Name:            bigquerySubID,
						Topic:           enrichedTopicID, // BigQuery consumes enriched messages
						ConsumerService: "",
					},
					{
						Name:            icestoreSubID,
						Topic:           ingestionTopicID, // IceStore consumes original ingested messages
						ConsumerService: "",
					},
				},
				BigQueryDatasets: []servicemanager.BigQueryDataset{{Name: uniqueDatasetID}},
				BigQueryTables: []servicemanager.BigQueryTable{
					{
						Name:                   uniqueTableID,
						Dataset:                uniqueDatasetID,
						SchemaSourceType:       "go_struct",
						SchemaSourceIdentifier: enrichedSchemaIdentifier,
					},
				},
				GCSBuckets: []servicemanager.GCSBucket{{Name: uniqueBucketName}},
			},
		}},
	}
	servicesDef, err := servicemanager.NewInMemoryServicesDefinition(servicesConfig)
	require.NoError(t, err)

	// Define the schema registry for the director to understand the BigQuery schema
	schemaRegistry := map[string]interface{}{
		enrichedSchemaIdentifier: EnrichedTestPayload{},
		originalSchemaIdentifier: TestPayload{}, // Include original payload schema for IceStore
	}

	// 3. Setup dependencies: Emulators and Real Cloud Clients
	var opts []option.ClientOption
	if creds := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); creds != "" {
		opts = append(opts, option.WithCredentialsFile(creds))
	}
	start := time.Now()
	mqttConn := emulators.SetupMosquittoContainer(t, totalTestContext, emulators.GetDefaultMqttImageContainer())
	redisConn := emulators.SetupRedisContainer(t, totalTestContext, emulators.GetDefaultRedisImageContainer())
	timings["EmulatorSetup"] = time.Since(start).String()

	fsClient, err := firestore.NewClient(totalTestContext, projectID, opts...)
	require.NoError(t, err)
	defer fsClient.Close()

	bqClient, err := bq.NewClient(totalTestContext, projectID, opts...)
	require.NoError(t, err)
	defer bqClient.Close()

	gcsClient, err := storage.NewClient(totalTestContext, opts...)
	require.NoError(t, err)
	defer gcsClient.Close()

	// 4. Populate Firestore with enrichment data for all test devices.
	devices, deviceToClientID, cleanupFirestore := setupEnrichmentTestData(
		t,
		totalTestContext,
		fsClient,
		firestoreCollection,
		runID,
		combinedTestNumDevices,
		combinedTestRate,
	)
	t.Cleanup(cleanupFirestore)

	// 5. Start services and orchestrate resources.
	start = time.Now()
	directorService, directorURL := startServiceDirector(t, totalTestContext, logger, servicesDef, schemaRegistry)
	t.Cleanup(directorService.Shutdown)
	timings["ServiceStartup(Director)"] = time.Since(start).String()

	start = time.Now()
	setupURL := directorURL + "/orchestrate/setup"
	resp, err := http.Post(setupURL, "application/json", bytes.NewBuffer([]byte{}))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()
	timings["CloudResourceSetup(Director)"] = time.Since(start).String()

	t.Cleanup(func() {
		bucket := gcsClient.Bucket(uniqueBucketName)
		// IMPORTANT: Always tear down resources if the test failed, regardless of the keepResources flag.
		// If the test passed AND keepResources is set, then skip teardown.
		if *keepResources && !t.Failed() {
			logger.Info().Msg("Test passed and -keep-resources flag is set.")
			logger.Info().Msg("Cloud resources will be KEPT for inspection. Details below:")
			// Log the BigQuery table details
			logger.Info().Str("BigQueryDataset", uniqueDatasetID).
				Str("BigQueryTable", uniqueTableID).
				Msg(fmt.Sprintf("BigQuery Table: https://console.cloud.google.com/bigquery?project=%s&ws=!1m5!1m4!4m3!1s%s!2s%s!3s%s", projectID, projectID, uniqueDatasetID, uniqueTableID))

			// Log the GCS bucket details
			logger.Info().Str("GCSBucket", uniqueBucketName).Str("GCS", getGCSBucketURL(bucket)).
				Msg(fmt.Sprintf("GCS Bucket: https://console.cloud.google.com/storage/browser/%s/?project=%s", uniqueBucketName, projectID))

			return // Exit the cleanup function, skipping teardown
		}

		// Proceed with teardown (only if test failed OR keepResources is false)
		teardownStart := time.Now()
		if t.Failed() {
			logger.Error().Msg("Test failed. Tearing down cloud resources.")
		} else {
			logger.Info().Msg("Tearing down cloud resources as -keep-resources flag is not set.")
		}

		teardownURL := directorURL + "/orchestrate/teardown"
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, teardownURL, nil)
		if err != nil {
			logger.Warn().Err(err).Msg("Failed to create teardown request to director.")
		} else {
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				logger.Warn().Err(err).Msg("Failed to send teardown request to director.")
			} else if resp.StatusCode != http.StatusOK {
				logger.Warn().Int("status", resp.StatusCode).Msg("Director teardown request returned non-OK status.")
			}
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
		}

		// Explicitly delete BigQuery dataset and GCS bucket with contents
		ds := bqClient.Dataset(uniqueDatasetID)
		if err := ds.DeleteWithContents(context.Background()); err != nil {
			if t.Failed() {
				logger.Error().Err(err).Str("dataset", uniqueDatasetID).Msg("ERROR: Failed to delete BigQuery dataset during cleanup after test failure.")
			} else {
				logger.Warn().Err(err).Str("dataset", uniqueDatasetID).Msg("Failed to delete BigQuery dataset during cleanup.")
			}
		} else {
			logger.Info().Str("dataset", uniqueDatasetID).Msg("BigQuery dataset deleted successfully.")
		}

		if err := bucket.Delete(context.Background()); err != nil {
			if t.Failed() {
				logger.Error().Err(err).Str("bucket", uniqueBucketName).Msg("ERROR: Failed to delete GCS bucket during cleanup after test failure.")
			} else {
				logger.Warn().Err(err).Str("bucket", uniqueBucketName).Msg("Failed to delete GCS bucket during cleanup.") // Changed variable name here for consistency.
			}
		} else {
			logger.Info().Str("bucket", uniqueBucketName).Msg("GCS bucket deleted successfully.")
		}

		timings["CloudResourceTeardown(Director)"] = time.Since(teardownStart).String()
	})

	start = time.Now()
	ingestionSvc, err := startAttributeIngestionService(t, logger, directorURL, mqttConn.EmulatorAddress, projectID, ingestionTopicID, dataflowName)
	require.NoError(t, err)
	timings["ServiceStartup(Ingestion)"] = time.Since(start).String()
	t.Cleanup(ingestionSvc.Shutdown)

	start = time.Now()
	enrichmentSvc, err := startEnrichmentService(t, totalTestContext, logger, directorURL, projectID, enrichmentSubID, enrichedTopicID, redisConn.EmulatorAddress, firestoreCollection, dataflowName)
	require.NoError(t, err)
	timings["ServiceStartup(Enrichment)"] = time.Since(start).String()
	t.Cleanup(enrichmentSvc.Shutdown)

	start = time.Now()
	bqSvc, err := startEnrichedBigQueryService(t, logger, directorURL, projectID, bigquerySubID, uniqueDatasetID, uniqueTableID, dataflowName)
	require.NoError(t, err)
	timings["ServiceStartup(BigQuery)"] = time.Since(start).String()
	t.Cleanup(bqSvc.Shutdown)

	start = time.Now()
	icestoreSvc, err := startIceStoreService(t, logger, directorURL, projectID, icestoreSubID, uniqueBucketName, dataflowName)
	require.NoError(t, err)
	timings["ServiceStartup(IceStore)"] = time.Since(start).String()
	t.Cleanup(icestoreSvc.Shutdown)

	logger.Info().Msg("All services started successfully.")

	// 6. Run Load Generator
	loadgenStart := time.Now()
	logger.Info().Msg("Starting MQTT load generator...")
	generator := loadgen.NewLoadGenerator(loadgen.NewMqttClient(mqttConn.EmulatorAddress, "devices/+/data", 1, logger), devices, logger)
	expectedMessageCount = generator.ExpectedMessagesForDuration(generateCombinedMessagesFor)

	publishedCount, err = generator.Run(totalTestContext, generateCombinedMessagesFor)
	require.NoError(t, err)
	timings["LoadGeneration"] = time.Since(loadgenStart).String()
	logger.Info().Int("published_count", publishedCount).Msg("Load generator finished.")

	// 7. Verify results in BigQuery and GCS
	verificationStart := time.Now()
	logger.Info().Msg("Starting BigQuery and GCS verification...")

	// BigQuery verification
	enrichedBigQueryValidator := func(t *testing.T, iter *bq.RowIterator) error {
		var rowCount int
		for {
			var row EnrichedTestPayload
			err := iter.Next(&row)
			if err == iterator.Done {
				break
			}
			if err != nil {
				return fmt.Errorf("failed to read BigQuery row: %w", err)
			}
			rowCount++

			// Verify enrichment fields
			expectedClientID, clientIDFound := deviceToClientID[row.DeviceID]
			if !clientIDFound {
				return fmt.Errorf("device ID %s not found in expected map during BQ verification", row.DeviceID)
			}
			require.Equal(t, expectedClientID, row.ClientID, "ClientID mismatch for device %s", row.DeviceID)
			require.Equal(t, "loc-456", row.LocationID, "LocationID mismatch for device %s", row.DeviceID)
			require.Equal(t, "cat-789", row.Category, "Category mismatch for device %s", row.DeviceID)
		}
		require.Equal(t, publishedCount, rowCount, "the final number of rows in BigQuery should match the number of messages published")
		verifiedBQCount = rowCount // Update verified count for metrics
		return nil
	}
	verifyBigQueryRows(t, logger, totalTestContext, projectID, uniqueDatasetID, uniqueTableID, publishedCount, enrichedBigQueryValidator)

	// GCS verification (IceStore)
	verifyGCSResults(t, logger, totalTestContext, gcsClient, uniqueBucketName, publishedCount)
	verifiedGCSCount = publishedCount // If verifyGCSResults passes, verified count equals published count.

	timings["VerificationDuration"] = time.Since(verificationStart).String()
	timings["ProcessingAndVerificationLatency"] = time.Since(loadgenStart).String()
}
