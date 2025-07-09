//go:build integration

package e2e

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/firestore"
	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-cloud-manager/pkg/servicemanager"
	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/helpers/loadgen"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
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
	if !flag.Parsed() {
		flag.Parse()
	}

	// --- Logger and Prerequisite Checks ---
	logger := zerolog.New(os.Stderr).With().Timestamp().Str("test", "TestEnrichmentBigQueryIceStoreE2E").Logger()

	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		t.Skip("Skipping E2E test: GOOGLE_CLOUD_PROJECT env var must be set.")
	}
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		logger.Warn().Msg("GOOGLE_APPLICATION_CREDENTIALS not set, relying on Application Default Credentials (ADC).")
		checkGCPAuth(t)
	}

	// --- Timing & Metrics Setup ---
	timings := make(map[string]string)
	testStart := time.Now()
	var publishedCount int
	var expectedMessageCount int
	var verifiedBQCount int
	var verifiedGCSCount int

	t.Cleanup(func() {
		timings["TotalTestDuration"] = time.Since(testStart).String()
		timings["MessagesExpected"] = strconv.Itoa(expectedMessageCount)
		timings["MessagesPublished(Actual)"] = strconv.Itoa(publishedCount)
		timings["MessagesVerified(BigQuery)"] = strconv.Itoa(verifiedBQCount)
		timings["MessagesVerified(GCS)"] = strconv.Itoa(verifiedGCSCount)

		logger.Info().Msg("\n--- Test Timing & Metrics Breakdown ---")
		for name, d := range timings {
			logger.Info().Msgf("%-35s: %s", name, d)
		}
		logger.Info().Msg("------------------------------------")
	})

	totalTestContext, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// 1. Define unique resources for this test run.
	runID := uuid.New().String()[:8]
	dataflowName := fmt.Sprintf("enrich-bq-icestore-flow-%s", runID)
	ingestionTopicID := fmt.Sprintf("ingest-topic-%s", runID)
	enrichmentSubID := fmt.Sprintf("enrich-sub-%s", runID)
	enrichedTopicID := fmt.Sprintf("enriched-output-topic-%s", runID)
	bigquerySubID := fmt.Sprintf("bq-sub-%s", runID)
	icestoreSubID := fmt.Sprintf("icestore-sub-%s", runID)
	uniqueBucketName := fmt.Sprintf("sm-icestore-bucket-%s", runID)
	uniqueDatasetID := fmt.Sprintf("dev_enriched_dataset_%s", runID)
	uniqueTableID := fmt.Sprintf("dev_enriched_payloads_%s", runID)
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
	enrichedSchemaIdentifier := "github.com/illmade-knight/go-iot-dataflows/dataflow/devflow/e2e.EnrichedTestPayload"

	servicesConfig := &servicemanager.MicroserviceArchitecture{
		Environment: servicemanager.Environment{
			Name:      "e2e-full-flow",
			ProjectID: projectID,
			Location:  "US",
		},
		Dataflows: map[string]servicemanager.ResourceGroup{
			dataflowName: {
				Name:      dataflowName,
				Lifecycle: &servicemanager.LifecyclePolicy{Strategy: lifecycleStrategy},
				Resources: servicemanager.CloudResourcesSpec{
					Topics: []servicemanager.TopicConfig{
						{CloudResource: servicemanager.CloudResource{Name: ingestionTopicID}},
						{CloudResource: servicemanager.CloudResource{Name: enrichedTopicID}},
					},
					Subscriptions: []servicemanager.SubscriptionConfig{
						{CloudResource: servicemanager.CloudResource{Name: enrichmentSubID}, Topic: ingestionTopicID},
						{CloudResource: servicemanager.CloudResource{Name: bigquerySubID}, Topic: enrichedTopicID},
						{CloudResource: servicemanager.CloudResource{Name: icestoreSubID}, Topic: ingestionTopicID},
					},
					GCSBuckets:       []servicemanager.GCSBucket{{CloudResource: servicemanager.CloudResource{Name: uniqueBucketName}, Location: "US"}},
					BigQueryDatasets: []servicemanager.BigQueryDataset{{CloudResource: servicemanager.CloudResource{Name: uniqueDatasetID}}},
					BigQueryTables: []servicemanager.BigQueryTable{
						{
							CloudResource:          servicemanager.CloudResource{Name: uniqueTableID},
							Dataset:                uniqueDatasetID,
							SchemaSourceIdentifier: enrichedSchemaIdentifier,
							ClusteringFields:       []string{"device_id"}, // makes the table clustered if present
						},
					},
				},
			},
		},
	}

	schemaRegistry := map[string]interface{}{
		enrichedSchemaIdentifier: EnrichedTestPayload{},
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

	// 4. Populate Firestore with enrichment data.
	devices, deviceToClientID, cleanupFirestore := setupEnrichmentTestData(t, totalTestContext, fsClient, firestoreCollection, runID, combinedTestNumDevices, combinedTestRate)
	t.Cleanup(cleanupFirestore)

	// 5. Start services and orchestrate resources.
	start = time.Now()
	directorService, directorURL := startServiceDirector(t, totalTestContext, logger, servicesConfig, schemaRegistry)
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
		// IMPORTANT: Always tear down resources if the test failed OR if the -keep-resources flag is false.
		if *keepResources && !t.Failed() {
			logger.Info().Msg("Test passed and -keep-resources flag is set. Cloud resources will be KEPT for inspection.")
			return // Exit the cleanup function, skipping teardown
		}

		teardownStart := time.Now()
		logger.Info().Msg("Requesting resource teardown from ServiceDirector...")
		teardownURL := directorURL + "/orchestrate/teardown"
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, teardownURL, nil)
		http.DefaultClient.Do(req)

		// Explicitly delete resources as a fallback, especially after a failed test.
		bqClient.Dataset(uniqueDatasetID).DeleteWithContents(context.Background())
		bucket := gcsClient.Bucket(uniqueBucketName)
		it := bucket.Objects(context.Background(), nil)
		for {
			attrs, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err == nil {
				bucket.Object(attrs.Name).Delete(context.Background())
			}
		}
		bucket.Delete(context.Background())
		timings["CloudResourceTeardown"] = time.Since(teardownStart).String()
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
			require.True(t, clientIDFound, "Device ID %s not found in expected map", row.DeviceID)
			require.Equal(t, expectedClientID, row.ClientID, "ClientID mismatch for device %s", row.DeviceID)
			require.Equal(t, "loc-456", row.LocationID, "LocationID mismatch for device %s", row.DeviceID)
		}
		require.Equal(t, publishedCount, rowCount, "the final number of rows in BigQuery should match the number of messages published")
		verifiedBQCount = rowCount // Update verified count for metrics
		return nil
	}
	verifyBigQueryRows(t, logger, totalTestContext, projectID, uniqueDatasetID, uniqueTableID, publishedCount, enrichedBigQueryValidator)

	// GCS verification (IceStore)
	verifyGCSResults(t, logger, totalTestContext, gcsClient, uniqueBucketName, publishedCount)
	verifiedGCSCount = publishedCount

	timings["VerificationDuration"] = time.Since(verificationStart).String()
	timings["ProcessingAndVerificationLatency"] = time.Since(loadgenStart).String()
}
