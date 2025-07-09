//go:build integration

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/firestore"
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
	generateEnrichedBigqueryMessagesFor = 5 * time.Second
	enrichmentBigQueryTestNumDevices    = 3
	enrichmentBigQueryTestRate          = 1.0
)

func TestEnrichmentToBigQueryE2E(t *testing.T) {
	logger := zerolog.New(os.Stderr).
		With().Timestamp().Str("test", "TestEnrichmentToBigQueryE2E").Logger()

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

	t.Cleanup(func() {
		timings["TotalTestDuration"] = time.Since(testStart).String()
		timings["MessagesExpected"] = strconv.Itoa(expectedMessageCount)
		timings["MessagesPublished(Actual)"] = strconv.Itoa(publishedCount)
		timings["MessagesVerified(Actual)"] = strconv.Itoa(publishedCount)

		t.Log("\n--- Test Timing & Metrics Breakdown ---")
		for name, d := range timings {
			t.Logf("%-35s: %s", name, d)
		}
		t.Log("------------------------------------")
	})

	totalTestContext, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// 1. Define unique resources for this test run.
	runID := uuid.New().String()[:8]
	dataflowName := fmt.Sprintf("enrichment-bq-flow-%s", runID)
	ingestionTopicID := fmt.Sprintf("enrich-bq-ingest-topic-%s", runID)
	enrichmentSubID := fmt.Sprintf("enrich-bq-sub-%s", runID)
	enrichedTopicID := fmt.Sprintf("enrich-bq-output-topic-%s", runID)
	bigquerySubID := fmt.Sprintf("bq-sub-%s", runID)
	firestoreCollection := fmt.Sprintf("devices-e2e-%s", runID)
	uniqueDatasetID := fmt.Sprintf("dev_enriched_dataset_%s", runID)
	uniqueTableID := fmt.Sprintf("dev_enriched_payloads_%s", runID)

	// 2. Build the services definition in memory using the new MicroserviceArchitecture struct.
	schemaIdentifier := "github.com/illmade-knight/go-iot-dataflows/dataflow/devflow/e2e.EnrichedTestPayload"

	servicesConfig := &servicemanager.MicroserviceArchitecture{
		Environment: servicemanager.Environment{
			Name:      "e2e-enrichment",
			ProjectID: projectID,
			Location:  "US",
		},
		Dataflows: map[string]servicemanager.ResourceGroup{
			dataflowName: {
				Name:      dataflowName,
				Lifecycle: &servicemanager.LifecyclePolicy{Strategy: servicemanager.LifecycleStrategyEphemeral},
				Resources: servicemanager.CloudResourcesSpec{
					Topics: []servicemanager.TopicConfig{{CloudResource: servicemanager.CloudResource{Name: ingestionTopicID}}, {CloudResource: servicemanager.CloudResource{Name: enrichedTopicID}}},
					Subscriptions: []servicemanager.SubscriptionConfig{
						{CloudResource: servicemanager.CloudResource{Name: enrichmentSubID}, Topic: ingestionTopicID},
						{CloudResource: servicemanager.CloudResource{Name: bigquerySubID}, Topic: enrichedTopicID},
					},
					BigQueryDatasets: []servicemanager.BigQueryDataset{{CloudResource: servicemanager.CloudResource{Name: uniqueDatasetID}}},
					BigQueryTables: []servicemanager.BigQueryTable{
						{
							CloudResource:          servicemanager.CloudResource{Name: uniqueTableID},
							Dataset:                uniqueDatasetID,
							SchemaSourceIdentifier: schemaIdentifier,
							ClusteringFields:       []string{"device_id"}, // makes the table clustered if present
						},
					},
				},
			},
		},
	}

	// Define the schema registry for the director to understand the BigQuery schema
	schemaRegistry := map[string]interface{}{
		schemaIdentifier: EnrichedTestPayload{},
	}

	// 3. Setup dependencies: Emulators and Real Firestore Client
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

	// 4. Populate Firestore with enrichment data for all test devices.
	devices, deviceToClientID, cleanupFirestore := setupEnrichmentTestData(
		t,
		totalTestContext,
		fsClient,
		firestoreCollection,
		runID,
		enrichmentBigQueryTestNumDevices,
		enrichmentBigQueryTestRate,
	)
	t.Cleanup(cleanupFirestore)

	// 5. Start services and orchestrate resources.
	start = time.Now()
	// Call the standard, refactored startServiceDirector helper.
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
		teardownStart := time.Now()
		teardownURL := directorURL + "/orchestrate/teardown"
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, teardownURL, nil)
		http.DefaultClient.Do(req)
		// Also explicitly delete the BigQuery dataset with contents
		ds := bqClient.Dataset(uniqueDatasetID)
		if err := ds.DeleteWithContents(context.Background()); err != nil {
			logger.Warn().Err(err).Str("dataset", uniqueDatasetID).Msg("Failed to delete BigQuery dataset during cleanup.")
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
	logger.Info().Msg("All services started successfully.")

	// 6. Run Load Generator
	loadgenStart := time.Now()
	logger.Info().Msg("Starting MQTT load generator...")
	generator := loadgen.NewLoadGenerator(loadgen.NewMqttClient(mqttConn.EmulatorAddress, "devices/+/data", 1, logger), devices, logger)
	expectedMessageCount = generator.ExpectedMessagesForDuration(generateEnrichedBigqueryMessagesFor)

	publishedCount, err = generator.Run(totalTestContext, generateEnrichedBigqueryMessagesFor)
	require.NoError(t, err)
	timings["LoadGeneration"] = time.Since(loadgenStart).String()
	logger.Info().Int("published_count", publishedCount).Msg("Load generator finished.")

	// 7. Verify results in BigQuery
	verificationStart := time.Now()
	logger.Info().Msg("Starting BigQuery verification...")

	// Define the BigQuery validator for enriched data
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
		return nil
	}

	// Call the generic verifier with the enriched data validator.
	verifyBigQueryRows(t, logger, totalTestContext, projectID, uniqueDatasetID, uniqueTableID, publishedCount, enrichedBigQueryValidator)

	timings["VerificationDuration"] = time.Since(verificationStart).String()
	timings["ProcessingAndVerificationLatency"] = time.Since(loadgenStart).String()
}
