//go:build integration

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"google.golang.org/api/iterator"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-iot-dataflows/builder"
	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/helpers/loadgen"
	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

const (
	generateSimpleBigqueryMessagesFor = 5 * time.Second
	fullBigQueryTestNumDevices        = 5
	fullBigQueryTestRate              = 2.0
)

func TestFullDataflowE2E(t *testing.T) {
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
	var expectedMessageCount int

	t.Cleanup(func() {
		timings["TotalTestDuration"] = time.Since(testStart).String()
		timings["MessagesExpected"] = strconv.Itoa(expectedMessageCount)
		timings["MessagesPublished(Actual)"] = strconv.Itoa(publishedCount)
		// For BQ, the verified count is always the published count if the test passes.
		timings["MessagesVerified(Actual)"] = strconv.Itoa(publishedCount)

		t.Log("\n--- Test Timing & Metrics Breakdown ---")
		for name, d := range timings {
			t.Logf("%-35s: %s", name, d)
		}
		t.Log("------------------------------------")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	logger := log.With().Str("test", "TestFullDataflowE2E").Logger()

	// 1. Define the exact resources needed for this test.
	runID := uuid.New().String()[:8]
	dataflowName := fmt.Sprintf("bq-flow-%s", runID)
	uniqueTopicID := fmt.Sprintf("dev-ingestion-topic-%s", runID)
	uniqueSubID := fmt.Sprintf("dev-bq-subscription-%s", runID)
	uniqueDatasetID := fmt.Sprintf("dev_dataflow_dataset_%s", runID)
	uniqueTableID := fmt.Sprintf("dev_ingested_payloads_%s", runID)
	logger.Info().Str("run_id", runID).Msg("Generated unique resources for test run")

	// 2. Build the services definition in memory.
	schemaIdentifier := "github.com/illmade-knight/go-iot-dataflows/dataflow/devflow/e2e.TestPayload"
	servicesConfig := &servicemanager.TopLevelConfig{
		DefaultProjectID: projectID,
		Environments: map[string]servicemanager.EnvironmentSpec{
			"dev": {ProjectID: projectID},
		},
		Dataflows: []servicemanager.ResourceGroup{
			{
				Name:      dataflowName,
				Lifecycle: &servicemanager.LifecyclePolicy{Strategy: servicemanager.LifecycleStrategyEphemeral},
				Resources: servicemanager.ResourcesSpec{
					Topics:           []servicemanager.TopicConfig{{Name: uniqueTopicID}},
					Subscriptions:    []servicemanager.SubscriptionConfig{{Name: uniqueSubID, Topic: uniqueTopicID}},
					BigQueryDatasets: []servicemanager.BigQueryDataset{{Name: uniqueDatasetID}},
					BigQueryTables: []servicemanager.BigQueryTable{
						{
							Name:                   uniqueTableID,
							Dataset:                uniqueDatasetID,
							SchemaSourceType:       "go_struct",
							SchemaSourceIdentifier: schemaIdentifier,
						},
					},
				},
			},
		},
	}
	servicesDef, err := servicemanager.NewInMemoryServicesDefinition(servicesConfig)
	require.NoError(t, err)

	schemaRegistry := map[string]interface{}{
		schemaIdentifier: TestPayload{},
	}

	// 3. Start services and setup resources.
	var opts []option.ClientOption
	if creds := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); creds != "" {
		opts = append(opts, option.WithCredentialsFile(creds))
	}
	bqClient, err := bq.NewClient(ctx, projectID, opts...)
	require.NoError(t, err)
	defer bqClient.Close()

	start := time.Now()
	mqttContainer := emulators.SetupMosquittoContainer(t, ctx, emulators.GetDefaultMqttImageContainer())
	timings["EmulatorSetup(MQTT)"] = time.Since(start).String()

	start = time.Now()
	directorService, directorURL := startServiceDirector(t, ctx, logger.With().Str("service", "servicedirector").Logger(), servicesDef, schemaRegistry)
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
		logger.Info().Msg("Requesting resource teardown from ServiceDirector...")
		teardownURL := directorURL + "/orchestrate/teardown"
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, teardownURL, nil)
		http.DefaultClient.Do(req)
		ds := bqClient.Dataset(uniqueDatasetID)
		if err := ds.DeleteWithContents(context.Background()); err != nil {
			logger.Warn().Err(err).Str("dataset", uniqueDatasetID).Msg("Failed to delete BigQuery dataset during cleanup.")
		}
		timings["CloudResourceTeardown"] = time.Since(teardownStart).String()
	})

	start = time.Now()
	var ingestionSvc builder.Service
	require.Eventually(t, func() bool {
		ingestionSvc, err = startIngestionService(t, logger.With().Str("service", "ingestion").Logger(), directorURL, mqttContainer.EmulatorAddress, projectID, uniqueTopicID, dataflowName)
		return err == nil
	}, 30*time.Second, 5*time.Second)
	timings["ServiceStartup(Ingestion)"] = time.Since(start).String()
	t.Cleanup(ingestionSvc.Shutdown)

	start = time.Now()
	var bqSvc builder.Service
	require.Eventually(t, func() bool {
		bqSvc, err = startBigQueryService(t, logger.With().Str("service", "bigquery").Logger(), directorURL, projectID, uniqueSubID, uniqueDatasetID, uniqueTableID, dataflowName)
		return err == nil
	}, 30*time.Second, 5*time.Second)
	timings["ServiceStartup(BigQuery)"] = time.Since(start).String()
	t.Cleanup(bqSvc.Shutdown)
	logger.Info().Msg("All services started successfully.")

	// 4. Run Load Generator
	loadgenStart := time.Now()
	logger.Info().Msg("Starting MQTT load generator...")
	loadgenClient := loadgen.NewMqttClient(mqttContainer.EmulatorAddress, "devices/%s/data", 1, logger)
	devices := make([]*loadgen.Device, fullBigQueryTestNumDevices)
	for i := 0; i < fullBigQueryTestNumDevices; i++ {
		devices[i] = &loadgen.Device{ID: fmt.Sprintf("e2e-bq-device-%d-%s", i, runID), MessageRate: fullBigQueryTestRate, PayloadGenerator: &testPayloadGenerator{}}
	}
	generator := loadgen.NewLoadGenerator(loadgenClient, devices, logger)
	expectedMessageCount = generator.ExpectedMessagesForDuration(generateSimpleBigqueryMessagesFor)

	publishedCount, err = generator.Run(ctx, generateSimpleBigqueryMessagesFor)
	require.NoError(t, err)
	timings["LoadGeneration"] = time.Since(loadgenStart).String()
	logger.Info().Int("published_count", publishedCount).Msg("Load generator finished.")

	// 5. Verify results in BigQuery
	verificationStart := time.Now()
	logger.Info().Msg("Starting BigQuery verification...")

	// Define a simple validator that checks the final row count.
	countValidator := func(t *testing.T, iter *bq.RowIterator) error {
		var rowCount int
		// We don't need to unmarshal the row data, just iterate to count them.
		for {
			var row map[string]bq.Value
			err := iter.Next(&row)
			if err == iterator.Done {
				break
			}
			if err != nil {
				return err // The verifier will fail the test if an error occurs here.
			}
			rowCount++
		}
		// Assert that the final count matches the number of published messages.
		require.Equal(t, publishedCount, rowCount, "the final number of rows in BigQuery should match the number of messages published")
		return nil // Return nil for a successful validation.
	}

	// Call the generic verifier with the count-checking validator.
	verifyBigQueryRows(t, logger, ctx, projectID, uniqueDatasetID, uniqueTableID, publishedCount, countValidator)

	timings["VerificationDuration"] = time.Since(verificationStart).String()
	timings["ProcessingAndVerificationLatency"] = time.Since(loadgenStart).String()
}
