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
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-cloud-manager/pkg/servicemanager"
	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/illmade-knight/go-iot-dataflows/dataflow/devflow/replay" // Assuming 'replay' package is created
)

const (
	replayToBigqueryMessagesFor = 5 * time.Second // Duration over which to replay messages
)

func TestReplayToSimpleBigqueryFlowE2E(t *testing.T) {
	// --- Logger and Prerequisite Checks ---
	logger := zerolog.New(os.Stderr).With().Timestamp().Str("test", "TestReplayToSimpleBigqueryFlowE2E").Logger()

	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		t.Skip("Skipping E2E test: GOOGLE_CLOUD_PROJECT env var must be set.")
	}
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		logger.Warn().Msg("GOOGLE_APPLICATION_CREDENTIALS not set, relying on Application Default Credentials (ADC).")
		adcCheckCtx, adcCheckCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer adcCheckCancel()
		realPubSubClient, errAdc := pubsub.NewClient(adcCheckCtx, projectID)
		if errAdc != nil {
			t.Skipf("Skipping cloud test: ADC check failed: %v. Please configure ADC or set GOOGLE_APPLICATION_CREDENTIALS.", errAdc)
		}
		realPubSubClient.Close()
	}

	// This bucket MUST exist and contain data from a previous test run.
	sourceGCSBucketName := os.Getenv("REPLAY_GCS_BUCKET_NAME")
	if sourceGCSBucketName == "" {
		t.Skip("Skipping replay test: Please set 'REPLAY_GCS_BUCKET_NAME' env var with a bucket containing archived messages.")
	}

	// --- Timing & Metrics Setup ---
	timings := make(map[string]string)
	testStart := time.Now()
	var replayedCount int
	var expectedReplayCount int

	t.Cleanup(func() {
		timings["TotalTestDuration"] = time.Since(testStart).String()
		timings["MessagesExpectedToReplay"] = strconv.Itoa(expectedReplayCount)
		timings["MessagesReplayed(Actual)"] = strconv.Itoa(replayedCount)
		timings["MessagesVerified(BigQuery)"] = strconv.Itoa(replayedCount)

		logger.Info().Msg("\n--- Test Timing & Metrics Breakdown ---")
		for name, d := range timings {
			logger.Info().Msgf("%-35s: %s", name, d)
		}
		logger.Info().Msg("------------------------------------")
	})

	totalTestContext, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// 1. Define unique resources for this REPLAY run.
	runID := uuid.New().String()[:8]
	replayDataflowName := fmt.Sprintf("replay-bq-flow-%s", runID)
	replayIngestionTopicID := fmt.Sprintf("replay-ingest-topic-%s", runID)
	replayBigquerySubID := fmt.Sprintf("replay-bq-subscription-%s", runID)
	replayDatasetID := fmt.Sprintf("replay_dataset_%s", runID)
	replayTableID := fmt.Sprintf("replay_ingested_payloads_%s", runID)

	// 2. Build the services definition in memory for the target replay flow.
	schemaIdentifier := "github.com/illmade-knight/go-iot-dataflows/dataflow/devflow/e2e.TestPayload"
	servicesConfig := &servicemanager.MicroserviceArchitecture{
		Environment: servicemanager.Environment{
			Name:      "e2e-replay",
			ProjectID: projectID,
			Location:  "US",
		},
		Dataflows: map[string]servicemanager.ResourceGroup{
			replayDataflowName: {
				Name:      replayDataflowName,
				Lifecycle: &servicemanager.LifecyclePolicy{Strategy: servicemanager.LifecycleStrategyEphemeral},
				Resources: servicemanager.CloudResourcesSpec{
					Topics:           []servicemanager.TopicConfig{{CloudResource: servicemanager.CloudResource{Name: replayIngestionTopicID}}},
					Subscriptions:    []servicemanager.SubscriptionConfig{{CloudResource: servicemanager.CloudResource{Name: replayBigquerySubID}, Topic: replayIngestionTopicID}},
					BigQueryDatasets: []servicemanager.BigQueryDataset{{CloudResource: servicemanager.CloudResource{Name: replayDatasetID}}},
					BigQueryTables: []servicemanager.BigQueryTable{
						{
							CloudResource:          servicemanager.CloudResource{Name: replayTableID},
							Dataset:                replayDatasetID,
							SchemaSourceIdentifier: schemaIdentifier,
						},
					},
				},
			},
		},
	}

	schemaRegistry := map[string]interface{}{
		schemaIdentifier: TestPayload{},
	}

	// 3. Setup dependencies
	var opts []option.ClientOption
	if creds := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); creds != "" {
		opts = append(opts, option.WithCredentialsFile(creds))
	}
	start := time.Now()
	mqttConn := emulators.SetupMosquittoContainer(t, totalTestContext, emulators.GetDefaultMqttImageContainer())
	timings["EmulatorSetup(MQTT)"] = time.Since(start).String()

	bqClient, err := bq.NewClient(totalTestContext, projectID, opts...)
	require.NoError(t, err)
	defer bqClient.Close()

	gcsClient, err := storage.NewClient(totalTestContext, opts...)
	require.NoError(t, err)
	defer gcsClient.Close()

	// 4. Read messages from the EXISTING GCS bucket.
	logger.Info().Str("bucket", sourceGCSBucketName).Msg("Reading messages from GCS for replay...")
	deviceMessagesToReplay, err := replay.ReadMessagesFromGCS(t, totalTestContext, logger, gcsClient, sourceGCSBucketName)
	require.NoError(t, err, "Failed to read messages from GCS bucket %s", sourceGCSBucketName)

	replayDevices, totalMessages := replay.CreateReplayDevices(t, logger, deviceMessagesToReplay, replayToBigqueryMessagesFor)
	expectedReplayCount = totalMessages
	logger.Info().Int("count", expectedReplayCount).Msg("Messages loaded from GCS and replay devices created.")
	require.Greater(t, expectedReplayCount, 0, "No messages found in GCS bucket %s to replay. Ensure the bucket was populated.", sourceGCSBucketName)

	// 5. Start services for the new replay dataflow pipeline.
	start = time.Now()
	directorService, directorURL := startServiceDirector(t, totalTestContext, logger.With().Str("service", "servicedirector").Logger(), servicesConfig, schemaRegistry)
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
		logger.Info().Msg("Requesting resource teardown from ServiceDirector for replay flow...")
		teardownURL := directorURL + "/orchestrate/teardown"
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, teardownURL, nil)
		http.DefaultClient.Do(req)
		ds := bqClient.Dataset(replayDatasetID)
		if err := ds.DeleteWithContents(context.Background()); err != nil {
			logger.Warn().Err(err).Str("dataset", replayDatasetID).Msg("Failed to delete BigQuery dataset during replay cleanup.")
		}
		timings["CloudResourceTeardown(Director)"] = time.Since(teardownStart).String()
	})

	start = time.Now()
	ingestionSvc, err := startIngestionService(t, logger, directorURL, mqttConn.EmulatorAddress, projectID, replayIngestionTopicID, replayDataflowName)
	require.NoError(t, err)
	timings["ServiceStartup(Ingestion)"] = time.Since(start).String()
	t.Cleanup(ingestionSvc.Shutdown)

	start = time.Now()
	bqSvc, err := startBigQueryService(t, logger, directorURL, projectID, replayBigquerySubID, replayDatasetID, replayTableID, replayDataflowName)
	require.NoError(t, err)
	timings["ServiceStartup(BigQuery)"] = time.Since(start).String()
	t.Cleanup(bqSvc.Shutdown)
	logger.Info().Msg("Replay target services started successfully.")

	// 6. Replay messages to MQTT emulator.
	replayStart := time.Now()
	logger.Info().Msg("Starting MQTT replay of GCS messages...")
	replayedCount, err = replay.ReplayGCSMessagesToMQTT(t, totalTestContext, logger, mqttConn.EmulatorAddress, replayDevices, replayToBigqueryMessagesFor)
	require.NoError(t, err)
	timings["ReplayLoadGeneration"] = time.Since(replayStart).String()
	logger.Info().Int("replayed_count", replayedCount).Msg("Messages replayed to MQTT emulator.")

	// 7. Verify results in the NEW BigQuery table.
	verificationStart := time.Now()
	logger.Info().Msg("Starting BigQuery verification for replayed messages...")

	countValidator := func(t *testing.T, iter *bq.RowIterator) error {
		var rowCount int
		for {
			var row map[string]bq.Value
			err := iter.Next(&row)
			if err == iterator.Done {
				break
			}
			if err != nil {
				return err
			}
			rowCount++
		}
		require.Equal(t, replayedCount, rowCount, "the final number of rows in BigQuery should match the number of messages replayed")
		return nil
	}

	verifyBigQueryRows(t, logger, totalTestContext, projectID, replayDatasetID, replayTableID, replayedCount, countValidator)

	timings["VerificationDuration"] = time.Since(verificationStart).String()
	timings["ProcessingAndVerificationLatency"] = time.Since(replayStart).String()
}
