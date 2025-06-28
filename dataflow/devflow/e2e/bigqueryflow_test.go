//go:build integration

package e2e

import (
	"bytes"
	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

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
	fullBigQueryTestDuration   = 5 * time.Second
	fullBigQueryTestNumDevices = 5
	fullBigQueryTestRate       = 1.0
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

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	logger := log.With().Str("test", "TestFullDataflowE2E").Logger()

	// 1. Define the exact resources needed for this test, with unique names.
	runID := uuid.New().String()[:8]
	uniqueTopicID := fmt.Sprintf("dev-ingestion-topic-%s", runID)
	uniqueSubID := fmt.Sprintf("dev-bq-subscription-%s", runID)
	uniqueDatasetID := fmt.Sprintf("dev_dataflow_dataset_%s", runID)
	uniqueTableID := fmt.Sprintf("dev_ingested_payloads_%s", runID)
	logger.Info().Str("run_id", runID).Msg("Generated unique resources for test run")

	// 2. Build the services definition in memory. This is the source of truth for the test.
	schemaIdentifier := "github.com/illmade-knight/go-iot-dataflows/dataflow/devflow/e2e.TestPayload"
	servicesConfig := &servicemanager.TopLevelConfig{
		DefaultProjectID: projectID,
		Environments: map[string]servicemanager.EnvironmentSpec{
			"dev": {ProjectID: projectID},
		},
		Dataflows: []servicemanager.ResourceGroup{
			{
				Name:      "devflow-main",
				Lifecycle: &servicemanager.LifecyclePolicy{Strategy: servicemanager.LifecycleStrategyEphemeral},
				Resources: servicemanager.ResourcesSpec{
					Topics: []servicemanager.TopicConfig{
						{Name: uniqueTopicID},
					},
					Subscriptions: []servicemanager.SubscriptionConfig{
						{Name: uniqueSubID, Topic: uniqueTopicID},
					},
					BigQueryDatasets: []servicemanager.BigQueryDataset{
						{Name: uniqueDatasetID},
					},
					BigQueryTables: []servicemanager.BigQueryTable{
						{
							Name:                   uniqueTableID,
							Dataset:                uniqueDatasetID,
							SchemaSourceType:       "go_struct",
							SchemaSourceIdentifier: schemaIdentifier, // Use the defined identifier
						},
					},
				},
			},
		},
	}
	servicesDef, err := servicemanager.NewInMemoryServicesDefinition(servicesConfig)
	require.NoError(t, err)

	// THIS IS THE FIX: Create the schema registry and pass it to the director.
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

	mqttContainer := emulators.SetupMosquittoContainer(t, ctx, emulators.GetDefaultMqttImageContainer())
	// Pass the schema registry to the service director starter.
	directorService, directorURL := startServiceDirector(t, ctx, logger.With().Str("service", "servicedirector").Logger(), servicesDef, schemaRegistry)
	t.Cleanup(directorService.Shutdown)

	setupURL := directorURL + "/orchestrate/setup"
	resp, err := http.Post(setupURL, "application/json", bytes.NewBuffer([]byte{}))
	require.NoError(t, err)
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		t.Fatalf("ServiceDirector setup failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}
	resp.Body.Close()
	logger.Info().Msg("ServiceDirector confirmed resource setup is complete.")

	// Implement the robust teardown strategy.
	t.Cleanup(func() {
		logger.Info().Msg("Requesting resource teardown from ServiceDirector...")
		teardownURL := directorURL + "/orchestrate/teardown"
		// Attempt teardown via director first.
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, teardownURL, bytes.NewBuffer(nil))
		http.DefaultClient.Do(req)

		// As a fallback, directly delete the BQ Dataset to ensure cleanup.
		logger.Info().Str("dataset", uniqueDatasetID).Msg("Ensuring BigQuery dataset is deleted directly.")
		ds := bqClient.Dataset(uniqueDatasetID)
		if err := ds.DeleteWithContents(context.Background()); err != nil {
			// Log the error but don't fail the test, as cleanup failures can be noisy.
			logger.Warn().Err(err).Str("dataset", uniqueDatasetID).Msg("Failed to delete BigQuery dataset during cleanup.")
		}
	})

	// Start ingestion service
	var ingestionSvc builder.Service
	require.Eventually(t, func() bool {
		ingestionSvc, err = startIngestionService(t, ctx, logger.With().Str("service", "ingestion").Logger(), directorURL, mqttContainer.EmulatorAddress, projectID, uniqueTopicID)
		return err == nil
	}, 30*time.Second, 5*time.Second)
	t.Cleanup(ingestionSvc.Shutdown)
	logger.Info().Msg("Ingestion service started successfully.")

	// Start BigQuery service
	var bqSvc builder.Service
	require.Eventually(t, func() bool {
		bqSvc, err = startBigQueryService(t, ctx, logger.With().Str("service", "bigquery").Logger(), directorURL, projectID, uniqueSubID, uniqueDatasetID, uniqueTableID)
		return err == nil
	}, 30*time.Second, 5*time.Second)
	t.Cleanup(bqSvc.Shutdown)
	logger.Info().Msg("BigQuery service started successfully.")

	// 4. Run Load Generator
	time.Sleep(5 * time.Second) // Settle time
	logger.Info().Msg("Starting MQTT load generator...")
	loadgenClient := loadgen.NewMqttClient(mqttContainer.EmulatorAddress, "devices/%s/data", 1, logger)
	devices := make([]*loadgen.Device, fullBigQueryTestNumDevices)
	for i := 0; i < fullBigQueryTestNumDevices; i++ {
		devices[i] = &loadgen.Device{ID: fmt.Sprintf("e2e-device-%d", i), MessageRate: fullBigQueryTestRate, PayloadGenerator: &testPayloadGenerator{}}
	}
	generator := loadgen.NewLoadGenerator(loadgenClient, devices, logger)
	err = generator.Run(ctx, fullBigQueryTestDuration)
	require.NoError(t, err)
	logger.Info().Msg("Load generator finished.")

	// 5. Verify results in BigQuery
	minExpectedMessages := int(float64(fullBigQueryTestNumDevices) * fullBigQueryTestRate * fullBigQueryTestDuration.Seconds() * 0.9)
	verifyBigQueryResults(t, ctx, projectID, uniqueDatasetID, uniqueTableID, minExpectedMessages)
}
