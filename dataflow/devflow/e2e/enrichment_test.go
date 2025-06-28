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

	"cloud.google.com/go/firestore"
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
	enrichmentLoadTestDuration   = 5 * time.Second
	enrichmentLoadTestNumDevices = 3 // Test with multiple devices
	enrichmentLoadTestRate       = 5.0
)

func TestEnrichmentDataflowE2E(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	logger := log.With().Str("test", "TestEnrichmentDataflowE2E").Logger()

	// 1. Define unique resources for this test run.
	runID := uuid.New().String()[:8]
	uniqueTopicID := fmt.Sprintf("ingestion-topic-%s", runID)
	uniqueEnrichedTopicID := fmt.Sprintf("enriched-topic-%s", runID)
	uniqueEnrichmentSubID := fmt.Sprintf("enrichment-sub-%s", runID)
	uniqueBqSubID := fmt.Sprintf("bq-sub-%s", runID)
	uniqueDatasetID := fmt.Sprintf("dataflow_dataset_%s", runID)
	uniqueTableID := fmt.Sprintf("enriched_payloads_%s", runID)
	firestoreCollection := fmt.Sprintf("devices-e2e-%s", runID)
	logger.Info().Str("run_id", runID).Msg("Generated unique resources for test run")

	// 2. Build the services definition in memory.
	schemaIdentifier := "github.com/illmade-knight/go-iot-dataflows/dataflow/devflow/e2e.EnrichedTestPayload"
	servicesConfig := &servicemanager.TopLevelConfig{
		DefaultProjectID: projectID,
		Environments:     map[string]servicemanager.EnvironmentSpec{"dev": {ProjectID: projectID}},
		Dataflows: []servicemanager.ResourceGroup{{
			Name:      "devflow-main",
			Lifecycle: &servicemanager.LifecyclePolicy{Strategy: servicemanager.LifecycleStrategyEphemeral},
			Resources: servicemanager.ResourcesSpec{
				Topics:           []servicemanager.TopicConfig{{Name: uniqueTopicID}, {Name: uniqueEnrichedTopicID}},
				Subscriptions:    []servicemanager.SubscriptionConfig{{Name: uniqueEnrichmentSubID, Topic: uniqueTopicID}, {Name: uniqueBqSubID, Topic: uniqueEnrichedTopicID}},
				BigQueryDatasets: []servicemanager.BigQueryDataset{{Name: uniqueDatasetID}},
				BigQueryTables: []servicemanager.BigQueryTable{{
					Name:                   uniqueTableID,
					Dataset:                uniqueDatasetID,
					SchemaSourceType:       "go_struct",
					SchemaSourceIdentifier: schemaIdentifier,
				}},
			},
		}},
	}
	servicesDef, err := servicemanager.NewInMemoryServicesDefinition(servicesConfig)
	require.NoError(t, err)

	// THIS IS THE FIX: Create the schema registry for the EnrichedTestPayload
	schemaRegistry := map[string]interface{}{
		schemaIdentifier: EnrichedTestPayload{},
	}

	// 3. Setup dependencies: Emulators and Real Clients
	var opts []option.ClientOption
	if creds := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); creds != "" {
		opts = append(opts, option.WithCredentialsFile(creds))
	}

	redisConn := emulators.SetupRedisContainer(t, ctx, emulators.GetDefaultRedisImageContainer())
	mqttContainer := emulators.SetupMosquittoContainer(t, ctx, emulators.GetDefaultMqttImageContainer())
	fsClient, err := firestore.NewClient(ctx, projectID, opts...)
	require.NoError(t, err)
	defer fsClient.Close()

	bqClient, err := bq.NewClient(ctx, projectID, opts...)
	require.NoError(t, err)
	defer bqClient.Close()

	// Populate Firestore with test data for a single, specific device.
	deviceEUI := fmt.Sprintf("e2e-device-enriched-%s", runID)
	expectedClientID := fmt.Sprintf("client-%s", runID)
	deviceDoc := map[string]interface{}{"clientID": expectedClientID, "locationID": "loc-456", "category": "cat-789"}
	_, err = fsClient.Collection(firestoreCollection).Doc(deviceEUI).Set(ctx, deviceDoc)
	require.NoError(t, err)
	t.Cleanup(func() {
		// Asynchronously delete the document to not slow down test completion.
		go fsClient.Collection(firestoreCollection).Doc(deviceEUI).Delete(context.Background())
	})

	// 4. Start services and setup resources.
	// Pass the schema registry to the service director.
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

	t.Cleanup(func() {
		teardownURL := directorURL + "/orchestrate/teardown"
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, teardownURL, bytes.NewBuffer(nil))
		// Fire and forget teardown on cleanup
		http.DefaultClient.Do(req)
	})

	var ingestionSvc, enrichmentSvc, bqSvc builder.Service
	require.Eventually(t, func() bool {
		ingestionSvc, err = startIngestionService(t, ctx, logger, directorURL, mqttContainer.EmulatorAddress, projectID, uniqueTopicID)
		return err == nil
	}, 30*time.Second, 5*time.Second)
	t.Cleanup(ingestionSvc.Shutdown)

	require.Eventually(t, func() bool {
		enrichmentSvc, err = startEnrichmentService(t, ctx, logger, directorURL, projectID, uniqueEnrichmentSubID, uniqueEnrichedTopicID, redisConn.EmulatorAddress, firestoreCollection)
		return err == nil
	}, 30*time.Second, 5*time.Second)
	t.Cleanup(enrichmentSvc.Shutdown)

	require.Eventually(t, func() bool {
		bqSvc, err = startEnrichedBigQueryService(t, ctx, logger, directorURL, projectID, uniqueBqSubID, uniqueDatasetID, uniqueTableID)
		return err == nil
	}, 30*time.Second, 5*time.Second)
	t.Cleanup(bqSvc.Shutdown)
	logger.Info().Msg("All services started successfully.")

	// 5. Run Load Generator
	time.Sleep(5 * time.Second) // Settle time
	logger.Info().Msg("Starting MQTT load generator...")
	loadgenClient := loadgen.NewMqttClient(mqttContainer.EmulatorAddress, "devices/%s/data", 1, logger)

	// Create a list of devices, ensuring the one with enrichment data is included.
	devices := make([]*loadgen.Device, enrichmentLoadTestNumDevices)
	devices[0] = &loadgen.Device{ID: deviceEUI, MessageRate: enrichmentLoadTestRate, PayloadGenerator: &testPayloadGenerator{}}
	for i := 1; i < enrichmentLoadTestNumDevices; i++ {
		devices[i] = &loadgen.Device{ID: fmt.Sprintf("e2e-generic-device-%d-%s", i, runID), MessageRate: enrichmentLoadTestRate, PayloadGenerator: &testPayloadGenerator{}}
	}

	generator := loadgen.NewLoadGenerator(loadgenClient, devices, logger)
	err = generator.Run(ctx, enrichmentLoadTestDuration)
	require.NoError(t, err)
	logger.Info().Msg("Load generator finished.")

	// 6. Verify results in BigQuery
	minExpectedMessages := int(float64(enrichmentLoadTestNumDevices) * enrichmentLoadTestRate * enrichmentLoadTestDuration.Seconds() * 0.8) // 80% success threshold
	verifyEnrichedBigQueryResults(t, ctx, projectID, uniqueDatasetID, uniqueTableID, deviceEUI, expectedClientID, minExpectedMessages)
}
