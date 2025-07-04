//go:build integration

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/helpers/loadgen"
	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/illmade-knight/go-iot/pkg/types"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

const (
	generateEnrichedMessagesFor     = 2 * time.Second
	enrichmentE2ELoadTestNumDevices = 3
	enrichmentE2ELoadTestRate       = 1.0
)

func TestEnrichmentE2E_HappyPath(t *testing.T) {
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		t.Skip("Skipping E2E test: GOOGLE_CLOUD_PROJECT env var must be set.")
	}
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		log.Warn().Msg("GOOGLE_APPLICATION_CREDENTIALS not set, relying on Application Default Credentials (ADC).")
		adcCheckCtx, adcCheckCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer adcCheckCancel()
		_, errAdc := pubsub.NewClient(adcCheckCtx, projectID)
		if errAdc != nil {
			t.Skipf("Skipping cloud test: ADC check failed: %v", errAdc)
		}
	}

	// --- Timing & Metrics Setup ---
	timings := make(map[string]string)
	testStart := time.Now()
	var publishedCount int
	var verifiedCount int
	var expectedMessageCount int

	t.Cleanup(func() {
		timings["TotalTestDuration"] = time.Since(testStart).String()
		timings["MessagesExpected"] = strconv.Itoa(expectedMessageCount)
		timings["MessagesPublished(Actual)"] = strconv.Itoa(publishedCount)
		timings["MessagesVerified(Actual)"] = strconv.Itoa(verifiedCount)

		t.Log("\n--- Test Timing & Metrics Breakdown ---")
		for name, d := range timings {
			t.Logf("%-35s: %s", name, d)
		}
		t.Log("------------------------------------")
	})

	totalTestContext, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	logger := log.With().Str("test", "TestEnrichmentE2E_HappyPath").Logger()

	// 1. Define unique resources for this test run.
	runID := uuid.New().String()[:8]
	dataflowName := fmt.Sprintf("enrichment-flow-%s", runID)
	ingestionTopicID := fmt.Sprintf("enrich-ingest-topic-%s", runID)
	enrichmentSubID := fmt.Sprintf("enrich-sub-%s", runID)

	enrichedTopicID := fmt.Sprintf("enrich-output-topic-%s", runID)
	verifierSubID := fmt.Sprintf("verifier-sub-%s", runID)
	firestoreCollection := fmt.Sprintf("devices-e2e-%s", runID)

	// 2. Build the services definition in memory.
	servicesConfig := &servicemanager.TopLevelConfig{
		DefaultProjectID: projectID,
		Environments:     map[string]servicemanager.EnvironmentSpec{"dev": {ProjectID: projectID}},
		Dataflows: []servicemanager.ResourceGroup{{
			Name:      dataflowName,
			Lifecycle: &servicemanager.LifecyclePolicy{Strategy: servicemanager.LifecycleStrategyEphemeral},
			Resources: servicemanager.ResourcesSpec{
				Topics: []servicemanager.TopicConfig{{Name: ingestionTopicID}, {Name: enrichedTopicID}},
				Subscriptions: []servicemanager.SubscriptionConfig{
					{
						Name:            enrichmentSubID,
						Topic:           ingestionTopicID,
						ConsumerService: "",
					},
					{
						Name:            verifierSubID,
						Topic:           enrichedTopicID,
						ConsumerService: "",
					},
				},
			},
		}},
	}
	servicesDef, err := servicemanager.NewInMemoryServicesDefinition(servicesConfig)
	require.NoError(t, err)

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

	// 4. Populate Firestore with enrichment data for all test devices.
	devices, deviceToClientID, cleanupFirestore := setupEnrichmentTestData(
		t,
		totalTestContext,
		fsClient,
		firestoreCollection,
		runID,
		enrichmentE2ELoadTestNumDevices,
		enrichmentE2ELoadTestRate,
	)
	t.Cleanup(cleanupFirestore)

	// 5. Start services and orchestrate resources.
	start = time.Now()
	directorService, directorURL := startServiceDirector(t, totalTestContext, logger, servicesDef)
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
		timings["CloudResourceTeardown(Director)"] = time.Since(teardownStart).String()
	})

	// ok our manager should have set up this subscription - check it with a short context to make sure we don't hang
	subContext, cancel := context.WithTimeout(totalTestContext, 10*time.Second)
	defer cancel()
	client, err := pubsub.NewClient(subContext, projectID)
	defer client.Close()
	require.NoError(t, err)
	verifierSub := client.Subscription(verifierSubID)
	ok, err := verifierSub.Exists(totalTestContext)
	require.NoError(t, err)
	require.True(t, ok)
	if !ok {
		logger.Fatal().Msg("no verifier subscription")
	}

	start = time.Now()
	ingestionSvc, err := startAttributeIngestionService(t, logger, directorURL, mqttConn.EmulatorAddress, projectID, ingestionTopicID, dataflowName)
	require.NoError(t, err)
	timings["ServiceStartup(Ingestion)"] = time.Since(start).String()
	t.Cleanup(ingestionSvc.Shutdown)

	start = time.Now()
	// Call the enrichment service starter without a DLT topic for the happy path test.
	enrichmentSvc, err := startEnrichmentService(t, totalTestContext, logger, directorURL, projectID, enrichmentSubID, enrichedTopicID, redisConn.EmulatorAddress, firestoreCollection, dataflowName)
	require.NoError(t, err)
	timings["ServiceStartup(Enrichment)"] = time.Since(start).String()
	t.Cleanup(enrichmentSvc.Shutdown)
	logger.Info().Msg("All services started successfully.")

	// 6. Start the Pub/Sub verifier in the background.
	verificationDone := make(chan struct{})
	verifierReady := make(chan struct{})
	expectedCountCh := make(chan int, 1)

	// Define the validation function for enriched messages
	enrichedMessageValidator := func(t *testing.T, msg *pubsub.Message) bool {
		var enrichedPayload types.PublishMessage
		if err := json.Unmarshal(msg.Data, &enrichedPayload); err != nil {
			logger.Error().Err(err).Msg("Failed to unmarshal enriched message for verification.")
			return false
		}

		if enrichedPayload.DeviceInfo == nil {
			logger.Error().Msg("Enriched message missing DeviceInfo.")
			return false
		}

		// Verify clientID
		expectedClientID, clientIDFound := deviceToClientID[enrichedPayload.DeviceInfo.UID]
		if !clientIDFound {
			logger.Error().Str("device_id", enrichedPayload.DeviceInfo.UID).Msg("DeviceID not found in expected map during verification.")
			return false
		}
		if enrichedPayload.DeviceInfo.Name != expectedClientID {
			logger.Error().Str("expected_client_id", expectedClientID).Str("actual_client_id", enrichedPayload.DeviceInfo.Name).Msg("ClientID mismatch.")
			return false
		}

		// Verify locationID and DeviceCategory (serviceTag)
		if enrichedPayload.DeviceInfo.Location != "loc-456" {
			logger.Error().Str("actual_location_id", enrichedPayload.DeviceInfo.Location).Msg("LocationID mismatch.")
			return false
		}
		if enrichedPayload.DeviceInfo.ServiceTag != "cat-789" {
			logger.Error().Str("actual_category", enrichedPayload.DeviceInfo.ServiceTag).Msg("DeviceCategory mismatch.")
			return false
		}
		return true
	}

	verifyContext, cancelVerify := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancelVerify()
	go func() {
		defer close(verificationDone)
		// Signal that the verifier is ready to receive messages and expected count.
		close(verifierReady)
		verifiedCount = verifyPubSubMessages(t, logger, verifyContext, verifierSub, expectedCountCh, enrichedMessageValidator)
	}()

	// Wait until the verifier confirms its subscription is created.
	<-verifierReady

	// 7. Run Load Generator
	generator := loadgen.NewLoadGenerator(loadgen.NewMqttClient(mqttConn.EmulatorAddress, "devices/+/data", 1, logger), devices, logger)
	expectedMessageCount = generator.ExpectedMessagesForDuration(generateEnrichedMessagesFor)

	loadgenStart := time.Now()
	logger.Info().Int("expected_messages", expectedMessageCount).Msg("Starting MQTT load generator...")

	publishedCount, err = generator.Run(context.Background(), generateEnrichedMessagesFor)
	require.NoError(t, err)

	// Send the exact count to the waiting verifier.
	expectedCountCh <- publishedCount
	close(expectedCountCh)

	timings["LoadGeneration"] = time.Since(loadgenStart).String()
	logger.Info().Int("published_count", publishedCount).Msg("Load generator finished.")

	//require.Equal(t, expectedMessageCount, publishedCount, "Load generator did not publish the expected number of messages.")

	// 8. Wait for verification to complete.
	logger.Info().Msg("Waiting for enrichment verification to complete...")
	select {
	case <-verificationDone:
		timings["ProcessingAndVerificationLatency"] = time.Since(loadgenStart).String()
		logger.Info().Msg("Verification successful!")
	case <-totalTestContext.Done():
		t.Fatal("Test timed out waiting for verification")
	}
}
