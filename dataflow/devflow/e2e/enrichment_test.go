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
	"github.com/illmade-knight/go-cloud-manager/pkg/servicemanager"
	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/helpers/loadgen"
	"github.com/illmade-knight/go-iot/pkg/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

const (
	generateEnrichedMessagesFor     = 2 * time.Second
	enrichmentE2ELoadTestNumDevices = 3
	enrichmentE2ELoadTestRate       = 1.0
)

func TestEnrichmentE2E_HappyPath(t *testing.T) {
	// --- Test & Logger Setup ---
	// Create a test-specific logger that writes to the test output.
	logger := zerolog.New(os.Stderr).
		With().Timestamp().Str("test", "TestEnrichmentE2E_HappyPath").Logger()

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
	var verifiedCount int
	var expectedMessageCount int

	t.Cleanup(func() {
		timings["TotalTestDuration"] = time.Since(testStart).String()
		timings["MessagesExpected"] = strconv.Itoa(expectedMessageCount)
		timings["MessagesPublished(Actual)"] = strconv.Itoa(publishedCount)
		timings["MessagesVerified(Actual)"] = strconv.Itoa(verifiedCount)

		logger.Info().Msg("--- Test Timing & Metrics Breakdown ---")
		for name, d := range timings {
			logger.Info().Msgf("%-35s: %s", name, d)
		}
		logger.Info().Msg("------------------------------------")
	})

	totalTestContext, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// 1. Define unique resources for this test run.
	runID := uuid.New().String()[:8]
	dataflowName := fmt.Sprintf("enrichment-flow-%s", runID)
	ingestionTopicID := fmt.Sprintf("enrich-ingest-topic-%s", runID)
	enrichmentSubID := fmt.Sprintf("enrich-sub-%s", runID)
	enrichedTopicID := fmt.Sprintf("enrich-output-topic-%s", runID)
	verifierSubID := fmt.Sprintf("verifier-sub-%s", runID)
	firestoreCollection := fmt.Sprintf("devices-e2e-%s", runID)
	logger.Info().Str("run_id", runID).Msg("Generated unique resources for test run")

	// 2. Build the services definition in memory.
	servicesConfig := &servicemanager.MicroserviceArchitecture{
		Environment: servicemanager.Environment{
			Name:      "e2e-enrichment",
			ProjectID: projectID,
		},
		Dataflows: map[string]servicemanager.ResourceGroup{
			dataflowName: {
				Name:      dataflowName,
				Lifecycle: &servicemanager.LifecyclePolicy{Strategy: servicemanager.LifecycleStrategyEphemeral},
				Resources: servicemanager.CloudResourcesSpec{
					Topics: []servicemanager.TopicConfig{{CloudResource: servicemanager.CloudResource{Name: ingestionTopicID}}, {CloudResource: servicemanager.CloudResource{Name: enrichedTopicID}}},
					Subscriptions: []servicemanager.SubscriptionConfig{
						{CloudResource: servicemanager.CloudResource{Name: enrichmentSubID}, Topic: ingestionTopicID},
						{CloudResource: servicemanager.CloudResource{Name: verifierSubID}, Topic: enrichedTopicID},
					},
				},
			},
		},
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
	directorService, directorURL := startServiceDirector(t, totalTestContext, logger, servicesConfig, nil)
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
	subContext, cancelSubCheck := context.WithTimeout(totalTestContext, 10*time.Second)
	defer cancelSubCheck()
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
		expectedClientID, clientIDFound := deviceToClientID[enrichedPayload.DeviceInfo.UID]
		if !clientIDFound {
			logger.Error().Str("device_id", enrichedPayload.DeviceInfo.UID).Msg("DeviceID not found in expected map during verification.")
			return false
		}
		if enrichedPayload.DeviceInfo.Name != expectedClientID {
			logger.Error().Str("expected_client_id", expectedClientID).Str("actual_client_id", enrichedPayload.DeviceInfo.Name).Msg("ClientID mismatch.")
			return false
		}
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
		close(verifierReady)
		verifiedCount = verifyPubSubMessages(t, logger, verifyContext, verifierSub, expectedCountCh, enrichedMessageValidator)
	}()

	<-verifierReady

	// 7. Run Load Generator
	generator := loadgen.NewLoadGenerator(loadgen.NewMqttClient(mqttConn.EmulatorAddress, "devices/+/data", 1, logger), devices, logger)
	expectedMessageCount = generator.ExpectedMessagesForDuration(generateEnrichedMessagesFor)

	loadgenStart := time.Now()
	logger.Info().Int("expected_messages", expectedMessageCount).Msg("Starting MQTT load generator...")

	publishedCount, err = generator.Run(context.Background(), generateEnrichedMessagesFor)
	require.NoError(t, err)

	expectedCountCh <- publishedCount
	close(expectedCountCh)

	timings["LoadGeneration"] = time.Since(loadgenStart).String()
	logger.Info().Int("published_count", publishedCount).Msg("Load generator finished.")

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
