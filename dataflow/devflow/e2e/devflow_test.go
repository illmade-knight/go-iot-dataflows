//go:build integration

package e2e

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-cloud-manager/pkg/servicemanager"
	"github.com/illmade-knight/go-iot-dataflows/builder"
	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/helpers/loadgen"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

const (
	totalDevflowTestDuration = 100 * time.Second
	loadTestDuration         = 5 * time.Second
	loadTestNumDevices       = 5
	loadTestRate             = 2.0
)

func TestDevflowE2E(t *testing.T) {

	totalTestContext, cancel := context.WithTimeout(context.Background(), totalDevflowTestDuration)
	defer cancel()

	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		t.Skip("Skipping E2E test: GOOGLE_CLOUD_PROJECT env var must be set.")
	}
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		log.Warn().Msg("GOOGLE_APPLICATION_CREDENTIALS not set, relying on Application Default Credentials (ADC).")
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

		t.Log("\n--- Test Timing & Metrics Breakdown ---")
		for name, d := range timings {
			t.Logf("%-35s: %s", name, d)
		}
		t.Log("------------------------------------")
	})

	logger := log.With().Str("test", "TestDevflowE2E").Logger()

	// 1. Define unique resources for this test run.
	runID := uuid.New().String()[:8]
	dataflowName := fmt.Sprintf("dev-flow-%s", runID)
	uniqueTopicID := fmt.Sprintf("dev-ingestion-topic-%s", runID)
	uniqueSubscriptionID := fmt.Sprintf("dev-verifier-sub-%s", runID)
	logger.Info().Str("run_id", runID).Str("topic_id", uniqueTopicID).Msg("Generated unique resources for test run")

	// 2. Build the services definition in memory using the new architecture struct.
	servicesConfig := &servicemanager.MicroserviceArchitecture{
		Environment: servicemanager.Environment{
			Name:      "e2e-devflow",
			ProjectID: projectID,
		},
		Dataflows: map[string]servicemanager.ResourceGroup{
			dataflowName: {
				Name:      dataflowName,
				Lifecycle: &servicemanager.LifecyclePolicy{Strategy: servicemanager.LifecycleStrategyEphemeral},
				Resources: servicemanager.CloudResourcesSpec{
					Topics: []servicemanager.TopicConfig{
						{CloudResource: servicemanager.CloudResource{Name: uniqueTopicID}},
					},
					Subscriptions: []servicemanager.SubscriptionConfig{
						{CloudResource: servicemanager.CloudResource{Name: uniqueSubscriptionID}, Topic: uniqueTopicID},
					},
				},
			},
		},
	}

	// 3. Start MQTT Broker and ServiceDirector
	start := time.Now()
	mqttContainer := emulators.SetupMosquittoContainer(t, totalTestContext, emulators.GetDefaultMqttImageContainer())
	timings["EmulatorSetup(MQTT)"] = time.Since(start).String()

	start = time.Now()
	directorLogger := logger.With().Str("service", "servicedirector").Logger()
	// The schema registry is passed to the director but is empty for this test.
	directorService, directorURL := startServiceDirector(t, totalTestContext, directorLogger, servicesConfig, nil)
	t.Cleanup(directorService.Shutdown)
	timings["ServiceStartup(Director)"] = time.Since(start).String()
	logger.Info().Str("url", directorURL).Msg("ServiceDirector is healthy")

	// 4. Set up resources via the director's API
	start = time.Now()
	setupURL := directorURL + "/orchestrate/setup"
	resp, err := http.Post(setupURL, "application/json", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "ServiceDirector setup request failed")
	resp.Body.Close()
	timings["CloudResourceSetup(Director)"] = time.Since(start).String()
	logger.Info().Msg("ServiceDirector confirmed resource setup is complete.")

	t.Cleanup(func() {
		teardownStart := time.Now()
		logger.Info().Msg("Test finished. Requesting resource teardown from ServiceDirector...")
		teardownURL := directorURL + "/orchestrate/teardown"
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, teardownURL, nil)
		cleanupResp, cleanupErr := http.DefaultClient.Do(req)
		if cleanupErr == nil {
			require.Equal(t, http.StatusOK, cleanupResp.StatusCode, "Teardown request failed")
			cleanupResp.Body.Close()
			logger.Info().Msg("ServiceDirector confirmed resource teardown.")
		} else {
			t.Logf("Failed to send teardown request: %v", cleanupErr)
		}
		timings["CloudResourceTeardown(Director)"] = time.Since(teardownStart).String()
	})

	// 5. Start Ingestion Service
	start = time.Now()
	ingestionLogger := logger.With().Str("service", "ingestion").Logger()
	var ingestionService builder.Service
	require.Eventually(t, func() bool {
		var startErr error
		ingestionService, startErr = startIngestionService(t, ingestionLogger, directorURL, mqttContainer.EmulatorAddress, projectID, uniqueTopicID, dataflowName)
		if startErr != nil {
			return false
		}
		return true
	}, 30*time.Second, 5*time.Second, "Ingestion service failed to start after multiple retries")
	timings["ServiceStartup(Ingestion)"] = time.Since(start).String()
	t.Cleanup(ingestionService.Shutdown)
	logger.Info().Msg("Ingestion service started successfully.")

	// 6. Start the Pub/Sub verifier in the background.
	verificationDone := make(chan struct{})
	expectedCountCh := make(chan int, 1)

	client, err := pubsub.NewClient(totalTestContext, projectID)
	defer client.Close()
	require.NoError(t, err)
	sub := client.Subscription(uniqueSubscriptionID)
	ok, err := sub.Exists(totalTestContext)
	require.NoError(t, err)
	require.True(t, ok)

	go func() {
		defer close(verificationDone)
		var noValidationNeeded MessageValidationFunc = nil
		verifiedCount = verifyPubSubMessages(
			t,
			logger,
			totalTestContext,
			sub,
			expectedCountCh,
			noValidationNeeded,
		)
	}()

	// 7. Run Load Generator
	loadgenStart := time.Now()
	logger.Info().Msg("Starting MQTT load generator...")
	loadgenClient := loadgen.NewMqttClient(mqttContainer.EmulatorAddress, "devices/%s/data", 1, logger)
	devices := make([]*loadgen.Device, loadTestNumDevices)
	for i := 0; i < loadTestNumDevices; i++ {
		devices[i] = &loadgen.Device{ID: fmt.Sprintf("e2e-device-%d-%s", i, runID), MessageRate: loadTestRate, PayloadGenerator: &testPayloadGenerator{}}
	}
	generator := loadgen.NewLoadGenerator(loadgenClient, devices, logger)
	expectedMessageCount = generator.ExpectedMessagesForDuration(loadTestDuration)

	publishedCount, err = generator.Run(totalTestContext, loadTestDuration)
	require.NoError(t, err)
	timings["LoadGeneration"] = time.Since(loadgenStart).String()
	logger.Info().Int("published_count", publishedCount).Msg("Load generator finished.")

	// Send the exact count to the waiting verifier.
	expectedCountCh <- publishedCount
	close(expectedCountCh)

	// 8. Wait for verification to complete
	logger.Info().Msg("Waiting for Pub/Sub verification to complete...")
	select {
	case <-verificationDone:
		timings["ProcessingAndVerificationLatency"] = time.Since(loadgenStart).String()
		logger.Info().Msg("Verification successful!")
	case <-totalTestContext.Done():
		t.Fatal("Test timed out waiting for verification")
	}
}
