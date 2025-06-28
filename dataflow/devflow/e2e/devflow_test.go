//go:build integration

package e2e

import (
	"bytes"
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
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
	"gopkg.in/yaml.v3"
)

const (
	loadTestDuration   = 5 * time.Second
	loadTestNumDevices = 5
	loadTestRate       = 1.0
)

func TestDevflowE2E(t *testing.T) {
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

	logger := log.With().Str("test", "TestDevflowE2E").Logger()

	// 1. Generate unique names and lifecycle for this test run.
	runID := uuid.New().String()[:8]
	uniqueTopicID := fmt.Sprintf("dev-ingestion-topic-%s", runID)
	logger.Info().Str("run_id", runID).Str("topic_id", uniqueTopicID).Msg("Generated unique resources for test run")

	// 2. Load services.yaml and programmatically override it for the test.
	yamlPath := "../services.yaml"
	yamlFile, err := os.ReadFile(yamlPath)
	require.NoError(t, err)
	var servicesConfig servicemanager.TopLevelConfig
	err = yaml.Unmarshal(yamlFile, &servicesConfig)
	require.NoError(t, err)

	servicesConfig.DefaultProjectID = projectID
	if devEnv, ok := servicesConfig.Environments["dev"]; ok {
		devEnv.ProjectID = projectID
		servicesConfig.Environments["dev"] = devEnv
	}
	require.NotEmpty(t, servicesConfig.Dataflows, "services.yaml must contain at least one dataflow")
	servicesConfig.Dataflows[0].Resources.Topics[0].Name = uniqueTopicID
	servicesConfig.Dataflows[0].Lifecycle.Strategy = servicemanager.LifecycleStrategyEphemeral
	logger.Info().Msg("Overrode services config with unique topic name and ephemeral lifecycle.")

	servicesDef, err := servicemanager.NewInMemoryServicesDefinition(&servicesConfig)
	require.NoError(t, err)

	// 3. Start MQTT Broker and ServiceDirector
	mqttContainer := emulators.SetupMosquittoContainer(t, ctx, emulators.GetDefaultMqttImageContainer())
	directorLogger := logger.With().Str("service", "servicedirector").Logger()
	directorService, directorURL := startServiceDirector(t, ctx, directorLogger, servicesDef)
	t.Cleanup(directorService.Shutdown)
	logger.Info().Str("url", directorURL).Msg("ServiceDirector is healthy")

	// 4. Set up resources via the director's API
	setupURL := directorURL + "/orchestrate/setup"
	resp, err := http.Post(setupURL, "application/json", bytes.NewBuffer([]byte{}))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "ServiceDirector setup request failed")
	resp.Body.Close()
	logger.Info().Msg("ServiceDirector confirmed resource setup is complete.")

	t.Cleanup(func() {
		logger.Info().Msg("Test finished. Requesting resource teardown from ServiceDirector...")
		teardownURL := directorURL + "/orchestrate/teardown"
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		req, _ := http.NewRequestWithContext(cleanupCtx, http.MethodPost, teardownURL, bytes.NewBuffer([]byte{}))
		cleanupResp, cleanupErr := http.DefaultClient.Do(req)
		if cleanupErr == nil {
			require.Equal(t, http.StatusOK, cleanupResp.StatusCode, "Teardown request failed")
			cleanupResp.Body.Close()
			logger.Info().Msg("ServiceDirector confirmed resource teardown.")
		} else {
			t.Logf("Failed to send teardown request: %v", cleanupErr)
		}
	})

	// 5. Start Ingestion Service
	ingestionLogger := logger.With().Str("service", "ingestion").Logger()
	var ingestionService builder.Service
	require.Eventually(t, func() bool {
		ingestionLogger.Info().Msg("Attempting to start Ingestion service...")
		var startErr error
		ingestionService, startErr = startIngestionService(t, ctx, ingestionLogger, directorURL, mqttContainer.EmulatorAddress, projectID, uniqueTopicID)
		if startErr != nil {
			ingestionLogger.Warn().Err(startErr).Msg("Failed to start ingestion service, will retry...")
			return false
		}
		return true
	}, 30*time.Second, 5*time.Second, "Ingestion service failed to start after multiple retries")
	t.Cleanup(ingestionService.Shutdown)
	logger.Info().Msg("Ingestion service started successfully.")

	// 6. Start Pub/Sub Verifier and wait for it to be ready.
	minExpectedMessages := int(float64(loadTestNumDevices) * loadTestRate * loadTestDuration.Seconds() * 0.9)
	verificationDone := make(chan struct{})
	verifierReady := make(chan struct{}) // New channel to signal readiness
	go func() {
		defer close(verificationDone)
		// Pass the ready channel to the verifier function.
		verifyPubSubMessages(t, ctx, projectID, uniqueTopicID, minExpectedMessages, verifierReady)
	}()

	// Block until the verifier signals that its subscription is active.
	<-verifierReady
	logger.Info().Msg("Pub/Sub verifier is ready and listening.")

	// 7. Run Load Generator
	logger.Info().Msg("Starting MQTT load generator...")
	loadgenClient := loadgen.NewMqttClient(mqttContainer.EmulatorAddress, "devices/%s/data", 1, logger)
	devices := make([]*loadgen.Device, loadTestNumDevices)
	for i := 0; i < loadTestNumDevices; i++ {
		devices[i] = &loadgen.Device{ID: fmt.Sprintf("e2e-device-%d", i), MessageRate: loadTestRate, PayloadGenerator: &testPayloadGenerator{}}
	}
	generator := loadgen.NewLoadGenerator(loadgenClient, devices, logger)
	err = generator.Run(ctx, loadTestDuration)
	require.NoError(t, err)
	logger.Info().Msg("Load generator finished.")

	logger.Info().Msg("Waiting for messages to be processed...")
	time.Sleep(10 * time.Second)

	// 8. Wait for verification
	logger.Info().Msg("Waiting for Pub/Sub verification to complete...")
	select {
	case <-verificationDone:
		logger.Info().Msg("Verification successful!")
	case <-ctx.Done():
		t.Fatal("Test timed out waiting for verification")
	}
}
