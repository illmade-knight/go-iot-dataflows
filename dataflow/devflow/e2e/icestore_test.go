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

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-iot-dataflows/builder"
	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/helpers/loadgen"
	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	generateIcestoreMessagesFor = 3 * time.Second
	icestoreLoadTestNumDevices  = 2
	icestoreLoadTestRate        = 5.0
)

func TestIceStoreDataflowE2E(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	logger := log.With().Str("test", "TestIceStoreDataflowE2E").Logger()

	// 1. Define unique resources for this test run.
	runID := uuid.New().String()[:8]
	dataflowName := fmt.Sprintf("icestore-flow-%s", runID)
	uniqueTopicID := fmt.Sprintf("icestore-ingestion-topic-%s", runID)
	uniqueIcestoreSubID := fmt.Sprintf("icestore-sub-%s", runID)
	uniqueBucketName := fmt.Sprintf("icestore-bucket-%s", runID)
	logger.Info().Str("run_id", runID).Msg("Generated unique resources for test run")

	// 2. Build the services definition in memory.
	servicesConfig := &servicemanager.TopLevelConfig{
		DefaultProjectID: projectID,
		Environments:     map[string]servicemanager.EnvironmentSpec{"dev": {ProjectID: projectID}},
		Dataflows: []servicemanager.ResourceGroup{{
			Name:      dataflowName,
			Lifecycle: &servicemanager.LifecyclePolicy{Strategy: servicemanager.LifecycleStrategyEphemeral},
			Resources: servicemanager.ResourcesSpec{
				Topics:        []servicemanager.TopicConfig{{Name: uniqueTopicID}},
				Subscriptions: []servicemanager.SubscriptionConfig{{Name: uniqueIcestoreSubID, Topic: uniqueTopicID}},
				GCSBuckets:    []servicemanager.GCSBucket{{Name: uniqueBucketName}},
			},
		}},
	}
	servicesDef, err := servicemanager.NewInMemoryServicesDefinition(servicesConfig)
	require.NoError(t, err)

	// 3. Setup dependencies.
	var opts []option.ClientOption
	if creds := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); creds != "" {
		opts = append(opts, option.WithCredentialsFile(creds))
	}
	start := time.Now()
	mqttConnInfo := emulators.SetupMosquittoContainer(t, ctx, emulators.GetDefaultMqttImageContainer())
	timings["EmulatorSetup(MQTT)"] = time.Since(start).String()

	gcsClient, err := storage.NewClient(ctx, opts...)
	require.NoError(t, err)
	defer gcsClient.Close()

	// 4. Start ServiceDirector and orchestrate resources.
	start = time.Now()
	directorService, directorURL := startServiceDirector(t, ctx, logger.With().Str("service", "servicedirector").Logger(), servicesDef)
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
		cleanupCtx := context.Background()
		logger.Info().Msg("Requesting resource teardown from ServiceDirector...")
		teardownURL := directorURL + "/orchestrate/teardown"
		req, _ := http.NewRequestWithContext(cleanupCtx, http.MethodPost, teardownURL, nil)
		http.DefaultClient.Do(req)

		logger.Info().Str("bucket", uniqueBucketName).Msg("Ensuring GCS bucket is deleted directly as a fallback.")
		bucket := gcsClient.Bucket(uniqueBucketName)
		it := bucket.Objects(cleanupCtx, nil)
		for {
			attrs, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err == nil {
				bucket.Object(attrs.Name).Delete(cleanupCtx)
			}
		}
		bucket.Delete(cleanupCtx)
		timings["CloudResourceTeardown"] = time.Since(teardownStart).String()
	})

	// 5. Start services
	start = time.Now()
	var ingestionSvc, icestoreSvc builder.Service
	require.Eventually(t, func() bool {
		ingestionSvc, err = startIngestionService(t, logger.With().Str("service", "ingestion").Logger(), directorURL, mqttConnInfo.EmulatorAddress, projectID, uniqueTopicID, dataflowName)
		return err == nil
	}, 30*time.Second, 5*time.Second, "Ingestion service failed to start in time")
	timings["ServiceStartup(Ingestion)"] = time.Since(start).String()
	t.Cleanup(ingestionSvc.Shutdown)

	start = time.Now()
	require.Eventually(t, func() bool {
		icestoreSvc, err = startIceStoreService(t, logger.With().Str("service", "icestore").Logger(), directorURL, projectID, uniqueIcestoreSubID, uniqueBucketName, dataflowName)
		return err == nil
	}, 30*time.Second, 5*time.Second, "IceStore service failed to start in time")
	timings["ServiceStartup(IceStore)"] = time.Since(start).String()
	t.Cleanup(icestoreSvc.Shutdown)
	logger.Info().Msg("All services started successfully.")

	// 6. Run Load Generator.
	loadgenStart := time.Now()
	logger.Info().Msg("Starting MQTT load generator...")
	loadgenClient := loadgen.NewMqttClient(mqttConnInfo.EmulatorAddress, "devices/+/data", 1, logger)

	devices := make([]*loadgen.Device, icestoreLoadTestNumDevices)
	for i := 0; i < icestoreLoadTestNumDevices; i++ {
		devices[i] = &loadgen.Device{ID: fmt.Sprintf("e2e-icestore-device-%d-%s", i, runID), MessageRate: icestoreLoadTestRate, PayloadGenerator: &testPayloadGenerator{}}
	}

	generator := loadgen.NewLoadGenerator(loadgenClient, devices, logger)
	expectedMessageCount = generator.ExpectedMessagesForDuration(generateIcestoreMessagesFor)
	publishedCount, err = generator.Run(ctx, generateIcestoreMessagesFor)
	require.NoError(t, err)
	timings["LoadGeneration"] = time.Since(loadgenStart).String()
	logger.Info().Int("published_count", publishedCount).Msg("Load generator finished.")

	// 7. Verify results.
	verificationStart := time.Now()
	// Use a new client for verification to ensure it has the correct context.
	gcsClientForVerification, err := storage.NewClient(ctx, opts...)
	require.NoError(t, err)
	defer gcsClientForVerification.Close()

	verifyGCSResults(t, logger, ctx, gcsClientForVerification, uniqueBucketName, publishedCount)
	timings["VerificationDuration"] = time.Since(verificationStart).String()
	timings["ProcessingAndVerificationLatency"] = time.Since(loadgenStart).String()
	verifiedCount = publishedCount // If verifyGCSResults passes, verified count equals published count.
}
