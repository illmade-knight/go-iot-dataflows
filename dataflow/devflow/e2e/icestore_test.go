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

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-cloud-manager/pkg/servicemanager"
	"github.com/illmade-knight/go-iot-dataflows/builder"
	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/helpers/loadgen"
	"github.com/rs/zerolog"
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
	// --- Logger and Prerequisite Checks ---
	logger := zerolog.New(os.Stderr).With().Timestamp().Str("test", "TestIceStoreDataflowE2E").Logger()

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

		logger.Info().Msg("\n--- Test Timing & Metrics Breakdown ---")
		for name, d := range timings {
			logger.Info().Msgf("%-35s: %s", name, d)
		}
		logger.Info().Msg("------------------------------------")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// 1. Define unique resources for this test run.
	runID := uuid.New().String()[:8]
	dataflowName := fmt.Sprintf("icestore-flow-%s", runID)
	uniqueTopicID := fmt.Sprintf("icestore-ingestion-topic-%s", runID)
	uniqueIcestoreSubID := fmt.Sprintf("icestore-sub-%s", runID)
	uniqueBucketName := fmt.Sprintf("sm-icestore-bucket-%s", runID) // Prefixed for clarity
	logger.Info().Str("run_id", runID).Msg("Generated unique resources for test run")

	// 2. Build the services definition in memory using the new architecture struct.
	servicesConfig := &servicemanager.MicroserviceArchitecture{
		Environment: servicemanager.Environment{
			Name:      "e2e-icestore",
			ProjectID: projectID,
			Location:  "US",
		},
		Dataflows: map[string]servicemanager.ResourceGroup{
			dataflowName: {
				Name:      dataflowName,
				Lifecycle: &servicemanager.LifecyclePolicy{Strategy: servicemanager.LifecycleStrategyEphemeral},
				Resources: servicemanager.CloudResourcesSpec{
					Topics:        []servicemanager.TopicConfig{{CloudResource: servicemanager.CloudResource{Name: uniqueTopicID}}},
					Subscriptions: []servicemanager.SubscriptionConfig{{CloudResource: servicemanager.CloudResource{Name: uniqueIcestoreSubID}, Topic: uniqueTopicID}},
					GCSBuckets:    []servicemanager.GCSBucket{{CloudResource: servicemanager.CloudResource{Name: uniqueBucketName}, Location: "US"}},
				},
			},
		},
	}

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
	directorService, directorURL := startServiceDirector(t, ctx, logger.With().Str("service", "servicedirector").Logger(), servicesConfig, nil)
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
	gcsClientForVerification, err := storage.NewClient(ctx, opts...)
	require.NoError(t, err)
	defer gcsClientForVerification.Close()

	verifyGCSResults(t, logger, ctx, gcsClientForVerification, uniqueBucketName, publishedCount)
	timings["VerificationDuration"] = time.Since(verificationStart).String()
	timings["ProcessingAndVerificationLatency"] = time.Since(loadgenStart).String()
	verifiedCount = publishedCount // If verifyGCSResults passes, verified count equals published count.
}
