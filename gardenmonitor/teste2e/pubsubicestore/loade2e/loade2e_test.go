//go:build integration

package loade2e_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-iot-dataflows/gardenmonitor/icestore/icinit"
	"github.com/illmade-knight/go-iot/pkg/icestore"
	"github.com/illmade-knight/go-iot/pkg/messagepipeline"
	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/illmade-knight/go-iot/pkg/types"
	"google.golang.org/api/iterator"

	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/illmade-knight/go-iot/helpers/emulators"
	"github.com/illmade-knight/go-iot/helpers/loadgen"
)

// --- Test Flags ---
var (
	keepResources = flag.Bool("keep-resources", false, "If true, ephemeral resources (like GCS buckets) will not be deleted after the test.")
)

// --- Test Constants ---
const (
	cloudTestGCPProjectID  = "gemini-power-test"
	loadTestDuration       = 30 * time.Second
	loadTestNumDevices     = 10
	loadTestRatePerDevice  = 2.0
	cloudLoadTestTimeout   = 10 * time.Minute
	cloudTestGCSHTTPPort   = ":9095"
	cloudGCSBucketPrefix   = "e2e-garden-monitor-loadtest-"
	maxBucketNameLength    = 63
	testMqttTopicPattern   = "devices/+/data"
	testMqttClientIDPrefix = "ingestion-service-cloud-load"
	cloudTestMqttHTTPPort  = ":9092"
	cloudTestBqHTTPPort    = ":9093"
)

// buildTestServicesDefinitionGCS creates a complete TopLevelConfig in memory for the GCS load test,
// conforming to the new ResourceGroup structure.
func buildTestServicesDefinitionGCS(runID, gcpProjectID string) *servicemanager.TopLevelConfig {
	dataflowName := "mqtt-to-gcs-loadtest"
	topicID := fmt.Sprintf("processed-device-data-gcs-%s", runID)
	subscriptionID := fmt.Sprintf("archival-service-sub-gcs-%s", runID)
	bucketName := fmt.Sprintf("%s%s", cloudGCSBucketPrefix, runID)

	// Sanitize bucket name for GCS compliance
	bucketName = strings.ToLower(strings.ReplaceAll(bucketName, "_", "-"))
	if len(bucketName) > maxBucketNameLength {
		bucketName = bucketName[:maxBucketNameLength]
	}

	return &servicemanager.TopLevelConfig{
		DefaultProjectID: gcpProjectID,
		Environments: map[string]servicemanager.EnvironmentSpec{
			"loadtest": {ProjectID: gcpProjectID},
		},
		Services: []servicemanager.ServiceSpec{
			{Name: "ingestion-service"},
			{Name: "archival-service"},
		},
		Dataflows: []servicemanager.ResourceGroup{
			{
				Name:         dataflowName,
				ServiceNames: []string{"ingestion-service", "archival-service"},
				Lifecycle: &servicemanager.LifecyclePolicy{
					Strategy:            servicemanager.LifecycleStrategyEphemeral,
					KeepResourcesOnTest: *keepResources,
				},
				Resources: servicemanager.ResourcesSpec{
					Topics: []servicemanager.TopicConfig{
						{Name: topicID, ProducerService: "ingestion-service"},
					},
					Subscriptions: []servicemanager.SubscriptionConfig{
						{Name: subscriptionID, Topic: topicID, ConsumerService: "archival-service"},
					},
					GCSBuckets: []servicemanager.GCSBucket{
						{
							Name:              bucketName,
							AccessingServices: []string{"archival-service"},
						},
					},
				},
			},
		},
	}
}

// startGCSProcessingService has been updated to use the correct signature for NewGooglePubsubConsumer.
func startGCSProcessingService(t *testing.T, ctx context.Context, gcpProjectID, subID, bucketName string) (*messagepipeline.ProcessingService[icestore.ArchivalData], *icinit.Server) {
	t.Helper()
	icestoreCfg := &icinit.IceServiceConfig{
		LogLevel:  "info",
		HTTPPort:  cloudTestGCSHTTPPort,
		ProjectID: gcpProjectID,
		Consumer: struct {
			SubscriptionID  string `mapstructure:"subscription_id"`
			CredentialsFile string `mapstructure:"credentials_file"`
		}{SubscriptionID: subID},
		IceStore: struct {
			CredentialsFile string `mapstructure:"credentials_file"`
			BucketName      string `mapstructure:"bucket_name"`
		}{BucketName: bucketName},
		BatchProcessing: struct {
			NumWorkers   int           `mapstructure:"num_workers"`
			BatchSize    int           `mapstructure:"batch_size"`
			FlushTimeout time.Duration `mapstructure:"flush_timeout"`
		}{NumWorkers: 5, BatchSize: 4, FlushTimeout: time.Second * 10},
	}

	gcsLogger := log.With().Str("service", "gcs-processor").Logger()
	gcsClient, err := storage.NewClient(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { gcsClient.Close() })

	pubsubClient, err := pubsub.NewClient(ctx, gcpProjectID)
	require.NoError(t, err)
	t.Cleanup(func() { pubsubClient.Close() })

	// Retry loop to wait for the bucket to become available.
	bucketHandle := gcsClient.Bucket(bucketName)
	require.Eventually(t, func() bool {
		_, err := bucketHandle.Attrs(ctx)
		if err == nil {
			gcsLogger.Info().Str("bucket", bucketName).Msg("Successfully confirmed GCS bucket exists.")
			return true
		}
		if !errors.Is(err, storage.ErrBucketNotExist) {
			t.Fatalf("Failed to get bucket attributes with unexpected error: %v", err)
		}
		gcsLogger.Warn().Str("bucket", bucketName).Msg("Bucket not yet found, retrying...")
		return false
	}, 30*time.Second, 2*time.Second, "GCS bucket %s was not found after multiple retries", bucketName)

	// Retry loop for the Pub/Sub subscription.
	subscription := pubsubClient.Subscription(subID)
	require.Eventually(t, func() bool {
		exists, err := subscription.Exists(ctx)
		require.NoError(t, err, "Failed to check for subscription existence with an unexpected error")
		if exists {
			gcsLogger.Info().Str("subscription", subID).Msg("Successfully confirmed Pub/Sub subscription exists.")
			return true
		}
		gcsLogger.Warn().Str("subscription", subID).Msg("Subscription not yet found, retrying...")
		return false
	}, 30*time.Second, 2*time.Second, "Pub/Sub subscription %s was not found after multiple retries", subID)

	// Correctly create the consumer with the client.
	gcsConsumer, err := messagepipeline.NewGooglePubsubConsumer(&messagepipeline.GooglePubsubConsumerConfig{
		ProjectID:      icestoreCfg.ProjectID,
		SubscriptionID: icestoreCfg.Consumer.SubscriptionID,
	}, pubsubClient, gcsLogger)
	require.NoError(t, err)

	batcher, err := icestore.NewGCSBatchProcessor(
		icestore.NewGCSClientAdapter(gcsClient),
		&icestore.BatcherConfig{BatchSize: icestoreCfg.BatchProcessing.BatchSize, FlushTimeout: icestoreCfg.BatchProcessing.FlushTimeout},
		icestore.GCSBatchUploaderConfig{BucketName: bucketName, ObjectPrefix: "archived-data"},
		gcsLogger,
	)
	require.NoError(t, err)

	processingService, err := icestore.NewIceStorageService(icestoreCfg.BatchProcessing.NumWorkers, gcsConsumer, batcher, icestore.ArchivalTransformer, gcsLogger)
	require.NoError(t, err)

	gcsServer := icinit.NewServer(icestoreCfg, processingService, gcsLogger)
	go func() {
		if srvErr := gcsServer.Start(); srvErr != nil && !errors.Is(srvErr, http.ErrServerClosed) {
			t.Errorf("GCS Processing server failed: %v", srvErr)
		}
	}()
	return processingService, gcsServer
}

// TestManagedCloudGCSSLoad subjects the entire pipeline to load, using the ServiceManager for setup and teardown.
func TestManagedCloudGCSSLoad(t *testing.T) {
	t.Setenv("GOOGLE_CLOUD_PROJECT", cloudTestGCPProjectID)
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		t.Skip("Skipping cloud GCS load test: GOOGLE_CLOUD_PROJECT environment variable must be set.")
	}
	t.Setenv("GOOGLE_CLOUD_PROJECT", projectID)

	ctx, cancel := context.WithTimeout(context.Background(), cloudLoadTestTimeout)
	defer cancel()
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	testLogger := log.Logger

	runID := uuid.New().String()[:8]
	servicesConfig := buildTestServicesDefinitionGCS(runID, projectID)

	servicesDef, err := servicemanager.NewInMemoryServicesDefinition(servicesConfig)
	require.NoError(t, err)

	schemaRegistry := make(map[string]interface{}) // Not used by GCS flow but required by manager
	schemaRegistry["github.com/illmade-knight/go-iot/pkg/types.GardenMonitorReadings"] = types.GardenMonitorReadings{}

	log.Info().Str("runID", runID).Msg("Initializing ServiceManager to provision dataflow...")
	manager, err := servicemanager.NewServiceManager(ctx, servicesDef, "loadtest", schemaRegistry, testLogger)
	require.NoError(t, err)

	dataflowName := servicesConfig.Dataflows[0].Name
	provisioned, err := manager.SetupDataflow(ctx, "loadtest", dataflowName)
	require.NoError(t, err, "ServiceManager.SetupDataflow failed")

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		log.Info().Str("runID", runID).Msg("Test finished. Tearing down dataflow resources...")
		teardownErr := manager.TeardownDataflow(cleanupCtx, "loadtest", dataflowName)
		assert.NoError(t, teardownErr, "ServiceManager.TeardownDataflow failed")
	})

	// Retrieve resource names from the new ProvisionedResources struct fields
	require.NotEmpty(t, provisioned.GCSBuckets, "Provisioning must return a GCS bucket")
	bucketName := provisioned.GCSBuckets[0].Name
	require.NotEmpty(t, provisioned.Topics, "Provisioning must return a Pub/Sub topic")
	topicID := provisioned.Topics[0].Name
	require.NotEmpty(t, provisioned.Subscriptions, "Provisioning must return a Pub/Sub subscription")
	subscriptionID := provisioned.Subscriptions[0].Name

	log.Info().Str("bucket", bucketName).Str("topic", topicID).Str("subscription", subscriptionID).Msg("Using provisioned resources")

	log.Info().Msg("LoadTest: Setting up Mosquitto container...")
	mqttConnections := emulators.SetupMosquittoContainer(t, ctx, emulators.GetDefaultMqttImageContainer())

	_, mqttServer := startIngestionService(t, ctx, projectID, mqttConnections.EmulatorAddress, topicID)
	defer mqttServer.Shutdown()

	_, gcsServer := startGCSProcessingService(t, ctx, projectID, subscriptionID, bucketName)
	defer gcsServer.Shutdown()

	log.Info().Msg("LoadTest: Pausing to allow services to start and connect...")
	time.Sleep(10 * time.Second)

	log.Info().Msg("LoadTest: Configuring and starting load generator...")
	loadgenLogger := log.With().Str("service", "load-generator").Logger()
	loadgenClient := loadgen.NewMqttClient(mqttConnections.EmulatorAddress, testMqttTopicPattern, 1, loadgenLogger)
	devices := make([]*loadgen.Device, loadTestNumDevices)
	for i := 0; i < loadTestNumDevices; i++ {
		devices[i] = &loadgen.Device{
			ID:               fmt.Sprintf("load-test-device-%d", i),
			MessageRate:      loadTestRatePerDevice,
			PayloadGenerator: &gardenMonitorPayloadGenerator{},
		}
	}

	loadGenerator := loadgen.NewLoadGenerator(loadgenClient, devices, loadgenLogger)
	err = loadGenerator.Run(ctx, loadTestDuration)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		require.NoError(t, err, "Load generator returned an unexpected error")
	}

	log.Info().Msg("Load generation complete. Verifying results in GCS...")
	verifyGCSResults(t, ctx, bucketName)
}

// verifyGCSResults polls the GCS bucket until the object count stabilizes, then asserts success.
func verifyGCSResults(t *testing.T, ctx context.Context, bucketName string) {
	t.Helper()
	expectedMinGCSObjects := 1 // At least one file should be created

	gcsClient, err := storage.NewClient(ctx)
	require.NoError(t, err)
	defer gcsClient.Close()
	bucketHandle := gcsClient.Bucket(bucketName)

	var lastObjectCount = -1
	require.Eventually(t, func() bool {
		currentObjects, err := listGCSObjectAttrs(t, ctx, bucketHandle)
		if err != nil {
			log.Warn().Err(err).Msg("Polling GCS objects failed")
			return false
		}
		currentObjectCount := len(currentObjects)
		log.Info().Int("current_count", currentObjectCount).Int("last_count", lastObjectCount).Msg("Polling GCS object count")

		if currentObjectCount == lastObjectCount && currentObjectCount >= expectedMinGCSObjects {
			log.Info().Msg("GCS object count has stabilized and meets the success threshold. Verification successful!")
			return true
		}
		lastObjectCount = currentObjectCount
		return false
	}, 5*time.Minute, 15*time.Second, "Test timed out waiting for GCS objects to stabilize")
}

// listGCSObjectAttrs is a helper function to list objects in a GCS bucket.
func listGCSObjectAttrs(t *testing.T, ctx context.Context, bucket *storage.BucketHandle) ([]*storage.ObjectAttrs, error) {
	t.Helper()
	var attrs []*storage.ObjectAttrs
	it := bucket.Objects(ctx, nil)
	for {
		objAttrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}
		attrs = append(attrs, objAttrs)
	}
	return attrs, nil
}
