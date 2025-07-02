//go:build integration

package e2e

import (
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/illmade-knight/go-iot/helpers/loadgen"
	"github.com/illmade-knight/go-iot/pkg/servicemanager"
)

// TestPayload is the data structure for the simple ingestion test.
type TestPayload struct {
	DeviceID  string    `json:"device_id" bigquery:"device_id"`
	Timestamp time.Time `json:"timestamp" bigquery:"timestamp"`
	Value     float64   `json:"value"     bigquery:"value"`
}

// EnrichedTestPayload defines the final, flat schema for the BigQuery table.
// This struct is used by the `startEnrichedBigQueryService` helper to correctly
// transform the `types.PublishMessage` into the format expected by the database.
type EnrichedTestPayload struct {
	DeviceID   string    `json:"device_id" bigquery:"device_id"`
	Timestamp  time.Time `json:"timestamp" bigquery:"timestamp"`
	Value      float64   `json:"value"     bigquery:"value"`
	ClientID   string    `json:"clientID"   bigquery:"clientID"`
	LocationID string    `json:"locationID" bigquery:"locationID"`
	Category   string    `json:"category"   bigquery:"category"`
}

// DeviceIDExtractor is a test-local implementation of the AttributeExtractor interface.
type DeviceIDExtractor struct{}

// Extract parses the JSON payload to get the device_id for test messages.
func (e *DeviceIDExtractor) Extract(payload []byte) (map[string]string, error) {
	var tempPayload struct {
		DeviceID string `json:"device_id"`
	}
	if err := json.Unmarshal(payload, &tempPayload); err != nil {
		return nil, fmt.Errorf("e2e extractor failed to unmarshal payload: %w", err)
	}
	if tempPayload.DeviceID == "" {
		return nil, fmt.Errorf("e2e extractor: device_id field is empty in payload")
	}
	return map[string]string{"uid": tempPayload.DeviceID}, nil
}

// bucketHandle is your *storage.BucketHandle instance
func getGCSBucketURL(bucketHandle *storage.BucketHandle) string {
	bucketName := bucketHandle.BucketName()
	return fmt.Sprintf("gcs://%s/", bucketName)
}

// If you have an object within the bucket, you'd append its path:
func getGCSObjectURL(bucketHandle *storage.BucketHandle, objectPath string) string {
	bucketName := bucketHandle.BucketName()
	// Ensure objectPath doesn't start with a slash if you're constructing it this way
	// or handle double slashes if it might.
	return fmt.Sprintf("gcs://%s/%s", bucketName, objectPath)
}

// InMemoryServicesDefinitionLoader is a helper for using in-memory configurations.
type InMemoryServicesDefinitionLoader struct {
	Def servicemanager.ServicesDefinition
}

// Load returns the in-memory services definition.
func (l *InMemoryServicesDefinitionLoader) Load(ctx context.Context) (servicemanager.ServicesDefinition, error) {
	if l.Def == nil {
		return nil, fmt.Errorf("in-memory services definition is nil")
	}
	return l.Def, nil
}

// --- Load Generation Helper ---

// testPayloadGenerator implements the loadgen.PayloadGenerator for our E2E tests.
type testPayloadGenerator struct{}

// GeneratePayload creates a simple JSON payload for testing.
func (g *testPayloadGenerator) GeneratePayload(device *loadgen.Device) ([]byte, error) {
	return json.Marshal(TestPayload{DeviceID: device.ID, Timestamp: time.Now().UTC(), Value: 123.45})
}

// createUniqueTestConfig creates a deep copy of a services configuration and
// applies unique names to its resources for an isolated test run.
func createUniqueTestConfig(t *testing.T, originalConfig *servicemanager.TopLevelConfig, runID string) *servicemanager.TopLevelConfig {
	t.Helper()

	testConfig := &servicemanager.TopLevelConfig{
		DefaultProjectID: originalConfig.DefaultProjectID,
		Environments:     make(map[string]servicemanager.EnvironmentSpec),
		Services:         make([]servicemanager.ServiceSpec, len(originalConfig.Services)),
		Dataflows:        make([]servicemanager.ResourceGroup, len(originalConfig.Dataflows)),
	}

	for k, v := range originalConfig.Environments {
		testConfig.Environments[k] = v
	}
	copy(testConfig.Services, originalConfig.Services)

	for i, df := range originalConfig.Dataflows {
		dfCopy := servicemanager.ResourceGroup{
			Name:         df.Name,
			Description:  df.Description,
			ServiceNames: append([]string(nil), df.ServiceNames...),
			Lifecycle: &servicemanager.LifecyclePolicy{
				Strategy: df.Lifecycle.Strategy,
			},
			Resources: servicemanager.ResourcesSpec{
				Topics:           make([]servicemanager.TopicConfig, len(df.Resources.Topics)),
				Subscriptions:    make([]servicemanager.SubscriptionConfig, len(df.Resources.Subscriptions)),
				BigQueryDatasets: make([]servicemanager.BigQueryDataset, len(df.Resources.BigQueryDatasets)),
				BigQueryTables:   make([]servicemanager.BigQueryTable, len(df.Resources.BigQueryTables)),
				GCSBuckets:       make([]servicemanager.GCSBucket, len(df.Resources.GCSBuckets)),
			},
		}
		copy(dfCopy.Resources.Topics, df.Resources.Topics)
		copy(dfCopy.Resources.Subscriptions, df.Resources.Subscriptions)
		copy(dfCopy.Resources.BigQueryDatasets, df.Resources.BigQueryDatasets)
		copy(dfCopy.Resources.BigQueryTables, df.Resources.BigQueryTables)
		copy(dfCopy.Resources.GCSBuckets, df.Resources.GCSBuckets)
		testConfig.Dataflows[i] = dfCopy
	}

	nameMapping := make(map[string]string)
	if len(testConfig.Dataflows) == 0 {
		return testConfig
	}

	mainDataflow := &testConfig.Dataflows[0]
	mainDataflow.Lifecycle.Strategy = servicemanager.LifecycleStrategyEphemeral

	rename := func(originalName string) string {
		if originalName == "" {
			return ""
		}
		if _, exists := nameMapping[originalName]; !exists {
			nameMapping[originalName] = fmt.Sprintf("%s-%s", originalName, runID)
		}
		return nameMapping[originalName]
	}

	for i := range mainDataflow.Resources.Topics {
		mainDataflow.Resources.Topics[i].Name = rename(mainDataflow.Resources.Topics[i].Name)
	}
	for i := range mainDataflow.Resources.BigQueryDatasets {
		mainDataflow.Resources.BigQueryDatasets[i].Name = rename(mainDataflow.Resources.BigQueryDatasets[i].Name)
	}
	for i := range mainDataflow.Resources.GCSBuckets {
		mainDataflow.Resources.GCSBuckets[i].Name = rename(mainDataflow.Resources.GCSBuckets[i].Name)
	}
	for i := range mainDataflow.Resources.Subscriptions {
		mainDataflow.Resources.Subscriptions[i].Topic = rename(mainDataflow.Resources.Subscriptions[i].Topic)
		mainDataflow.Resources.Subscriptions[i].Name = rename(mainDataflow.Resources.Subscriptions[i].Name)
	}
	for i := range mainDataflow.Resources.BigQueryTables {
		mainDataflow.Resources.BigQueryTables[i].Dataset = rename(mainDataflow.Resources.BigQueryTables[i].Dataset)
		mainDataflow.Resources.BigQueryTables[i].Name = rename(mainDataflow.Resources.BigQueryTables[i].Name)
	}

	return testConfig
}
