//go:build integration

package e2e

import (
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/api/iterator"
	"os"
	"testing"
	"time"

	"github.com/illmade-knight/go-iot/helpers/loadgen"
)

func checkGCPAuth(t *testing.T) {
	t.Helper()

	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID == "" {
		t.Skip("Skipping E2E test: GOOGLE_CLOUD_PROJECT env var must be set.")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Step 1: Create the client. This finds credentials but doesn't validate them.
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		// This can still fail if there's a fundamental setup issue.
		t.Fatalf("Failed to create Pub/Sub client for auth check: %v", err)
	}
	defer client.Close()

	// Step 2: Force the authentication handshake by making a real API call.
	// We try to list topics; this is a lightweight, read-only operation.
	it := client.Topics(ctx)
	_, err = it.Next()

	// If the error is iterator.Done, it means there are no topics, which is a success for our auth check.
	// Any other error indicates a problem, most likely with authentication.
	if err != nil && err != iterator.Done {
		t.Fatalf(`
		---------------------------------------------------------------------
		GCP AUTHENTICATION FAILED!
		---------------------------------------------------------------------
		The first API call to Google Cloud failed. This is most likely due
		to expired or missing Application Default Credentials (ADC).

		To fix this, please run the following command in your terminal:

		gcloud auth application-default login

		Original Error: %v
		---------------------------------------------------------------------
		`, err)
	}

	// If we reach here, the authentication check was successful.
}

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

// --- Load Generation Helper ---

// testPayloadGenerator implements the loadgen.PayloadGenerator for our E2E tests.
type testPayloadGenerator struct{}

// GeneratePayload creates a simple JSON payload for testing.
func (g *testPayloadGenerator) GeneratePayload(device *loadgen.Device) ([]byte, error) {
	return json.Marshal(TestPayload{DeviceID: device.ID, Timestamp: time.Now().UTC(), Value: 123.45})
}
