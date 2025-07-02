//go:build integration

package e2e

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
)

// MessageValidationFunc defines a function for custom validation logic.
// It is injected into the generic verifier to check message contents.
// It should return 'true' if the message is valid and should be counted.
type MessageValidationFunc func(t *testing.T, msg *pubsub.Message) bool

// verifyPubSubMessages is a generic, reusable verifier. It handles the core logic
// of receiving messages and checking counts, while delegating custom validation.
func verifyPubSubMessages(
	t *testing.T,
	logger zerolog.Logger,
	ctx context.Context,
	sub *pubsub.Subscription,
	expectedCountCh <-chan int,
	validator MessageValidationFunc,
) int {
	t.Helper()

	verifierLogger := logger.With().Str("component", "PubSubVerifier").Str("subscription", sub.ID()).Logger()

	var receivedCount atomic.Int32
	var targetCount atomic.Int32
	targetCount.Store(-1) // Initialize to -1 to indicate no target set yet

	var mu sync.Mutex
	milestoneTimings := make(map[string]time.Duration)
	receiverStartTime := time.Now()

	receiveCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Goroutine to listen for the expected count from the load generator
	go func() {
		verifierLogger.Info().Msg("Verifier is ready and listening for messages and expected count...")

		select {
		case count, ok := <-expectedCountCh:
			if !ok {
				verifierLogger.Warn().Msg("Expected count channel was closed before a count was sent.")
				return
			}
			targetCount.Store(int32(count))
			verifierLogger.Info().Int("expected_exact", count).Msg("Verifier received exact count from load generator.")

			// Check if target is already met by messages that arrived very quickly
			if receivedCount.Load() >= targetCount.Load() {
				verifierLogger.Info().Msg("Target count already met on receipt of target count.")
				cancel() // Cancel receiveCtx if target already met
			}
		case <-receiveCtx.Done():
			verifierLogger.Info().Msg("Receive context cancelled. Stopping expected count listener.")
			return
		}
	}()

	// Start receiving messages
	err := sub.Receive(receiveCtx, func(receiveCallbackCtx context.Context, msg *pubsub.Message) {
		// Always acknowledge the message to prevent redelivery.
		msg.Ack()

		// If a custom validator is provided, use it to filter messages.
		// If the message is not valid, we stop processing it here.
		if validator != nil {
			if !validator(t, msg) {
				return
			}
		}

		// Only increment the count for messages that passed validation (or if no validator was provided).
		newCount := receivedCount.Add(1)

		mu.Lock()
		if newCount == 1 {
			milestoneTimings["First Valid Message"] = time.Since(receiverStartTime)
		}

		// Check if the number of *valid* messages has reached the target.
		if targetCount.Load() >= 0 && newCount >= targetCount.Load() {
			if _, ok := milestoneTimings["Final Valid Message"]; !ok {
				milestoneTimings["Final Valid Message"] = time.Since(receiverStartTime)
			}
			// Cancel the receive context to stop the loop.
			cancel()
			verifierLogger.Info().Int32("received", newCount).Int32("target", targetCount.Load()).Msg("Target count reached. Signalling receive completion.")
		}
		mu.Unlock()
	})

	// A Canceled error is expected upon graceful shutdown, so we ignore it.
	if err != nil && err != context.Canceled {
		t.Errorf("Pub/Sub Receive returned an unexpected error: %v", err)
	}

	finalCount := int(receivedCount.Load())
	verifierLogger.Info().Int("final_valid_count", finalCount).Msg("Verifier finished receiving.")

	t.Log("--- Verification Milestone Timings ---")
	mu.Lock()
	for name, duration := range milestoneTimings {
		t.Logf("Time to receive %-20s: %v", name, duration)
	}
	mu.Unlock()
	t.Log("------------------------------------")

	// Assert that the final count of valid messages matches the expected count.
	if targetCount.Load() >= 0 {
		require.Equal(t, int(targetCount.Load()), finalCount, "Did not receive the exact number of expected (and valid) messages.")
	} else {
		verifierLogger.Warn().Msg("No valid expected count was ever provided. Cannot assert exact message count.")
	}
	return finalCount
}

// BQRowValidator defines a function to validate a set of BigQuery rows.
// It returns an error if validation fails, which the polling function will catch.
type BQRowValidator func(t *testing.T, iter *bq.RowIterator) error

// verifyBigQueryRows polls BigQuery until a target row count is met,
// then runs a final validation using the provided validator function.
func verifyBigQueryRows(
	t *testing.T,
	logger zerolog.Logger,
	ctx context.Context,
	projectID, datasetID, tableID string,
	expectedCount int,
	validator BQRowValidator,
) {
	t.Helper()
	verifierLogger := logger.With().Str("component", "BQVerifier").Str("table", tableID).Logger()

	client, err := bq.NewClient(ctx, projectID)
	require.NoError(t, err)
	defer client.Close()

	// 1. Poll until the expected number of rows appears.
	countQuery := fmt.Sprintf("SELECT COUNT(*) as count FROM `%s.%s.%s`", projectID, datasetID, tableID)
	require.Eventually(t, func() bool {
		q := client.Query(countQuery)
		it, err := q.Read(ctx)
		if err != nil {
			verifierLogger.Warn().Err(err).Msg("Polling: Failed to query BQ for row count")
			return false
		}
		var row struct{ Count int64 }
		if err := it.Next(&row); err != nil {
			return false
		}
		verifierLogger.Info().Int64("current_count", row.Count).Int("expected", expectedCount).Msg("Polling BQ results...")
		return row.Count >= int64(expectedCount)
	}, 90*time.Second, 5*time.Second, "BigQuery row count did not meet threshold in time")

	// 2. Once the count is met, run the full validation logic.
	verifierLogger.Info().Msg("Expected row count reached. Running final validation...")
	fullQuery := fmt.Sprintf("SELECT * FROM `%s.%s.%s`", projectID, datasetID, tableID)
	it, err := client.Query(fullQuery).Read(ctx)
	require.NoError(t, err, "Failed to query for full BQ results")

	err = validator(t, it)
	require.NoError(t, err, "BigQuery row validation failed")

	verifierLogger.Info().Msg("BigQuery validation successful!")
}

// CORRECTED: verifyGCSResults now polls for an exact number of records and uses structured logging.
func verifyGCSResults(t *testing.T, logger zerolog.Logger, ctx context.Context, gcsClient *storage.Client, bucketName string, expectedCount int) {
	t.Helper()
	verifierLogger := logger.With().Str("component", "GCSVerifier").Str("bucket", bucketName).Logger()

	var totalCount int
	require.Eventually(t, func() bool {
		totalCount = 0
		bucket := gcsClient.Bucket(bucketName)
		it := bucket.Objects(ctx, nil)
		for {
			attrs, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				// In a test environment, transient errors can occur. Log and continue polling.
				verifierLogger.Warn().Err(err).Msg("Failed to list GCS objects during polling")
				return false
			}

			rc, err := bucket.Object(attrs.Name).NewReader(ctx)
			if err != nil {
				verifierLogger.Warn().Err(err).Str("object", attrs.Name).Msg("Failed to create GCS object reader")
				continue
			}

			gzr, err := gzip.NewReader(rc)
			if err != nil {
				rc.Close()
				verifierLogger.Warn().Err(err).Str("object", attrs.Name).Msg("Failed to create gzip reader")
				continue
			}

			scanner := bufio.NewScanner(gzr)
			for scanner.Scan() {
				totalCount++
			}
			gzr.Close()
			rc.Close()
		}

		verifierLogger.Info().Int("current_count", totalCount).Int("expected_exact", expectedCount).Msg("Polling GCS results...")
		return totalCount >= expectedCount

	}, 90*time.Second, 5*time.Second, "GCS object count did not meet threshold in time")

	// Final assertion for the exact count.
	require.Equal(t, expectedCount, totalCount, "Did not find the exact number of expected records in GCS")
}
