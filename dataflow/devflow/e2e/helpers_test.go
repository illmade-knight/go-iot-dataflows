//go:build integration

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
	"github.com/illmade-knight/go-iot-dataflows/builder"
	"github.com/illmade-knight/go-iot-dataflows/builder/bigquery"
	"github.com/illmade-knight/go-iot-dataflows/builder/enrichment"
	"github.com/illmade-knight/go-iot-dataflows/builder/ingestion"
	"github.com/illmade-knight/go-iot-dataflows/builder/servicedirector"
	"github.com/illmade-knight/go-iot/helpers/loadgen"
	"github.com/illmade-knight/go-iot/pkg/device"
	pkgenrichment "github.com/illmade-knight/go-iot/pkg/enrichment"
	"github.com/illmade-knight/go-iot/pkg/messagepipeline"
	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/illmade-knight/go-iot/pkg/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
)

// TestPayload is the data structure for the simple ingestion test.
type TestPayload struct {
	DeviceID  string    `json:"device_id" bigquery:"device_id"`
	Timestamp time.Time `json:"timestamp" bigquery:"timestamp"`
	Value     float64   `json:"value"     bigquery:"value"`
}

// EnrichedTestPayload is the data structure for the full dataflow test.
type EnrichedTestPayload struct {
	DeviceID   string    `json:"device_id" bigquery:"device_id"`
	Timestamp  time.Time `json:"timestamp" bigquery:"timestamp"`
	Value      float64   `json:"value"     bigquery:"value"`
	ClientID   string    `json:"clientID"   bigquery:"clientID"`
	LocationID string    `json:"locationID" bigquery:"locationID"`
	Category   string    `json:"category"   bigquery:"category"`
}

// --- Service Starting Helpers ---

type InMemoryServicesDefinitionLoader struct {
	Def servicemanager.ServicesDefinition
}

func (l *InMemoryServicesDefinitionLoader) Load(ctx context.Context) (servicemanager.ServicesDefinition, error) {
	if l.Def == nil {
		return nil, fmt.Errorf("in-memory services definition is nil")
	}
	return l.Def, nil
}

// startServiceDirector now accepts a schemaRegistry to pass to the director.
func startServiceDirector(t *testing.T, ctx context.Context, logger zerolog.Logger, servicesDef servicemanager.ServicesDefinition, schemaRegistry ...map[string]interface{}) (builder.Service, string) {
	t.Helper()
	loader := &InMemoryServicesDefinitionLoader{Def: servicesDef}
	cfg := &servicedirector.Config{
		BaseConfig:  builder.BaseConfig{HTTPPort: ":0"},
		Environment: "dev",
	}
	var reg map[string]interface{}
	if len(schemaRegistry) > 0 {
		reg = schemaRegistry[0]
	}
	// The schemaRegistry is now passed when creating the director.
	director, err := servicedirector.NewServiceDirector(ctx, cfg, loader, reg, logger)
	require.NoError(t, err)

	go func() {
		if err := director.Start(); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("ServiceDirector failed during test")
		}
	}()

	var directorURL string
	require.Eventually(t, func() bool {
		port := director.GetHTTPPort()
		if port == "" || port == ":0" {
			return false
		}
		directorURL = fmt.Sprintf("http://localhost%s", port)
		resp, err := http.Get(directorURL + "/healthz")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 10*time.Second, 200*time.Millisecond, "ServiceDirector health check did not become OK")
	return director, directorURL
}

func startIngestionService(t *testing.T, ctx context.Context, logger zerolog.Logger, directorURL, mqttURL, projectID, topicID string) (builder.Service, error) {
	t.Helper()
	cfg := &ingestion.Config{
		BaseConfig:         builder.BaseConfig{LogLevel: "debug", HTTPPort: ":0", ProjectID: projectID},
		ServiceName:        "ingestion-service-e2e",
		DataflowName:       "devflow-main",
		ServiceDirectorURL: directorURL,
	}
	cfg.Publisher.TopicID = topicID
	cfg.MQTT.BrokerURL = mqttURL
	cfg.MQTT.Topic = "devices/+/data"
	cfg.MQTT.ClientIDPrefix = "ingestion-e2e-"
	wrapper, err := ingestion.NewIngestionServiceWrapper(cfg, logger, cfg.ServiceName, cfg.DataflowName)
	if err != nil {
		return nil, err
	}
	go func() {
		if err := wrapper.Start(); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("IngestionService failed")
		}
	}()
	return wrapper, nil
}

func startEnrichmentService(t *testing.T, ctx context.Context, logger zerolog.Logger, directorURL, projectID, subID, topicID, redisAddr, firestoreCollection string) (builder.Service, error) {
	t.Helper()
	cfg := &enrichment.Config{
		BaseConfig:         builder.BaseConfig{LogLevel: "debug", HTTPPort: ":0", ProjectID: projectID},
		ServiceName:        "enrichment-service-e2e",
		DataflowName:       "devflow-main",
		ServiceDirectorURL: directorURL,
	}
	cfg.Consumer.SubscriptionID = subID
	cfg.Producer = &messagepipeline.GooglePubsubProducerConfig{TopicID: topicID}
	cfg.CacheConfig.RedisConfig.Addr = redisAddr
	cfg.CacheConfig.FirestoreConfig = &device.FirestoreFetcherConfig{CollectionName: firestoreCollection}
	cfg.ProcessorConfig.NumWorkers = 5
	wrapper, err := enrichment.NewEnrichmentServiceWrapper[pkgenrichment.EnrichedMessage](cfg, logger, pkgenrichment.NewMessageEnricher)
	if err != nil {
		return nil, err
	}
	go func() {
		if err := wrapper.Start(); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("EnrichmentService failed")
		}
	}()
	return wrapper, nil
}

// startBigQueryService starts a BigQuery service for non-enriched payloads.
func startBigQueryService(t *testing.T, ctx context.Context, logger zerolog.Logger, directorURL, projectID, subID, datasetID, tableID string) (builder.Service, error) {
	t.Helper()
	cfg := &bigquery.Config{
		BaseConfig:         builder.BaseConfig{LogLevel: "debug", HTTPPort: ":0", ProjectID: projectID},
		ServiceName:        "bigquery-service-e2e",
		DataflowName:       "devflow-main",
		ServiceDirectorURL: directorURL,
	}
	cfg.Consumer.SubscriptionID = subID
	cfg.BigQueryConfig.DatasetID = datasetID
	cfg.BigQueryConfig.TableID = tableID
	cfg.BatchProcessing.BatchSize = 10
	cfg.BatchProcessing.NumWorkers = 2
	cfg.BatchProcessing.FlushTimeout = 5 * time.Second
	transformer := func(msg types.ConsumedMessage) (*TestPayload, bool, error) {
		var p TestPayload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			return nil, false, err
		}
		return &p, false, nil
	}
	wrapper, err := bigquery.NewBQServiceWrapper[TestPayload](cfg, logger, transformer)
	if err != nil {
		return nil, err
	}
	go func() {
		if err := wrapper.Start(); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("BigQueryService (simple) failed")
		}
	}()
	return wrapper, nil
}

// startEnrichedBigQueryService starts a BigQuery service for enriched payloads.
func startEnrichedBigQueryService(t *testing.T, ctx context.Context, logger zerolog.Logger, directorURL, projectID, subID, datasetID, tableID string) (builder.Service, error) {
	t.Helper()
	cfg := &bigquery.Config{
		BaseConfig:         builder.BaseConfig{LogLevel: "debug", HTTPPort: ":0", ProjectID: projectID},
		ServiceName:        "bigquery-service-e2e",
		DataflowName:       "devflow-main",
		ServiceDirectorURL: directorURL,
	}
	cfg.Consumer.SubscriptionID = subID
	cfg.BigQueryConfig.DatasetID = datasetID
	cfg.BigQueryConfig.TableID = tableID
	cfg.BatchProcessing.BatchSize = 10
	cfg.BatchProcessing.NumWorkers = 2
	cfg.BatchProcessing.FlushTimeout = 5 * time.Second
	transformer := func(msg types.ConsumedMessage) (*EnrichedTestPayload, bool, error) {
		var enrichedMsg pkgenrichment.EnrichedMessage
		if err := json.Unmarshal(msg.Payload, &enrichedMsg); err != nil {
			return nil, false, fmt.Errorf("unmarshal outer enriched message: %w", err)
		}
		var originalPayload TestPayload
		if err := json.Unmarshal(enrichedMsg.ConsumedMessage.Payload, &originalPayload); err != nil {
			return nil, false, fmt.Errorf("unmarshal inner original payload: %w", err)
		}
		return &EnrichedTestPayload{
			DeviceID:   originalPayload.DeviceID,
			Timestamp:  originalPayload.Timestamp,
			Value:      originalPayload.Value,
			ClientID:   enrichedMsg.ClientID,
			LocationID: enrichedMsg.LocationID,
			Category:   enrichedMsg.Category,
		}, false, nil
	}
	wrapper, err := bigquery.NewBQServiceWrapper[EnrichedTestPayload](cfg, logger, transformer)
	if err != nil {
		return nil, err
	}
	go func() {
		if err := wrapper.Start(); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("BigQueryService (enriched) failed")
		}
	}()
	return wrapper, nil
}

// --- Result Verification Helpers ---

func verifyPubSubMessages(t *testing.T, ctx context.Context, projectID, topicID string, expectedCount int, ready chan<- struct{}) {
	t.Helper()
	client, err := pubsub.NewClient(ctx, projectID)
	require.NoError(t, err)
	defer client.Close()
	subID := fmt.Sprintf("e2e-verifier-sub-%s", uuid.New().String()[:8])
	sub, err := client.CreateSubscription(ctx, subID, pubsub.SubscriptionConfig{
		Topic:            client.Topic(topicID),
		ExpirationPolicy: 24 * time.Hour,
	})
	require.NoError(t, err)
	defer sub.Delete(context.Background())
	var count int32
	receiveCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	close(ready)
	sub.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
		atomic.AddInt32(&count, 1)
		msg.Ack()
	})
	require.GreaterOrEqual(t, int(count), expectedCount, "Did not receive enough messages")
}

func verifyBigQueryResults(t *testing.T, ctx context.Context, projectID, datasetID, tableID string, expectedCount int) {
	t.Helper()
	logger := log.With().Str("component", "bq-verifier").Logger()
	client, err := bq.NewClient(ctx, projectID)
	require.NoError(t, err)
	defer client.Close()
	queryStr := fmt.Sprintf("SELECT COUNT(*) as count FROM `%s.%s.%s`", projectID, datasetID, tableID)
	var rowCount int64
	require.Eventually(t, func() bool {
		q := client.Query(queryStr)
		it, err := q.Read(ctx)
		if err != nil {
			logger.Warn().Err(err).Msg("Failed to query BigQuery for row count")
			return false
		}
		var row struct{ Count int64 }
		if err := it.Next(&row); err != nil {
			logger.Warn().Err(err).Msg("Failed to iterate BigQuery count results")
			return false
		}
		rowCount = row.Count
		logger.Info().Int64("current_count", rowCount).Int("expected_min", expectedCount).Msg("Polling BQ results...")
		return rowCount >= int64(expectedCount)
	}, 60*time.Second, 5*time.Second, "BigQuery row count did not meet threshold")
}

// verifyEnrichedBigQueryResults polls BigQuery until a minimum number of rows are present,
// then verifies that a specific device has been correctly enriched.
func verifyEnrichedBigQueryResults(t *testing.T, ctx context.Context, projectID, datasetID, tableID string, deviceID, expectedClientID string, expectedMinCount int) {
	t.Helper()
	logger := log.With().Str("component", "bq-verifier-enriched").Logger()
	client, err := bq.NewClient(ctx, projectID)
	require.NoError(t, err)
	defer client.Close()

	var finalClientID string

	// Poll until both conditions are met: total count is sufficient AND the specific device's data is enriched correctly.
	require.Eventually(t, func() bool {
		// 1. Check total row count
		countQueryStr := fmt.Sprintf("SELECT COUNT(*) as count FROM `%s.%s.%s`", projectID, datasetID, tableID)
		var rowCount int64
		countIt, err := client.Query(countQueryStr).Read(ctx)
		if err != nil {
			logger.Warn().Err(err).Msg("Polling failed: could not execute count query.")
			return false
		}
		var countRow struct{ Count int64 }
		if err = countIt.Next(&countRow); err != nil {
			logger.Warn().Err(err).Msg("Polling failed: could not read count result.")
			return false
		}
		rowCount = countRow.Count
		logger.Info().Int64("current_count", rowCount).Int("expected_min", expectedMinCount).Msg("Polling BQ results...")
		if rowCount < int64(expectedMinCount) {
			return false // Not enough messages yet, continue polling.
		}

		// 2. Once count is sufficient, check for enrichment
		logger.Info().Msg("Minimum row count reached. Now verifying enrichment.")
		enrichQueryStr := fmt.Sprintf("SELECT clientID FROM `%s.%s.%s` WHERE device_id = @deviceID LIMIT 1", projectID, datasetID, tableID)
		enrichQuery := client.Query(enrichQueryStr)
		enrichQuery.Parameters = []bq.QueryParameter{{Name: "deviceID", Value: deviceID}}
		enrichIt, err := enrichQuery.Read(ctx)
		if err != nil {
			logger.Warn().Err(err).Msg("Failed to query BigQuery for enriched row")
			return false
		}

		var enrichRow struct {
			ClientID string `bigquery:"clientID"`
		}
		err = enrichIt.Next(&enrichRow)
		if err == iterator.Done {
			logger.Warn().Msg("Enriched row not found yet, though total count is sufficient. Retrying.")
			return false // Row not found yet, retry.
		}
		if err != nil {
			logger.Error().Err(err).Msg("Error iterating BigQuery results for enriched row")
			return false
		}

		finalClientID = enrichRow.ClientID
		// Both conditions met: count is good, and we found the enriched row. Check if ClientID is correct.
		return finalClientID == expectedClientID
	}, 90*time.Second, 5*time.Second, "Did not find an enriched row with the correct ClientID in time, or minimum count not met.")

	logger.Info().Str("device_id", deviceID).Str("found_client_id", finalClientID).Msg("Enriched data verified in BigQuery.")
	require.Equal(t, expectedClientID, finalClientID, "Enriched ClientID in BigQuery does not match expected value")
}

// --- Load Generation Helper ---
type testPayloadGenerator struct{}

func (g *testPayloadGenerator) GeneratePayload(device *loadgen.Device) ([]byte, error) {
	return json.Marshal(TestPayload{DeviceID: device.ID, Timestamp: time.Now().UTC(), Value: 123.45})
}
