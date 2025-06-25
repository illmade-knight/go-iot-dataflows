//go:build integration

package loade2e_test

import (
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	"fmt"
	"github.com/illmade-knight/go-iot-dataflows/gardenmonitor/bigquery/bqinit"
	"github.com/illmade-knight/go-iot-dataflows/gardenmonitor/ingestion/mqinit"
	"github.com/illmade-knight/go-iot/pkg/bqstore"
	"github.com/illmade-knight/go-iot/pkg/messagepipeline"
	"github.com/illmade-knight/go-iot/pkg/mqttconverter"
	"github.com/illmade-knight/go-iot/pkg/types"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"net/http"
	"testing"
	"time"
)

func getRowCount(ctx context.Context, client *bigquery.Client, datasetID, tableID string) (int64, error) {
	queryString := fmt.Sprintf("SELECT COUNT(*) as count FROM `%s.%s`", datasetID, tableID)
	query := client.Query(queryString)
	it, err := query.Read(ctx)
	if err != nil {
		return 0, err
	}
	var row struct {
		Count int64 `bigquery:"count"`
	}
	if err := it.Next(&row); err != nil {
		if errors.Is(err, iterator.Done) {
			return 0, nil
		}
		return 0, err
	}
	return row.Count, nil
}

func startIngestionService(t *testing.T, ctx context.Context, projectID, mqttBrokerURL, topicID string) (*mqttconverter.IngestionService, *mqinit.Server) {
	t.Helper()
	mqttCfg := &mqinit.Config{
		LogLevel: "info", HTTPPort: cloudTestMqttHTTPPort, ProjectID: projectID,
		Publisher: struct {
			TopicID         string `mapstructure:"topic_id"`
			CredentialsFile string `mapstructure:"credentials_file"`
		}{TopicID: topicID},
		MQTT:    mqttconverter.MQTTClientConfig{BrokerURL: mqttBrokerURL, Topic: testMqttTopicPattern, ClientIDPrefix: testMqttClientIDPrefix},
		Service: mqttconverter.IngestionServiceConfig{InputChanCapacity: 1000, NumProcessingWorkers: 20},
	}
	logger := log.With().Str("service", "mqtt-ingestion").Logger()
	publisher, err := mqttconverter.NewGooglePubsubPublisher(ctx,
		mqttconverter.GooglePubsubPublisherConfig{ProjectID: mqttCfg.ProjectID, TopicID: mqttCfg.Publisher.TopicID}, logger)
	require.NoError(t, err)
	service := mqttconverter.NewIngestionService(publisher, nil, logger, mqttCfg.Service, mqttCfg.MQTT)
	server := mqinit.NewServer(mqttCfg, service, logger)
	go func() {
		if err := server.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error().Err(err).Msg("Ingestion server failed during test execution")
		}
	}()
	return service, server
}

func startBQProcessingService(t *testing.T, ctx context.Context, gcpProjectID, subID, datasetID, tableID string) (*messagepipeline.ProcessingService[types.GardenMonitorReadings], *bqinit.Server[types.GardenMonitorReadings]) {
	t.Helper()
	bqCfg := &bqinit.Config{
		LogLevel:  "info",
		HTTPPort:  cloudTestBqHTTPPort,
		ProjectID: gcpProjectID,
		Consumer:  bqinit.Consumer{SubscriptionID: subID},
		BigQueryConfig: bqstore.BigQueryDatasetConfig{
			ProjectID: gcpProjectID,
			DatasetID: datasetID,
			TableID:   tableID,
		},
		BatchProcessing: struct {
			bqstore.BatchInserterConfig `mapstructure:"datasetup"`
			NumWorkers                  int `mapstructure:"num_workers"`
		}{
			BatchInserterConfig: bqstore.BatchInserterConfig{
				BatchSize:    5,
				FlushTimeout: 10 * time.Second,
			},
			NumWorkers: 2,
		},
	}
	bqLogger := log.With().Str("service", "bq-processor").Logger()
	bqClient, err := bigquery.NewClient(ctx, gcpProjectID)
	require.NoError(t, err)

	psClient, err := pubsub.NewClient(ctx, gcpProjectID)
	require.NoError(t, err)

	bqConsumer, err := messagepipeline.NewGooglePubsubConsumer(&messagepipeline.GooglePubsubConsumerConfig{
		ProjectID:      bqCfg.ProjectID,
		SubscriptionID: bqCfg.Consumer.SubscriptionID,
	}, psClient, bqLogger)
	require.NoError(t, err)

	batchInserter, err := bqstore.NewBigQueryBatchProcessor[types.GardenMonitorReadings](ctx, bqClient, &bqCfg.BatchProcessing.BatchInserterConfig, &bqCfg.BigQueryConfig, bqLogger)
	require.NoError(t, err)

	processingService, err := bqstore.NewBigQueryService[types.GardenMonitorReadings](bqCfg.BatchProcessing.NumWorkers, bqConsumer, batchInserter, types.ConsumedMessageTransformer, bqLogger)
	require.NoError(t, err)

	bqServer := bqinit.NewServer(bqCfg, processingService, bqLogger)
	require.NoError(t, err)
	go func() {
		if srvErr := bqServer.Start(); srvErr != nil && !errors.Is(srvErr, http.ErrServerClosed) {
			t.Errorf("BQ Processing server failed: %v", srvErr)
		}
	}()
	return processingService, bqServer
}
