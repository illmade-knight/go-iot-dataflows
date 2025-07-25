package bigquery

import (
	"flag"
	"github.com/illmade-knight/go-iot-dataflows/pkg"
	"os"
	"time"

	"github.com/illmade-knight/go-cloud-manager/microservice"
	"github.com/illmade-knight/go-iot/pkg/bqstore"
)

// Consumer defines the configuration for the Pub/Sub subscriber.
type Consumer struct {
	SubscriptionID  string
	CredentialsFile string
}

// Config holds all configuration for the BigQuery batch inserter application.
type Config struct {
	microservice.BaseConfig

	ServiceName        string
	DataflowName       string
	ServiceDirectorURL string
	Consumer           Consumer
	BigQueryConfig     bqstore.BigQueryDatasetConfig

	BatchProcessing struct {
		bqstore.BatchInserterConfig // Embeds batch size and flush timeout
		NumWorkers                  int
	}
}

// LoadConfig initializes and loads configuration using a clear hierarchy:
// 1. Set Defaults
// 2. Override with command-line flags
// 3. Override with Environment Variables
func LoadConfig() (*Config, error) {
	// 1. Start with a struct containing all default values.
	cfg := &Config{
		BaseConfig: microservice.BaseConfig{
			LogLevel:  "debug",
			HTTPPort:  ":8082",
			ProjectID: "local-dev-project",
		},
		ServiceName:        "bigquery-local",
		DataflowName:       "dataflow-local",
		ServiceDirectorURL: "",
	}
	cfg.BatchProcessing.NumWorkers = 2
	cfg.BatchProcessing.BatchSize = 10
	cfg.BatchProcessing.FlushTimeout = 2 * time.Second
	cfg.BigQueryConfig.DatasetID = "dev_dataset"
	cfg.BigQueryConfig.TableID = "dev_table"
	cfg.Consumer.SubscriptionID = "dev-subscription"

	// 2. Define flags to pkg.Override defaults.
	flag.StringVar(&cfg.ProjectID, "project-id", cfg.ProjectID, "GCP Project ID")
	flag.StringVar(&cfg.ServiceName, "service-name", cfg.ServiceName, "Unique name of this service instance")
	flag.StringVar(&cfg.DataflowName, "dataflow-name", cfg.DataflowName, "Dataflow this service belongs to")
	flag.StringVar(&cfg.Consumer.SubscriptionID, "consumer.subscription-id", cfg.Consumer.SubscriptionID, "Pub/Sub Subscription ID to consume from")
	flag.StringVar(&cfg.BigQueryConfig.DatasetID, "bigquery.dataset-id", cfg.BigQueryConfig.DatasetID, "BigQuery Dataset ID")
	flag.StringVar(&cfg.BigQueryConfig.TableID, "bigquery.table-id", cfg.BigQueryConfig.TableID, "BigQuery Table ID")
	flag.IntVar(&cfg.BatchProcessing.NumWorkers, "batchProcessing.num-workers", cfg.BatchProcessing.NumWorkers, "Number of processing workers")
	flag.IntVar(&cfg.BatchProcessing.BatchSize, "batchProcessing.batch-size", cfg.BatchProcessing.BatchSize, "BigQuery batch insert size")
	flag.DurationVar(&cfg.BatchProcessing.FlushTimeout, "batchProcessing.flush-timeout", cfg.BatchProcessing.FlushTimeout, "BigQuery batch flush timeout")
	flag.Parse()

	// 3. Override with environment variables if they are set. ENV > flag > default.
	pkg.OverrideWithStringEnvVar("PROJECT_ID", &cfg.ProjectID)
	pkg.OverrideWithStringEnvVar("SERVICE_DIRECTOR_URL", &cfg.ServiceDirectorURL)
	pkg.OverrideWithStringEnvVar("APP_SERVICE_NAME", &cfg.ServiceName)
	pkg.OverrideWithStringEnvVar("APP_DATAFLOW_NAME", &cfg.DataflowName)
	pkg.OverrideWithStringEnvVar("APP_CONSUMER_SUBSCRIPTION_ID", &cfg.Consumer.SubscriptionID)
	pkg.OverrideWithStringEnvVar("APP_BIGQUERY_DATASET_ID", &cfg.BigQueryConfig.DatasetID)
	pkg.OverrideWithStringEnvVar("APP_BIGQUERY_TABLE_ID", &cfg.BigQueryConfig.TableID)
	pkg.OverrideWithIntEnvVar("APP_BATCHPROCESSING_NUM_WORKERS", &cfg.BatchProcessing.NumWorkers)
	pkg.OverrideWithIntEnvVar("APP_BATCHPROCESSING_BATCH_SIZE", &cfg.BatchProcessing.BatchSize)
	pkg.OverrideWithDurationEnvVar("APP_BATCHPROCESSING_FLUSH_TIMEOUT", &cfg.BatchProcessing.FlushTimeout)

	// 4. Special handling for Cloud Run's PORT.
	if port := os.Getenv("PORT"); port != "" {
		cfg.HTTPPort = ":" + port
	}

	return cfg, nil
}
