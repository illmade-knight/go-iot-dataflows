package bigqueries

import (
	"flag"
	"google.golang.org/api/option"
	"os"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/bqstore"
	"github.com/illmade-knight/go-dataflow/pkg/microservice"
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
	ClientConnections  map[string][]option.ClientOption

	BatchProcessing struct {
		// Embed the refactored config, which includes BatchSize, FlushInterval, and InsertTimeout
		bqstore.BatchInserterConfig
		NumWorkers int
	}
}

// LoadConfigDefaults initializes and loads configuration from defaults, flags, and environment variables.
func LoadConfigDefaults(projectID string) (*Config, error) {
	cfg := &Config{
		BaseConfig: microservice.BaseConfig{
			ProjectID: projectID,
			LogLevel:  "debug",
			HTTPPort:  ":8084",
		},
	}
	cfg.BigQueryConfig.ProjectID = projectID

	cfg.BatchProcessing.NumWorkers = 5
	cfg.BatchProcessing.BatchSize = 100
	cfg.BatchProcessing.FlushInterval = 1 * time.Minute
	cfg.BatchProcessing.InsertTimeout = 2 * time.Minute

	flag.IntVar(&cfg.BatchProcessing.NumWorkers, "batch.num-workers", cfg.BatchProcessing.NumWorkers, "Number of processing workers")
	flag.IntVar(&cfg.BatchProcessing.BatchSize, "batch.size", cfg.BatchProcessing.BatchSize, "BigQuery batch insert size")
	flag.DurationVar(&cfg.BatchProcessing.FlushInterval, "batch.flush-interval", cfg.BatchProcessing.FlushInterval, "BigQuery batch flush interval")
	flag.DurationVar(&cfg.BatchProcessing.InsertTimeout, "batch.insert-timeout", cfg.BatchProcessing.InsertTimeout, "BigQuery batch insert operation timeout")
	flag.Parse()

	if port := os.Getenv("PORT"); port != "" {
		cfg.HTTPPort = ":" + port
	}

	return cfg, nil
}
