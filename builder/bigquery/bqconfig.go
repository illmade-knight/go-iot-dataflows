// github.com/illmade-knight/go-iot-dataflows/builder/bigquery/config.go
package bigquery

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/illmade-knight/go-iot-dataflows/builder"
	"github.com/illmade-knight/go-iot/pkg/bqstore"
	"github.com/rs/zerolog/log"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Consumer defines the configuration for the Pub/Sub subscriber.
type Consumer struct {
	SubscriptionID  string `mapstructure:"subscription_id"`
	CredentialsFile string `mapstructure:"credentials_file"`
}

// Config holds all configuration for the BigQuery batch inserter application.
type Config struct {
	builder.BaseConfig `mapstructure:",squash"`

	// ServiceName is the unique name of this service instance.
	ServiceName string `mapstructure:"service_name"`
	// DataflowName is the conceptual dataflow this service belongs to.
	DataflowName string `mapstructure:"dataflow_name"`
	// ServiceDirectorURL is the base URL for the ServiceDirector API.
	ServiceDirectorURL string `mapstructure:"service_director_url"`

	// Consumer holds settings for the Pub/Sub subscriber.
	Consumer Consumer `mapstructure:"consumer"`

	// BigQueryConfig holds settings for the BigQuery inserter.
	BigQueryConfig bqstore.BigQueryDatasetConfig `mapstructure:"bigquery"`

	// BatchProcessing holds settings for the processing and batching logic.
	BatchProcessing struct {
		bqstore.BatchInserterConfig `mapstructure:",squash"` // Embeds batch size and flush timeout
		NumWorkers                  int                      `mapstructure:"num_workers"`
	} `mapstructure:"batchProcessing"`
}

// LoadConfigFromEnv loads configuration strictly from environment variables using Viper.
func LoadConfigFromEnv() (*Config, error) {
	v := viper.New()
	v.SetEnvPrefix("APP")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	v.SetDefault("log_level", "info")
	v.SetDefault("http_port", ":8080")
	v.SetDefault("batchProcessing.num_workers", 5)
	v.SetDefault("batchProcessing.batch_size", 100)
	v.SetDefault("batchProcessing.flush_timeout", 5*time.Second)

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal configuration from environment: %w", err)
	}

	if port := os.Getenv("PORT"); port != "" {
		cfg.HTTPPort = ":" + port
	}

	if cfg.ProjectID == "" || cfg.ServiceName == "" || cfg.DataflowName == "" || cfg.Consumer.SubscriptionID == "" || cfg.BigQueryConfig.DatasetID == "" || cfg.BigQueryConfig.TableID == "" {
		return nil, fmt.Errorf("missing required configuration environment variables (e.g., APP_PROJECT_ID, APP_SERVICE_NAME, APP_DATAFLOW_NAME, APP_CONSUMER_SUBSCRIPTION_ID, etc.)")
	}

	return &cfg, nil
}

// LoadConfigWithOverrides provides a flexible way to load configuration for development and testing.
func LoadConfigWithOverrides() (*Config, error) {
	v := viper.New()
	pflag.String("config", "", "ServiceSourcePath to config file (e.g., config-bq.yaml)")

	setDevelopmentDefaults(v)

	defineFlags(pflag.CommandLine)
	if err := v.BindPFlags(pflag.CommandLine); err != nil {
		return nil, fmt.Errorf("failed to bind pflags: %w", err)
	}

	configFile := v.GetString("config")
	if configFile != "" {
		v.SetConfigFile(configFile)
		v.SetConfigType("yaml")
		if err := v.ReadInConfig(); err != nil {
			if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
				log.Warn().Err(err).Str("config_file", configFile).Msg("Failed to read config file")
			}
		}
	}

	v.SetEnvPrefix("APP")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal configuration: %w", err)
	}

	if port := os.Getenv("PORT"); port != "" {
		cfg.HTTPPort = ":" + port
	}

	return &cfg, nil
}

func setDevelopmentDefaults(v *viper.Viper) {
	v.SetDefault("log_level", "debug")
	v.SetDefault("http_port", ":8082")
	v.SetDefault("project_id", "local-dev-project")
	v.SetDefault("service_name", "bigquery-local")
	v.SetDefault("dataflow_name", "dataflow-local")
	v.SetDefault("service_director_url", "")

	v.SetDefault("batchProcessing.num_workers", 2)
	v.SetDefault("batchProcessing.batch_size", 10)
	v.SetDefault("batchProcessing.flush_timeout", 2*time.Second)

	v.SetDefault("bigquery.dataset_id", "dev_dataset")
	v.SetDefault("bigquery.table_id", "dev_table")

	v.SetDefault("consumer.subscription_id", "dev-subscription")
	v.SetDefault("consumer.credentials_file", "")
}

func defineFlags(fs *pflag.FlagSet) {
	fs.String("log-level", "", "Log level")
	fs.String("http-port", "", "HTTP health check port")
	fs.String("project-id", "", "GCP Project ID")
	fs.String("credentials-file", "", "ServiceSourcePath to general GCP credentials JSON file")

	fs.String("service-name", "", "Unique name of this service instance")
	fs.String("dataflow-name", "", "Dataflow this service belongs to")
	fs.String("service-director-url", "", "URL of the ServiceDirector API")

	fs.String("consumer.subscription-id", "", "Pub/Sub Subscription ID to consume from")
	fs.String("bigquery.dataset-id", "", "BigQuery Dataset ID")
	fs.String("bigquery.table-id", "", "BigQuery Table ID")

	fs.Int("batchProcessing.num-workers", 0, "Number of processing workers")
	fs.Int("batchProcessing.batch-size", 0, "BigQuery batch insert size")
	fs.Duration("batchProcessing.flush-timeout", 0, "BigQuery batch flush timeout")
}
