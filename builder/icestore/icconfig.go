// github.com/illmade-knight/go-iot-dataflows/builder/icestore/config.go
package icestore

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/illmade-knight/go-iot-dataflows/builder" // Import the builder for BaseConfig
	"github.com/rs/zerolog/log"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Consumer defines the configuration for the Pub/Sub subscriber.
type Consumer struct {
	SubscriptionID  string `mapstructure:"subscription_id"`
	CredentialsFile string `mapstructure:"credentials_file"` // Optional path to GCP credentials file
}

// IceStore defines configuration specific to Google Cloud Storage (GCS) for archival.
type IceStore struct {
	CredentialsFile string `mapstructure:"credentials_file"` // Optional path to GCP credentials file for GCS
	BucketName      string `mapstructure:"bucket_name"`      // Name of the GCS bucket for archival
	ObjectPrefix    string `mapstructure:"object_prefix"`    // Prefix for objects stored in the bucket (e.g., "archived-data/")
}

// Config holds all configuration for the IceStore microservice.
type Config struct {
	builder.BaseConfig `mapstructure:",squash"` // Embed BaseConfig to inherit common fields

	// ServiceName is the unique name of this service instance.
	ServiceName string `mapstructure:"service_name"`
	// DataflowName is the conceptual dataflow this service belongs to.
	DataflowName string `mapstructure:"dataflow_name"`
	// ServiceDirectorURL is the base URL for the ServiceDirector API.
	ServiceDirectorURL string `mapstructure:"service_director_url"`

	// Consumer holds settings for the Pub/Sub subscriber.
	Consumer Consumer `mapstructure:"consumer"`

	// IceStore holds settings for Google Cloud Storage.
	IceStore IceStore `mapstructure:"ice_store"`

	// BatchProcessing holds settings for the message processing and batching logic.
	BatchProcessing struct {
		NumWorkers   int           `mapstructure:"num_workers"`   // Number of concurrent message processing workers
		BatchSize    int           `mapstructure:"batch_size"`    // Batch size for GCS writes
		FlushTimeout time.Duration `mapstructure:"flush_timeout"` // Flush timeout for GCS writes
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

	if cfg.ProjectID == "" || cfg.ServiceName == "" || cfg.DataflowName == "" || cfg.Consumer.SubscriptionID == "" || cfg.IceStore.BucketName == "" {
		return nil, fmt.Errorf("missing required configuration environment variables (e.g., APP_PROJECT_ID, APP_SERVICE_NAME, APP_DATAFLOW_NAME, APP_CONSUMER_SUBSCRIPTION_ID, APP_ICE_STORE_BUCKET_NAME)")
	}

	return &cfg, nil
}

// LoadConfigWithOverrides provides a flexible way to load configuration for development and testing.
func LoadConfigWithOverrides() (*Config, error) {
	v := viper.New()
	pflag.String("config", "", "ServiceSourcePath to config file (e.g., config-icestore.yaml)")

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
	v.SetDefault("http_port", ":8083")
	v.SetDefault("project_id", "local-dev-project")
	v.SetDefault("service_name", "icestore-local")
	v.SetDefault("dataflow_name", "dataflow-local")
	v.SetDefault("service_director_url", "")

	v.SetDefault("batchProcessing.num_workers", 2)
	v.SetDefault("batchProcessing.batch_size", 10)
	v.SetDefault("batchProcessing.flush_timeout", 2*time.Second)

	v.SetDefault("consumer.subscription_id", "dev-icestore-subscription")
	v.SetDefault("consumer.credentials_file", "")

	v.SetDefault("ice_store.bucket_name", "dev-archive-bucket")
	v.SetDefault("ice_store.object_prefix", "archived-dev/")
	v.SetDefault("ice_store.credentials_file", "")
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
	fs.String("consumer.credentials-file", "", "ServiceSourcePath to Pub/Sub credentials JSON file for consumer")

	fs.String("ice_store.credentials-file", "", "ServiceSourcePath to GCS credentials JSON file")
	fs.String("ice_store.bucket-name", "", "Google Cloud Storage bucket name for archival")
	fs.String("ice_store.object-prefix", "", "Prefix for objects in GCS bucket")

	fs.Int("batchProcessing.num-workers", 0, "Number of processing workers")
	fs.Int("batchProcessing.batch-size", 0, "GCS batch write size")
	fs.Duration("batchProcessing.flush-timeout", 0, "GCS batch flush timeout")
}
