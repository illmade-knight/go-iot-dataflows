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

// BatchProcessing defines settings for the message processing and batching logic.
type BatchProcessing struct {
	NumWorkers   int           `mapstructure:"num_workers"`   // Number of concurrent message processing workers
	BatchSize    int           `mapstructure:"batch_size"`    // Batch size for GCS writes
	FlushTimeout time.Duration `mapstructure:"flush_timeout"` // Flush timeout for GCS writes
}

// Config holds all configuration for the IceStore microservice.
// It structures various settings into logical groups.
type Config struct {
	builder.BaseConfig `mapstructure:",squash"` // Embed BaseConfig to inherit common fields

	// Consumer holds settings for the Pub/Sub subscriber.
	Consumer Consumer `mapstructure:"consumer"`

	// IceStore holds settings for Google Cloud Storage.
	IceStore IceStore `mapstructure:"ice_store"`

	// BatchProcessing holds settings for the processing and batching logic.
	BatchProcessing BatchProcessing `mapstructure:"batch_processing"`
}

// LoadConfig initializes and loads the application configuration for the IceStore service.
// It sets defaults, binds command-line flags, and reads from a config file.
// It also explicitly checks for the Cloud Run `PORT` environment variable.
func LoadConfig() (*Config, error) {
	v := viper.New()

	// --- 1. Set Defaults ---
	// Provide sensible default values for all configuration fields.
	v.SetDefault("log_level", "info")
	v.SetDefault("http_port", ":8083") // Default HTTP port for this service
	v.SetDefault("project_id", "default-project-id")
	v.SetDefault("credentials_file", "") // Default to empty, rely on ADC for general GCP

	v.SetDefault("consumer.subscription_id", "default-icestore-sub")
	v.SetDefault("consumer.credentials_file", "") // Default to empty, rely on ADC for Pub/Sub

	v.SetDefault("ice_store.credentials_file", "") // Default to empty, rely on ADC for GCS
	v.SetDefault("ice_store.bucket_name", "default-archive-bucket")
	v.SetDefault("ice_store.object_prefix", "archived-data/")

	v.SetDefault("batch_processing.num_workers", 5)
	v.SetDefault("batch_processing.batch_size", 100)
	v.SetDefault("batch_processing.flush_timeout", 5*time.Second)

	// --- 2. Set up pflag for command-line overrides ---
	// Define command-line flags that can override config file or env variables.
	pflag.String("config", "", "Path to config file (e.g., config-icestore.yaml)")
	pflag.String("log-level", "", "Log level (debug, info, warn, error)")
	pflag.String("http-port", "", "HTTP health check port")
	pflag.String("project-id", "", "GCP Project ID")
	pflag.String("credentials-file", "", "Path to GCP credentials JSON file for general clients")

	pflag.String("subscription-id", "", "Pub/Sub Subscription ID to consume from")
	pflag.String("pubsub-credentials-file", "", "Path to Pub/Sub credentials JSON file for consumer")

	pflag.String("gcs-credentials-file", "", "Path to GCS credentials JSON file")
	pflag.String("gcs-bucket-name", "", "Google Cloud Storage bucket name for archival")
	pflag.String("gcs-object-prefix", "", "Prefix for objects in GCS bucket (e.g., 'data/')")

	pflag.Int("num-workers", 0, "Number of processing workers")
	pflag.Int("batch-size", 0, "GCS batch write size")
	pflag.Duration("flush-timeout", 0, "GCS batch flush timeout")

	pflag.Parse()                          // Parse command-line flags
	err := v.BindPFlags(pflag.CommandLine) // Bind flags to Viper
	if err != nil {
		log.Warn().Err(err).Msg("Failed to bind flags")
	}

	// --- 3. Set up Viper to read from file ---
	// Read the config file path from the flag.
	configFile := v.GetString("config")
	if configFile != "" { // Only try to read if a config file path is provided
		v.SetConfigFile(configFile)
		v.SetConfigType("yaml") // Assuming YAML format
		if err := v.ReadInConfig(); err != nil {
			// It's okay if the config file doesn't exist; we can rely on flags/env vars.
			// Log a warning if it's any other error.
			if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
				log.Warn().Err(err).Str("config_file", configFile).Msg("Failed to read config file, using defaults/flags/environment variables.")
			}
		}
	}

	// --- 4. Set up environment variable support ---
	// Allow environment variables to override settings.
	// Environment variables should be prefixed with "APP_" (e.g., APP_LOG_LEVEL).
	// Replace dots and hyphens with underscores for environment variable names.
	v.SetEnvPrefix("APP")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv() // Automatically read matching environment variables

	// --- 5. Unmarshal config into our struct ---
	var cfg Config
	// Unmarshal all configurations (from defaults, flags, file, env) into the Config struct.
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal configuration: %w", err)
	}

	// Manual overrides for specific flags to ensure they take precedence
	// (though BindPFlags should handle this for direct matches, useful for complex mapping)
	if v.IsSet("log-level") {
		cfg.LogLevel = v.GetString("log-level")
	}
	if v.IsSet("http-port") {
		cfg.HTTPPort = v.GetString("http-port")
	}
	if v.IsSet("project-id") {
		cfg.ProjectID = v.GetString("project-id")
	}
	if v.IsSet("credentials-file") {
		cfg.CredentialsFile = v.GetString("credentials-file")
	}
	if v.IsSet("subscription-id") {
		cfg.Consumer.SubscriptionID = v.GetString("subscription-id")
	}
	if v.IsSet("pubsub-credentials-file") {
		cfg.Consumer.CredentialsFile = v.GetString("pubsub-credentials-file")
	}
	if v.IsSet("gcs-credentials-file") {
		cfg.IceStore.CredentialsFile = v.GetString("gcs-credentials-file")
	}
	if v.IsSet("gcs-bucket-name") {
		cfg.IceStore.BucketName = v.GetString("gcs-bucket-name")
	}
	if v.IsSet("gcs-object-prefix") {
		cfg.IceStore.ObjectPrefix = v.GetString("gcs-object-prefix")
	}
	if v.IsSet("num-workers") && v.GetInt("num-workers") != 0 {
		cfg.BatchProcessing.NumWorkers = v.GetInt("num-workers")
	}
	if v.IsSet("batch-size") && v.GetInt("batch-size") != 0 {
		cfg.BatchProcessing.BatchSize = v.GetInt("batch-size")
	}
	if v.IsSet("flush-timeout") && v.GetDuration("flush-timeout") != 0 {
		cfg.BatchProcessing.FlushTimeout = v.GetDuration("flush-timeout")
	}

	// --- 6. Explicitly check for Cloud Run PORT environment variable ---
	// The `PORT` environment variable is automatically set by Cloud Run and takes precedence.
	if port := os.Getenv("PORT"); port != "" {
		log.Info().Str("old_http_port", cfg.HTTPPort).Str("new_http_port", ":"+port).Msg("Overriding HTTP port with Cloud Run PORT environment variable.")
		cfg.HTTPPort = ":" + port
	}

	return &cfg, nil
}
