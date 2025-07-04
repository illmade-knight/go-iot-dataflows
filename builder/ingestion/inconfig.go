// github.com/illmade-knight/go-iot-dataflows/builder/ingestion/config.go
package ingestion

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/illmade-knight/go-iot-dataflows/builder"
	"github.com/illmade-knight/go-iot/pkg/mqttconverter"
	"github.com/rs/zerolog/log"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Config holds all configuration for the MQTT ingestion service.
type Config struct {
	builder.BaseConfig `mapstructure:",squash"`

	// ServiceName is the unique name of this service instance (e.g., "ingestion-service-alpha").
	ServiceName string `mapstructure:"service_name"`
	// DataflowName is the conceptual dataflow this service belongs to.
	DataflowName string `mapstructure:"dataflow_name"`

	// ServiceDirectorURL is the base URL for the ServiceDirector API.
	ServiceDirectorURL string `mapstructure:"service_director_url"`

	Publisher struct {
		TopicID         string `mapstructure:"topic_id"`
		CredentialsFile string `mapstructure:"credentials_file"`
	} `mapstructure:"publisher"`

	MQTT    mqttconverter.MQTTClientConfig       `mapstructure:"mqtt"`
	Service mqttconverter.IngestionServiceConfig `mapstructure:"service"`
}

// --- Production Loader ---

// LoadConfigFromEnv loads configuration strictly from environment variables using Viper.
// It is intended for production environments and fails if required variables are missing.
func LoadConfigFromEnv() (*Config, error) {
	v := viper.New()
	v.SetEnvPrefix("APP") // e.g., APP_PROJECT_ID, APP_MQTT_BROKER_URL
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	// Set defaults for non-critical values that might not be in the environment.
	v.SetDefault("log_level", "info")
	v.SetDefault("http_port", ":8080")
	v.SetDefault("service.input_chan_capacity", 5000)
	v.SetDefault("service.num_processing_workers", 20)
	v.SetDefault("mqtt.client_id_prefix", "prod-ingestion-")
	v.SetDefault("mqtt.keep_alive", 60*time.Second)
	v.SetDefault("mqtt.connect_timeout", 10*time.Second)

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal configuration from environment: %w", err)
	}

	// The `PORT` environment variable is automatically set by Cloud Run and takes precedence.
	if port := os.Getenv("PORT"); port != "" {
		log.Info().Str("old_http_port", cfg.HTTPPort).Str("new_http_port", ":"+port).Msg("Overriding HTTP port with Cloud Run PORT env var.")
		cfg.HTTPPort = ":" + port
	}

	// Validate that required fields were successfully loaded from the environment.
	if cfg.ProjectID == "" || cfg.ServiceName == "" || cfg.DataflowName == "" || cfg.Publisher.TopicID == "" || cfg.MQTT.BrokerURL == "" {
		return nil, fmt.Errorf("missing required configuration environment variables (e.g., APP_PROJECT_ID, APP_SERVICE_NAME, APP_DATAFLOW_NAME, APP_PUBLISHER_TOPIC_ID, APP_MQTT_BROKER_URL)")
	}

	return &cfg, nil
}

// --- Development/Test Loader (Viper-based) ---

// LoadConfigWithOverrides provides a flexible way to load configuration for development
// and testing, using a hierarchy of defaults -> config file -> env vars -> flags.
func LoadConfigWithOverrides() (*Config, error) {
	v := viper.New()
	pflag.String("config", "", "ServiceSourcePath to config file (e.g., config-ingestion.yaml)")

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

// --- Helper functions for config loading ---

func setDevelopmentDefaults(v *viper.Viper) {
	v.SetDefault("log_level", "debug")
	v.SetDefault("http_port", ":8081")
	v.SetDefault("project_id", "local-dev-project")
	v.SetDefault("service_name", "ingestion-local")
	v.SetDefault("dataflow_name", "dataflow-local")
	v.SetDefault("service_director_url", "")
	v.SetDefault("publisher.topic_id", "ingestion-topic")
	v.SetDefault("publisher.credentials_file", "")
	v.SetDefault("mqtt.broker_url", "tcp://localhost:1883")
	v.SetDefault("mqtt.topic", "telemetry/#")
	v.SetDefault("mqtt.client_id_prefix", "ingestion-dev-")
	v.SetDefault("mqtt.keep_alive", 30*time.Second)
	v.SetDefault("mqtt.connect_timeout", 10*time.Second)
	v.SetDefault("service.input_chan_capacity", 1000)
	v.SetDefault("service.num_processing_workers", 5)
}

func defineFlags(fs *pflag.FlagSet) {
	fs.String("log-level", "", "Log level")
	fs.String("http-port", "", "HTTP health check port")
	fs.String("project-id", "", "GCP Project ID")
	fs.String("service-name", "", "Unique name of this service instance")
	fs.String("dataflow-name", "", "Dataflow this service belongs to")
	fs.String("service-director-url", "", "URL of the ServiceDirector API")
	fs.String("credentials-file", "", "ServiceSourcePath to general GCP credentials JSON file")
	fs.String("publisher.topic-id", "", "Google Pub/Sub Topic ID")
	fs.String("publisher.credentials-file", "", "ServiceSourcePath to publisher-specific credentials file")
	fs.String("mqtt.broker-url", "", "MQTT Broker URL")
	fs.String("mqtt.topic", "", "MQTT Topic to subscribe to")
	fs.String("mqtt.username", "", "MQTT Username")
	fs.String("mqtt.password", "", "MQTT Password")
	fs.String("mqtt.client-id-prefix", "", "MQTT Client ID Prefix")
}
