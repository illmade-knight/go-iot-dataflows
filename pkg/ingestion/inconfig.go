package ingestion

import (
	"flag"
	"fmt"
	"github.com/illmade-knight/go-iot-dataflows/pkg"
	"os"
	"time"

	"github.com/illmade-knight/go-cloud-manager/microservice"
	"github.com/illmade-knight/go-iot/pkg/mqttconverter"
	"github.com/rs/zerolog/log"
)

// Config holds all configuration for the MQTT ingestion service.
type Config struct {
	microservice.BaseConfig

	ServiceName        string `mapstructure:"service_name"`
	DataflowName       string `mapstructure:"dataflow_name"`
	ServiceDirectorURL string `mapstructure:"service_director_url"`

	Publisher struct {
		TopicID         string `mapstructure:"topic_id"`
		CredentialsFile string `mapstructure:"credentials_file"`
	}

	MQTT    mqttconverter.MQTTClientConfig
	Service mqttconverter.IngestionServiceConfig
}

// LoadConfig initializes and loads configuration using a clear hierarchy:
// 1. Set Defaults
// 2. Override with command-line flags
// 3. Override with Environment Variables
func LoadConfig() (*Config, error) {
	// 1. Start with a struct containing all default values.
	cfg := &Config{
		BaseConfig: microservice.BaseConfig{
			LogLevel: "debug",
			HTTPPort: ":8081",
		},
		ServiceName:        "ingestion-local",
		DataflowName:       "dataflow-local",
		ServiceDirectorURL: "",
	}
	cfg.Publisher.TopicID = "ingestion-topic"
	cfg.MQTT.BrokerURL = "tcp://localhost:1883"
	cfg.MQTT.Topic = "telemetry/#"
	cfg.MQTT.ClientIDPrefix = "ingestion-dev-"
	cfg.MQTT.KeepAlive = 30 * time.Second
	cfg.MQTT.ConnectTimeout = 10 * time.Second
	cfg.Service.InputChanCapacity = 1000
	cfg.Service.NumProcessingWorkers = 5

	// 2. Define flags, using the defaults we just set.
	// These will only be used if no environment variable is present.
	flag.StringVar(&cfg.ProjectID, "project-id", cfg.ProjectID, "GCP Project ID")
	flag.StringVar(&cfg.ServiceName, "service-name", cfg.ServiceName, "Unique name of this service instance")
	flag.StringVar(&cfg.DataflowName, "dataflow-name", cfg.DataflowName, "Dataflow this service belongs to")
	flag.StringVar(&cfg.ServiceDirectorURL, "service-director-url", cfg.ServiceDirectorURL, "URL of the ServiceDirector API")
	flag.StringVar(&cfg.Publisher.TopicID, "publisher-topic-id", cfg.Publisher.TopicID, "Google Pub/Sub Topic ID")
	flag.StringVar(&cfg.MQTT.BrokerURL, "mqtt-broker-url", cfg.MQTT.BrokerURL, "MQTT Broker URL")
	flag.StringVar(&cfg.MQTT.Topic, "mqtt-topic", cfg.MQTT.Topic, "MQTT Topic to subscribe to")
	flag.StringVar(&cfg.MQTT.Username, "mqtt-username", cfg.MQTT.Username, "MQTT Username")
	flag.StringVar(&cfg.MQTT.Password, "mqtt-password", cfg.MQTT.Password, "MQTT Password")
	flag.Parse()

	// 3. Override with environment variables if they are set.
	// This creates a clear hierarchy: ENV > flag > default.
	pkg.OverrideWithStringEnvVar("PROJECT_ID", &cfg.ProjectID)
	pkg.OverrideWithStringEnvVar("SERVICE_DIRECTOR_URL", &cfg.ServiceDirectorURL)
	pkg.OverrideWithStringEnvVar("APP_SERVICE_NAME", &cfg.ServiceName)
	pkg.OverrideWithStringEnvVar("APP_DATAFLOW_NAME", &cfg.DataflowName)
	pkg.OverrideWithStringEnvVar("APP_PUBLISHER_TOPIC_ID", &cfg.Publisher.TopicID)
	pkg.OverrideWithStringEnvVar("APP_MQTT_BROKER_URL", &cfg.MQTT.BrokerURL)
	pkg.OverrideWithStringEnvVar("APP_MQTT_TOPIC", &cfg.MQTT.Topic)
	pkg.OverrideWithStringEnvVar("APP_MQTT_USERNAME", &cfg.MQTT.Username)
	pkg.OverrideWithStringEnvVar("APP_MQTT_PASSWORD", &cfg.MQTT.Password)

	// Special handling for Cloud Run's PORT, which takes highest precedence.
	if port := os.Getenv("PORT"); port != "" {
		newPort := ":" + port
		if port == cfg.HTTPPort { // Check against the original default
			log.Info().Str("port", newPort).Msg("HTTP port set by Cloud Run PORT env var.")
		} else {
			log.Info().Str("old_http_port", cfg.HTTPPort).Str("new_http_port", newPort).Msg("Overriding HTTP port with Cloud Run PORT env var.")
		}
		cfg.HTTPPort = newPort
	}
	// Final validation for production-critical variables
	if os.Getenv("APP_ENV") == "production" {
		if cfg.ProjectID == "" || cfg.ServiceName == "" || cfg.DataflowName == "" || cfg.Publisher.TopicID == "" || cfg.MQTT.BrokerURL == "" {
			return nil, fmt.Errorf("missing required configuration for production environment")
		}
	}
	return cfg, nil
}
