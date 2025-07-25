// builder/ingestion/inconfig.go
package ingestion

import (
	"flag"
	"fmt"
	"os"

	"github.com/illmade-knight/go-cloud-manager/microservice"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline" // Import the generic pipeline
	"github.com/illmade-knight/go-iot-dataflows/pkg"
	"github.com/illmade-knight/go-iot-dataflows/pkg/mqttconverter" // Import the MQTT service library
)

// Config now includes configuration for the generic GooglePubsubProducer.
type Config struct {
	microservice.BaseConfig
	ServiceName        string
	DataflowName       string
	ServiceDirectorURL string
	MQTT               mqttconverter.MQTTClientConfig
	Service            mqttconverter.IngestionServiceConfig
	Producer           messagepipeline.GooglePubsubProducerConfig // UPDATED: Use the generic producer config
}

// LoadConfig initializes and loads the updated configuration.
func LoadConfig() (*Config, error) {
	cfg := &Config{
		BaseConfig: microservice.BaseConfig{
			LogLevel: "debug",
			HTTPPort: ":8081",
		},
		ServiceName:        "ingestion-local",
		DataflowName:       "dataflow-local",
		ServiceDirectorURL: "",
	}

	// Set defaults for all components
	producerCfg, _ := messagepipeline.LoadGooglePubsubProducerConfigFromEnv()
	cfg.Producer = *producerCfg

	mqttCfg, _ := mqttconverter.LoadMQTTClientConfigFromEnv()
	cfg.MQTT = *mqttCfg

	cfg.Service = mqttconverter.DefaultIngestionServiceConfig()

	// Override defaults with flags
	flag.StringVar(&cfg.ProjectID, "project-id", cfg.ProjectID, "GCP Project ID")
	flag.StringVar(&cfg.ServiceName, "service-name", cfg.ServiceName, "Unique name of this service instance")
	flag.StringVar(&cfg.Producer.TopicID, "producer-topic-id", cfg.Producer.TopicID, "Google Pub/Sub Topic ID for output")
	flag.StringVar(&cfg.MQTT.BrokerURL, "mqtt-broker-url", cfg.MQTT.BrokerURL, "MQTT Broker URL")
	flag.StringVar(&cfg.MQTT.Topic, "mqtt-topic", cfg.MQTT.Topic, "MQTT Topic to subscribe to")
	flag.Parse()

	// Override with environment variables: ENV > flag > default
	pkg.OverrideWithStringEnvVar("PROJECT_ID", &cfg.ProjectID)
	pkg.OverrideWithStringEnvVar("APP_SERVICE_NAME", &cfg.ServiceName)
	pkg.OverrideWithStringEnvVar("APP_PRODUCER_TOPIC_ID", &cfg.Producer.TopicID)
	pkg.OverrideWithStringEnvVar("APP_MQTT_BROKER_URL", &cfg.MQTT.BrokerURL)
	pkg.OverrideWithStringEnvVar("APP_MQTT_TOPIC", &cfg.MQTT.Topic)

	if port := os.Getenv("PORT"); port != "" {
		cfg.HTTPPort = ":" + port
	}

	if cfg.ProjectID == "" {
		return nil, fmt.Errorf("GCP_PROJECT_ID is required")
	}

	return cfg, nil
}
