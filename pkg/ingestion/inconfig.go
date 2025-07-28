package ingestion

import (
	"flag"
	"fmt"
	"os"
	"strconv"

	"google.golang.org/api/option"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/microservice"
	"github.com/illmade-knight/go-dataflow/pkg/mqttconverter"

	"github.com/illmade-knight/go-iot-dataflows/pkg"
)

// Config is updated to reflect the new architecture.
type Config struct {
	microservice.BaseConfig
	ServiceName        string
	DataflowName       string
	ServiceDirectorURL string
	PubsubOptions      []option.ClientOption

	MQTT     mqttconverter.MQTTClientConfig
	Producer messagepipeline.GooglePubsubProducerConfig
	// REFACTOR: Removed obsolete Service config, added NumProcessingWorkers.
	NumProcessingWorkers int
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
		// REFACTOR: Set a default for the number of workers.
		NumProcessingWorkers: 20,
	}

	producerCfg := messagepipeline.NewGooglePubsubProducerDefaults()
	cfg.Producer = *producerCfg

	mqttCfg := mqttconverter.LoadMQTTClientConfigFromEnv()
	cfg.MQTT = *mqttCfg

	// Override defaults with flags
	flag.StringVar(&cfg.ProjectID, "project-id", cfg.ProjectID, "GCP Project ID")
	flag.StringVar(&cfg.ServiceName, "service-name", cfg.ServiceName, "Unique name of this service instance")
	flag.StringVar(&cfg.Producer.TopicID, "producer-topic-id", cfg.Producer.TopicID, "Google Pub/Sub Topic ID for output")
	flag.StringVar(&cfg.MQTT.BrokerURL, "mqtt-broker-url", cfg.MQTT.BrokerURL, "MQTT Broker URL")
	flag.StringVar(&cfg.MQTT.Topic, "mqtt-topic", cfg.MQTT.Topic, "MQTT Topic to subscribe to")
	flag.IntVar(&cfg.NumProcessingWorkers, "num-workers", cfg.NumProcessingWorkers, "Number of concurrent processing workers")
	flag.Parse()

	// Override with environment variables: ENV > flag > default
	pkg.OverrideWithStringEnvVar("PROJECT_ID", &cfg.ProjectID)
	pkg.OverrideWithStringEnvVar("APP_SERVICE_NAME", &cfg.ServiceName)
	pkg.OverrideWithStringEnvVar("APP_PRODUCER_TOPIC_ID", &cfg.Producer.TopicID)
	pkg.OverrideWithStringEnvVar("APP_MQTT_BROKER_URL", &cfg.MQTT.BrokerURL)
	pkg.OverrideWithStringEnvVar("APP_MQTT_TOPIC", &cfg.MQTT.Topic)
	if workers := os.Getenv("APP_NUM_WORKERS"); workers != "" {
		if val, err := strconv.Atoi(workers); err == nil && val > 0 {
			cfg.NumProcessingWorkers = val
		}
	}

	if cfg.Producer.TopicID == "" {
		return nil, fmt.Errorf("producer topic ID must be set via flag or APP_PRODUCER_TOPIC_ID")
	}
	if port := os.Getenv("PORT"); port != "" {
		cfg.HTTPPort = ":" + port
	}
	if cfg.ProjectID == "" {
		return nil, fmt.Errorf("GCP_PROJECT_ID is required")
	}

	return cfg, nil
}
