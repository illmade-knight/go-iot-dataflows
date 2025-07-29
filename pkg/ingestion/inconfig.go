package ingestion

import (
	"flag"
	"os"
	"strconv"

	"google.golang.org/api/option"

	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/microservice"
	"github.com/illmade-knight/go-dataflow/pkg/mqttconverter"
)

// Config is updated to reflect the new architecture.
type Config struct {
	microservice.BaseConfig
	ServiceName        string
	DataflowName       string
	ServiceDirectorURL string
	PubsubOptions      []option.ClientOption

	MQTT                 mqttconverter.MQTTClientConfig
	Producer             messagepipeline.GooglePubsubProducerConfig
	NumProcessingWorkers int
}

// LoadConfigDefaults initializes and loads defaults
func LoadConfigDefaults(projectID string) (*Config, error) {
	cfg := &Config{
		BaseConfig: microservice.BaseConfig{
			ProjectID: projectID,
			LogLevel:  "debug",
			HTTPPort:  ":8081",
		},
		NumProcessingWorkers: 20,
	}

	producerCfg := messagepipeline.NewGooglePubsubProducerDefaults(projectID)
	cfg.Producer = *producerCfg

	mqttCfg := mqttconverter.LoadMQTTClientConfigFromEnv()
	cfg.MQTT = *mqttCfg

	// Override defaults with flags
	flag.IntVar(&cfg.NumProcessingWorkers, "num-workers", cfg.NumProcessingWorkers, "Number of concurrent processing workers")
	flag.Parse()

	if workers := os.Getenv("INGESTION_NUM_WORKERS"); workers != "" {
		if val, err := strconv.Atoi(workers); err == nil && val > 0 {
			cfg.NumProcessingWorkers = val
		}
	}
	if port := os.Getenv("PORT"); port != "" {
		cfg.HTTPPort = ":" + port
	}
	return cfg, nil
}
