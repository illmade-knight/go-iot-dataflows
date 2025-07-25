package icestore

import (
	"flag"
	"fmt"
	"github.com/illmade-knight/go-iot-dataflows/pkg"
	"os"
	"time"

	"github.com/illmade-knight/go-cloud-manager/microservice"
)

// Consumer defines the configuration for the Pub/Sub subscriber.
type Consumer struct {
	SubscriptionID  string
	CredentialsFile string // Optional path to GCP credentials file
}

// IceStore defines configuration specific to Google Cloud Storage (GCS) for archival.
type IceStore struct {
	CredentialsFile string // Optional path to GCP credentials file for GCS
	BucketName      string // Name of the GCS bucket for archival
	ObjectPrefix    string // Prefix for objects stored in the bucket (e.g., "archived-data/")
}

// Config holds all configuration for the IceStore microservice.
type Config struct {
	microservice.BaseConfig // Embed BaseConfig to inherit common fields

	ServiceName        string
	DataflowName       string
	ServiceDirectorURL string
	Consumer           Consumer
	IceStore           IceStore

	BatchProcessing struct {
		NumWorkers   int
		BatchSize    int
		FlushTimeout time.Duration
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
			HTTPPort:  ":8083",
			ProjectID: "local-dev-project",
		},
		ServiceName:        "icestore-local",
		DataflowName:       "dataflow-local",
		ServiceDirectorURL: "",
	}
	cfg.BatchProcessing.NumWorkers = 2
	cfg.BatchProcessing.BatchSize = 10
	cfg.BatchProcessing.FlushTimeout = 2 * time.Second
	cfg.Consumer.SubscriptionID = "dev-icestore-subscription"
	cfg.IceStore.BucketName = "dev-archive-bucket"
	cfg.IceStore.ObjectPrefix = "archived-dev/"

	// 2. Define flags to pkg.Override defaults.
	flag.StringVar(&cfg.ProjectID, "project-id", cfg.ProjectID, "GCP Project ID")
	flag.StringVar(&cfg.CredentialsFile, "credentials-file", cfg.CredentialsFile, "Path to general GCP credentials JSON file")
	flag.StringVar(&cfg.ServiceName, "service-name", cfg.ServiceName, "Unique name of this service instance")
	flag.StringVar(&cfg.DataflowName, "dataflow-name", cfg.DataflowName, "Dataflow this service belongs to")
	flag.StringVar(&cfg.ServiceDirectorURL, "service-director-url", cfg.ServiceDirectorURL, "URL of the ServiceDirector API")
	flag.StringVar(&cfg.Consumer.SubscriptionID, "consumer.subscription-id", cfg.Consumer.SubscriptionID, "Pub/Sub Subscription ID to consume from")
	flag.StringVar(&cfg.IceStore.BucketName, "ice_store.bucket-name", cfg.IceStore.BucketName, "Google Cloud Storage bucket name for archival")
	flag.StringVar(&cfg.IceStore.ObjectPrefix, "ice_store.object-prefix", cfg.IceStore.ObjectPrefix, "Prefix for objects in GCS bucket")
	flag.IntVar(&cfg.BatchProcessing.NumWorkers, "batchProcessing.num-workers", cfg.BatchProcessing.NumWorkers, "Number of processing workers")
	flag.IntVar(&cfg.BatchProcessing.BatchSize, "batchProcessing.batch-size", cfg.BatchProcessing.BatchSize, "GCS batch write size")
	flag.DurationVar(&cfg.BatchProcessing.FlushTimeout, "batchProcessing.flush-timeout", cfg.BatchProcessing.FlushTimeout, "GCS batch flush timeout")

	flag.Parse()

	// 3. Override with environment variables if they are set. ENV > flag > default.
	pkg.OverrideWithStringEnvVar("PROJECT_ID", &cfg.ProjectID)
	pkg.OverrideWithStringEnvVar("SERVICE_DIRECTOR_URL", &cfg.ServiceDirectorURL)
	pkg.OverrideWithStringEnvVar("APP_SERVICE_NAME", &cfg.ServiceName)
	pkg.OverrideWithStringEnvVar("APP_DATAFLOW_NAME", &cfg.DataflowName)
	pkg.OverrideWithStringEnvVar("APP_CONSUMER_SUBSCRIPTION_ID", &cfg.Consumer.SubscriptionID)
	pkg.OverrideWithStringEnvVar("APP_ICE_STORE_BUCKET_NAME", &cfg.IceStore.BucketName)
	pkg.OverrideWithStringEnvVar("APP_ICE_STORE_OBJECT_PREFIX", &cfg.IceStore.ObjectPrefix)
	pkg.OverrideWithIntEnvVar("APP_BATCHPROCESSING_NUM_WORKERS", &cfg.BatchProcessing.NumWorkers)
	pkg.OverrideWithIntEnvVar("APP_BATCHPROCESSING_BATCH_SIZE", &cfg.BatchProcessing.BatchSize)
	pkg.OverrideWithDurationEnvVar("APP_BATCHPROCESSING_FLUSH_TIMEOUT", &cfg.BatchProcessing.FlushTimeout)

	// 4. Special handling for Cloud Run's PORT, which takes highest precedence.
	if port := os.Getenv("PORT"); port != "" {
		cfg.HTTPPort = ":" + port
	}

	// 5. Final validation for production-critical variables
	if os.Getenv("APP_ENV") == "production" {
		if cfg.ProjectID == "" || cfg.ServiceName == "" || cfg.DataflowName == "" || cfg.Consumer.SubscriptionID == "" || cfg.IceStore.BucketName == "" {
			return nil, fmt.Errorf("missing required configuration for production environment")
		}
	}

	return cfg, nil
}
