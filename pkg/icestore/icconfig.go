package icestore

import (
	"flag"
	"google.golang.org/api/option"
	"os"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/microservice"
	"github.com/illmade-knight/go-iot-dataflows/pkg"
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
	microservice.BaseConfig
	ServiceName        string
	DataflowName       string
	ServiceDirectorURL string
	Consumer           Consumer
	IceStore           IceStore

	// FIX: Add fields for client options to support emulator connections.
	PubsubOptions []option.ClientOption
	GCSOptions    []option.ClientOption

	BatchProcessing struct {
		NumWorkers    int
		BatchSize     int
		FlushInterval time.Duration // RENAMED: Was FlushTimeout
		UploadTimeout time.Duration // NEW: Timeout for a single GCS upload operation
	}
}

// LoadConfig initializes and loads configuration from defaults, flags, and environment variables.
func LoadConfig() (*Config, error) {
	cfg := &Config{
		BaseConfig: microservice.BaseConfig{
			LogLevel: "debug",
			HTTPPort: ":8083",
		},
	}
	cfg.BatchProcessing.NumWorkers = 5
	cfg.BatchProcessing.BatchSize = 100
	cfg.BatchProcessing.FlushInterval = 1 * time.Minute
	cfg.BatchProcessing.UploadTimeout = 2 * time.Minute

	flag.StringVar(&cfg.Consumer.SubscriptionID, "consumer.subscription-id", cfg.Consumer.SubscriptionID, "Pub/Sub Subscription ID to consume from")
	flag.StringVar(&cfg.IceStore.BucketName, "ice_store.bucket-name", cfg.IceStore.BucketName, "Google Cloud Storage bucket name")
	flag.IntVar(&cfg.BatchProcessing.NumWorkers, "batch.num-workers", cfg.BatchProcessing.NumWorkers, "Number of processing workers")
	flag.IntVar(&cfg.BatchProcessing.BatchSize, "batch.size", cfg.BatchProcessing.BatchSize, "GCS batch write size")
	flag.DurationVar(&cfg.BatchProcessing.FlushInterval, "batch.flush-interval", cfg.BatchProcessing.FlushInterval, "GCS batch flush interval")
	flag.DurationVar(&cfg.BatchProcessing.UploadTimeout, "batch.upload-timeout", cfg.BatchProcessing.UploadTimeout, "GCS batch upload timeout")

	flag.Parse()

	pkg.OverrideWithIntEnvVar("APP_BATCH_NUM_WORKERS", &cfg.BatchProcessing.NumWorkers)
	pkg.OverrideWithIntEnvVar("APP_BATCH_SIZE", &cfg.BatchProcessing.BatchSize)
	pkg.OverrideWithDurationEnvVar("APP_BATCH_FLUSH_INTERVAL", &cfg.BatchProcessing.FlushInterval)
	pkg.OverrideWithDurationEnvVar("APP_BATCH_UPLOAD_TIMEOUT", &cfg.BatchProcessing.UploadTimeout)

	if port := os.Getenv("PORT"); port != "" {
		cfg.HTTPPort = ":" + port
	}

	return cfg, nil
}
