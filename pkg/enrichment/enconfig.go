package enrichment

import (
	"flag"
	"github.com/illmade-knight/go-iot-dataflows/pkg"
	"os"
	"time"

	"github.com/illmade-knight/go-cloud-manager/microservice"
	"github.com/illmade-knight/go-iot/pkg/device"
	"github.com/illmade-knight/go-iot/pkg/messagepipeline"
)

// Consumer defines the configuration for the Pub/Sub subscriber.
type Consumer struct {
	SubscriptionID string
}

// CacheConfig defines settings for device metadata caching.
type CacheConfig struct {
	RedisConfig     device.RedisConfig
	FirestoreConfig *device.FirestoreFetcherConfig
}

// ProcessorConfig holds settings specific to the message processing workers.
type ProcessorConfig struct {
	NumWorkers int
}

// Config holds all configuration for the enrichment microservice.
type Config struct {
	microservice.BaseConfig

	ServiceName        string
	DataflowName       string
	ServiceDirectorURL string

	Consumer        Consumer
	ProducerConfig  *messagepipeline.GooglePubsubProducerConfig
	CacheConfig     CacheConfig
	ProcessorConfig ProcessorConfig
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
			HTTPPort:  ":8082",
			ProjectID: "local-dev-project",
		},
		ServiceName:        "enrichment-local",
		DataflowName:       "dataflow-local",
		ServiceDirectorURL: "",
		ProducerConfig:     &messagepipeline.GooglePubsubProducerConfig{},
		CacheConfig: CacheConfig{
			FirestoreConfig: &device.FirestoreFetcherConfig{},
		},
	}
	cfg.Consumer.SubscriptionID = "dev-enrichment-sub"
	cfg.ProducerConfig.TopicID = "dev-enriched-topic"
	cfg.ProducerConfig.BatchDelay = 20 * time.Millisecond
	cfg.CacheConfig.RedisConfig.Addr = "localhost:6379"
	cfg.CacheConfig.RedisConfig.CacheTTL = 2 * time.Hour
	cfg.CacheConfig.FirestoreConfig.CollectionName = "devices"
	cfg.ProcessorConfig.NumWorkers = 5

	// 2. Define flags to pkg.Override defaults.
	flag.StringVar(&cfg.ProjectID, "project-id", cfg.ProjectID, "GCP Project ID")
	flag.StringVar(&cfg.ServiceName, "service-name", cfg.ServiceName, "Unique name of this service instance")
	flag.StringVar(&cfg.Consumer.SubscriptionID, "consumer.subscription-id", cfg.Consumer.SubscriptionID, "Pub/Sub subscription to consume from")
	flag.StringVar(&cfg.ProducerConfig.TopicID, "producer.topic-id", cfg.ProducerConfig.TopicID, "Pub/Sub topic to publish to")
	flag.StringVar(&cfg.CacheConfig.RedisConfig.Addr, "cache.redis.addr", cfg.CacheConfig.RedisConfig.Addr, "Redis address")
	flag.DurationVar(&cfg.CacheConfig.RedisConfig.CacheTTL, "cache.redis.cache-ttl", cfg.CacheConfig.RedisConfig.CacheTTL, "Redis cache TTL")
	flag.StringVar(&cfg.CacheConfig.FirestoreConfig.CollectionName, "cache.firestore.collection-name", cfg.CacheConfig.FirestoreConfig.CollectionName, "Firestore collection for device metadata")
	flag.IntVar(&cfg.ProcessorConfig.NumWorkers, "processor.num-workers", cfg.ProcessorConfig.NumWorkers, "Number of message processing workers")
	flag.Parse()

	// 3. Override with environment variables if they are set. ENV > flag > default.
	pkg.OverrideWithStringEnvVar("PROJECT_ID", &cfg.ProjectID)
	pkg.OverrideWithStringEnvVar("SERVICE_DIRECTOR_URL", &cfg.ServiceDirectorURL)
	pkg.OverrideWithStringEnvVar("APP_SERVICE_NAME", &cfg.ServiceName)
	pkg.OverrideWithStringEnvVar("APP_CONSUMER_SUBSCRIPTION_ID", &cfg.Consumer.SubscriptionID)
	pkg.OverrideWithStringEnvVar("APP_PRODUCER_TOPIC_ID", &cfg.ProducerConfig.TopicID)
	pkg.OverrideWithStringEnvVar("APP_CACHE_REDIS_ADDR", &cfg.CacheConfig.RedisConfig.Addr)
	pkg.OverrideWithDurationEnvVar("APP_CACHE_REDIS_CACHE_TTL", &cfg.CacheConfig.RedisConfig.CacheTTL)
	pkg.OverrideWithStringEnvVar("APP_CACHE_FIRESTORE_COLLECTION_NAME", &cfg.CacheConfig.FirestoreConfig.CollectionName)
	pkg.OverrideWithIntEnvVar("APP_PROCESSOR_NUM_WORKERS", &cfg.ProcessorConfig.NumWorkers)

	// 4. Special handling for Cloud Run's PORT.
	if port := os.Getenv("PORT"); port != "" {
		cfg.HTTPPort = ":" + port
	}

	return cfg, nil
}
