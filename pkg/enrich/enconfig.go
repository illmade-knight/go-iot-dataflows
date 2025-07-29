package enrich

import (
	"flag"
	"os"
	"time"

	"google.golang.org/api/option"

	"github.com/illmade-knight/go-dataflow/pkg/cache"
	"github.com/illmade-knight/go-dataflow/pkg/messagepipeline"
	"github.com/illmade-knight/go-dataflow/pkg/microservice"
)

// Consumer defines the configuration for the Pub/Sub subscriber.
type Consumer struct {
	SubscriptionID string
}

// CacheConfig defines settings for device metadata caching.
type CacheConfig struct {
	RedisConfig       cache.RedisConfig
	FirestoreConfig   *cache.FirestoreConfig
	CacheWriteTimeout time.Duration // Timeout for the fetcher's background cache write.
}

// ProcessorConfig holds settings for the message processing workers.
type ProcessorConfig struct {
	NumWorkers int
}

// Config holds all configuration for the enrichment microservice.
type Config struct {
	microservice.BaseConfig
	ServiceName        string
	DataflowName       string
	ServiceDirectorURL string
	Consumer           Consumer
	ProducerConfig     *messagepipeline.GooglePubsubProducerConfig
	CacheConfig        CacheConfig
	ProcessorConfig    ProcessorConfig
	ClientConnections  map[string][]option.ClientOption
}

// LoadConfigDefaults initializes and loads configuration from defaults, flags, and environment variables.
func LoadConfigDefaults(projectID string) (*Config, error) {
	cfg := &Config{
		BaseConfig: microservice.BaseConfig{
			ProjectID: projectID,
			LogLevel:  "debug",
			HTTPPort:  ":8082",
		},
		ProducerConfig: messagepipeline.NewGooglePubsubProducerDefaults(projectID),
		CacheConfig: CacheConfig{
			CacheWriteTimeout: 5 * time.Second,
			RedisConfig: cache.RedisConfig{
				CacheTTL: 2 * time.Hour,
			},
			FirestoreConfig: &cache.FirestoreConfig{
				ProjectID: projectID,
			},
		},
		ProcessorConfig: ProcessorConfig{
			NumWorkers: 5,
		},
	}

	flag.StringVar(&cfg.CacheConfig.RedisConfig.Addr, "cache.redis.addr", cfg.CacheConfig.RedisConfig.Addr, "Redis address")
	flag.DurationVar(&cfg.CacheConfig.RedisConfig.CacheTTL, "cache.redis.cache-ttl", cfg.CacheConfig.RedisConfig.CacheTTL, "Redis cache TTL")
	flag.DurationVar(&cfg.CacheConfig.CacheWriteTimeout, "cache.write-timeout", cfg.CacheConfig.CacheWriteTimeout, "Timeout for background cache writes")
	flag.IntVar(&cfg.ProcessorConfig.NumWorkers, "processor.num-workers", cfg.ProcessorConfig.NumWorkers, "Number of message processing workers")
	flag.Parse()

	if port := os.Getenv("PORT"); port != "" {
		cfg.HTTPPort = ":" + port
	}

	return cfg, nil
}
