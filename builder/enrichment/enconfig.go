// github.com/illmade-knight/go-iot-dataflows/builder/enrichment/enconfig.go
package enrichment

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/illmade-knight/go-iot-dataflows/builder"
	"github.com/illmade-knight/go-iot/pkg/device"
	"github.com/illmade-knight/go-iot/pkg/messagepipeline"
	"github.com/rs/zerolog/log"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Consumer defines the configuration for the Pub/Sub subscriber.
type Consumer struct {
	SubscriptionID string `mapstructure:"subscription_id"`
}

// CacheConfig defines settings for device metadata caching.
type CacheConfig struct {
	RedisConfig     device.RedisConfig             `mapstructure:"redis"`
	FirestoreConfig *device.FirestoreFetcherConfig `mapstructure:"firestore"`
}

// ProcessorConfig holds settings specific to the message processing workers.
type ProcessorConfig struct {
	NumWorkers int `mapstructure:"num_workers"`
}

// Config holds all configuration for the enrichment microservice.
type Config struct {
	builder.BaseConfig `mapstructure:",squash"`

	ServiceName        string `mapstructure:"service_name"`
	DataflowName       string `mapstructure:"dataflow_name"`
	ServiceDirectorURL string `mapstructure:"service_director_url"`

	Consumer        Consumer                                    `mapstructure:"consumer"`
	ProducerConfig  *messagepipeline.GooglePubsubProducerConfig `mapstructure:"producer"`
	CacheConfig     CacheConfig                                 `mapstructure:"cache"`
	ProcessorConfig ProcessorConfig                             `mapstructure:"processor"`
}

// LoadConfigFromEnv loads configuration strictly from environment variables using Viper.
func LoadConfigFromEnv() (*Config, error) {
	v := viper.New()
	v.SetEnvPrefix("APP")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	v.SetDefault("log_level", "info")
	v.SetDefault("http_port", ":8080")
	v.SetDefault("processor.num_workers", 5)
	v.SetDefault("producer.batch_delay", 50*time.Millisecond)
	v.SetDefault("cache.redis.cache_ttl", 120*time.Minute)

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal configuration from environment: %w", err)
	}

	if port := os.Getenv("PORT"); port != "" {
		cfg.HTTPPort = ":" + port
	}

	if cfg.ProjectID == "" || cfg.ServiceName == "" || cfg.DataflowName == "" || cfg.Consumer.SubscriptionID == "" || cfg.ProducerConfig.TopicID == "" || cfg.CacheConfig.RedisConfig.Addr == "" || cfg.CacheConfig.FirestoreConfig.CollectionName == "" {
		return nil, fmt.Errorf("missing required configuration environment variables (e.g., APP_PROJECT_ID, APP_SERVICE_NAME, APP_CONSUMER_SUBSCRIPTION_ID, APP_PRODUCER_TOPIC_ID, etc.)")
	}
	return &cfg, nil
}

// LoadConfigWithOverrides provides a flexible way to load configuration for development and testing.
func LoadConfigWithOverrides() (*Config, error) {
	v := viper.New()
	pflag.String("config", "", "Path to config file (e.g., config-enrichment.yaml)")

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

func setDevelopmentDefaults(v *viper.Viper) {
	v.SetDefault("log_level", "debug")
	v.SetDefault("http_port", ":8082")
	v.SetDefault("project_id", "local-dev-project")
	v.SetDefault("service_name", "enrichment-local")
	v.SetDefault("dataflow_name", "dataflow-local")
	v.SetDefault("service_director_url", "")

	v.SetDefault("consumer.subscription_id", "dev-enrichment-sub")
	v.SetDefault("producer.topic_id", "dev-enriched-topic")
	v.SetDefault("producer.batch_delay", 20*time.Millisecond)

	v.SetDefault("cache.redis.addr", "localhost:6379")
	v.SetDefault("cache.redis.cache_ttl", 2*time.Hour)
	v.SetDefault("cache.firestore.collection_name", "devices")

	v.SetDefault("processor.num_workers", 5)
}

func defineFlags(fs *pflag.FlagSet) {
	fs.String("log-level", "", "Log level")
	fs.String("http-port", "", "HTTP health check port")
	fs.String("project-id", "", "GCP Project ID")
	fs.String("credentials-file", "", "Path to general GCP credentials JSON file")

	fs.String("service-name", "", "Unique name of this service instance")
	fs.String("dataflow-name", "", "Dataflow this service belongs to")
	fs.String("service-director-url", "", "URL of the ServiceDirector API")

	fs.String("consumer.subscription-id", "", "Pub/Sub subscription to consume from")
	fs.String("producer.topic-id", "", "Pub/Sub topic to publish to")

	fs.String("cache.redis.addr", "", "Redis address")
	fs.Duration("cache.redis.cache-ttl", 0, "Redis cache TTL")
	fs.String("cache.firestore.collection-name", "", "Firestore collection for device metadata")

	fs.Int("processor.num-workers", 0, "Number of message processing workers")
}
