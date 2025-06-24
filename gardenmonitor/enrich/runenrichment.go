package main

import (
	"cloud.google.com/go/firestore"
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/illmade-knight/go-iot-dataflows/gardenmonitor/enrich/eninit"
	"github.com/illmade-knight/go-iot/pkg/device"
	"github.com/illmade-knight/go-iot/pkg/messagepipeline"
	"google.golang.org/api/option"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// --- 1. Load Configuration (Unchanged) ---
	cfg, err := eninit.LoadConfig()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load configuration")
	}

	// --- 2. Set up Logger (Unchanged) ---
	level, err := zerolog.ParseLevel(cfg.LogLevel)
	if err != nil {
		level = zerolog.InfoLevel
		log.Warn().Str("log_level", cfg.LogLevel).Msg("Invalid log level, defaulting to 'info'")
	}
	zerolog.SetGlobalLevel(level)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: zerolog.TimeFieldFormat})
	log.Info().Msg("Logger configured.")
	log.Info().Interface("cfg", cfg).Msg("Configuration")

	// --- 3. Build Service Components (Updated) ---
	ctx := context.Background()

	// shared opts
	// Create the enrichment specific components.
	var opts []option.ClientOption
	if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
		log.Info().Str("credentials_file", cfg.CredentialsFile).Msg("Using specified credentials file for BigQuery client")
	} else {
		log.Info().Msg("Using Application Default Credentials (ADC) for BigQuery client")
	}

	sharedClient, err := pubsub.NewClient(ctx, cfg.ProjectID, opts...)
	defer sharedClient.Close()

	// Create the Pub/Sub consumer using the shared messagepipeline package.
	consumerCfg := &messagepipeline.GooglePubsubConsumerConfig{
		ProjectID:      cfg.ProjectID,
		SubscriptionID: cfg.Consumer.SubscriptionID,
	}

	pubsubLog := log.Logger

	consumer, err := messagepipeline.NewGooglePubsubConsumer(consumerCfg, sharedClient, log.Logger)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create Pub/Sub consumer")
	}

	producer, err := messagepipeline.NewGooglePubsubProducer[eninit.TestEnrichedMessage](sharedClient, cfg.Producer, pubsubLog)

	// setup the metadata fetcher
	cacheLog := log.Logger

	firestoreClient, err := firestore.NewClient(ctx, cfg.ProjectID, opts...)
	defer firestoreClient.Close()

	sourceFetcher, err := device.NewGoogleDeviceMetadataFetcher(firestoreClient, cfg.CacheConfig.FirestoreConfig, cacheLog)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create Metadata Fetcher")
	}

	// --- Create the Chained Fetcher ---
	// We pass the concrete `sourceFetcher` which satisfies the `SourceFetcher` interface.
	redisConfig := &device.RedisConfig{Addr: cfg.CacheConfig.RedisConfig.Addr, CacheTTL: 120 * time.Minute}
	fetcher, cleanup, err := device.NewChainedFetcher(ctx, redisConfig, sourceFetcher, cacheLog)
	defer func() {
		err := cleanup()
		if err != nil {
			cacheLog.Info().Err(err).Msg("Failed to clean up")
		}
	}()

	processLog := log.Logger
	// Assemble the final service using the new, clean constructor.
	// Transformer: Use the corrected enricher function.
	enricher := eninit.NewTestMessageEnricher(fetcher, processLog)

	// Processing Service: Ties all the components together.
	processingService, err := messagepipeline.NewProcessingService(
		2, // Number of workers
		consumer,
		producer,
		enricher,
		processLog,
	)

	// --- 4. Create and Run the Server (Unchanged) ---
	server := eninit.NewServer(cfg, processingService, log.Logger)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := server.Start(); err != nil {
			log.Fatal().Err(err).Msg("Server failed to start")
		}
	}()

	<-stop
	log.Warn().Msg("Shutdown signal received")
	server.Shutdown()
	log.Info().Msg("Server shut down gracefully.")
}
